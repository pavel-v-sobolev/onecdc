"""
Лог-таблицы пайплайна в БД (общие хелперы на SQLAlchemy Core).

- `onecdc_replicator_log` — лог загрузки: строка на каждую загрузку объекта. `type` — вид загрузки
  (`changes` — пакет изменений, `full` — полная выгрузка); `message_no` — номер пакета обмена
  (NULL для полной выгрузки); `started_at`/`finished_at` (серверное `func.now()`,
  finished_at=NULL у незавершённой/упавшей); счётчики строк merge и `total_time` наращиваются
  в БД по мере сохранений (см. ReplicatorLog.write_result).

Время берётся серверным `func.now()`. Схема не задана (schema=None) — работаем в схеме БД
по умолчанию (public у PostgreSQL), как это делает и dbmerge.
"""

import time
from dataclasses import dataclass
from datetime import timedelta

from dbmerge import mergeResult

from sqlalchemy import (Column, DateTime, Engine, Index, Integer, MetaData, String,
                        Table, func, insert, inspect, select, text, update, schema, Numeric)
from sqlalchemy.exc import DatabaseError

from onecdc.common_functions import DB_NOW_WITH_TIMEZONE, POSTGRES_MAX_IDENTIFIER
from onecdc.logging_config import get_logger

logger = get_logger(__name__)

REPLICATOR_LOG = "onecdc_replicator_log"

# Тип строки лога: обработка пакета изменений или полная выгрузка.
LOAD_TYPE_CHANGES = 'changes'
LOAD_TYPE_FULL = 'full'


# «Проверить и создать» — не атомарная пара: между проверкой существования и CREATE успевает
# вклиниться другой процесс или поток (несколько репликаторов на одну схему — штатный сценарий:
# по репликатору на очередь-отдачу). Тогда CREATE падает на уникальном индексе системного каталога
# — в PostgreSQL это UniqueViolation по pg_class_relname_nsp_index, в других СУБД «already exists».
#
# Гасить это блокировкой не нужно: проигравшему гонку нужен не сам CREATE, а его результат, и
# результат уже есть. Поэтому ошибку проглатываем и ПЕРЕПРОВЕРЯЕМ — объект на месте, идём дальше;
# нет (нет прав, нет схемы, битое соединение) — пробрасываем как есть. Случается это один раз на
# схему, на старте, так что цена перепроверки никакая.
def _create_if_absent(engine: Engine, create, exists, what: str) -> None:
    try:
        create()
    except DatabaseError as error:
        if not exists():
            raise
        logger.debug("%s already created concurrently, continuing (%s)", what, type(error).__name__)


def _check_db_schema(db_schema: str | None) -> str | None:
    """
    Имя схемы БД либо None (схема по умолчанию). Пустая строка — почти наверняка незаполненная
    переменная окружения, а не осознанный выбор.

    Кавычки и управляющие символы отвергаем — это про написание, а не про безопасность. Значение с
    кавычкой или переводом строки почти всегда означает неразвёрнутую переменную окружения, и
    узнать об этом лучше на старте, чем найти потом в базе схему с таким именем. Своему SQL
    библиотека имя схемы не подставляет нигде (кавычит SQLAlchemy), но обработчик пишет его в свой
    запрос сам, f-строкой от context.schema, — и кавычка внутри имени сломала бы такой запрос.

    Длину меряем в БАЙТАХ: Postgres режет идентификатор по 63 байтам МОЛЧА, а кириллица занимает
    по два байта на символ (см. common_functions.truncate_to_bytes).
    """
    if db_schema is None:
        return None
    if not isinstance(db_schema, str):
        raise ValueError(f"db_schema must be a schema name string or None (got {db_schema!r})")
    name = db_schema.strip() or None
    if name is None:
        return None
    if '"' in name or any(ord(ch) < 32 for ch in name):
        raise ValueError(f"db_schema looks like an unexpanded value: quotes and control "
                         f"characters are not allowed in a schema name (got {db_schema!r})")
    if len(name.encode('utf-8')) > POSTGRES_MAX_IDENTIFIER:
        raise ValueError(f"db_schema is longer than {POSTGRES_MAX_IDENTIFIER} bytes and Postgres "
                         f"would truncate it silently (got {db_schema!r})")
    return name


def _check_create_schema(engine: Engine, schema_name: str | None) -> str | None:
    # Проверка имени здесь, а не у вызывающего: этот хелпер зовут все, кто заводит свои таблицы
    # (репликатор, цикл обработчиков, реестр имён), и другой точки, общей для всех, нет.
    schema_name = _check_db_schema(schema_name)
    # schema_name=None — работаем в схеме БД по умолчанию, создавать нечего: возвращаем None,
    # не дёргая has_schema/CreateSchema с None.
    if schema_name is None:
        return None

    def create() -> None:
        with engine.begin() as conn:
            if not conn.dialect.has_schema(conn, schema_name):
                logger.info(f"""Creating schema "{schema_name}".""")
                conn.execute(schema.CreateSchema(schema_name))

    _create_if_absent(engine, create, lambda: inspect(engine).has_schema(schema_name),
                      f'schema "{schema_name}"')
    return schema_name


def create_table_if_absent(engine: Engine, table: Table) -> tuple[set[str], set[str]]:
    """
    CREATE TABLE идемпотентно и без гонки на старте нескольких репликаторов (см. _create_if_absent),
    а сразу следом — переезд состава колонок. Возвращает (что было до переезда, что добавили).

    Переезд именно ЗДЕСЬ, а не отдельным вызовом рядом: `create(checkfirst=True)` заводит таблицу
    целиком, но давно созданную не трогает, поэтому колонка, добавленная в служебную таблицу
    новой версией библиотеки, на свежей базе есть, а на действующей нет — и запрос падает с
    «column does not exist» только у пользователя. Оффлайн-тесты это не ловят в принципе: они
    всегда начинают со схемы с нуля. Правило «добавили колонку — не забудьте позвать переезд»
    мы в этом же проекте забыли дважды (onecdc_writes_in_process, onecdc_replicator_log), так что
    забывать больше нечего: кто завёл таблицу, тот её и обновил.
    """
    _create_if_absent(engine, lambda: table.create(engine, checkfirst=True),
                      lambda: inspect(engine).has_table(table.name, schema=table.schema),
                      f'table "{table.name}"')
    return add_missing_columns(engine, table)


def add_missing_columns(engine: Engine, table: Table) -> tuple[set[str], set[str]]:
    """
    Дописывает в существующую таблицу колонки, которых в ней ещё нет. Возвращает (что было,
    что добавили) — вызывающему это нужно, если переезд на новую колонку требует переноса данных.

    Отдельно от create_table_if_absent — только чтобы звать переезд без создания. Обычному коду
    этого не нужно: create_table_if_absent сам зовёт это последним шагом и отдаёт тот же ответ.
    """
    in_db = inspect(engine).get_columns(table.name, schema=table.schema)
    # Тип сверяется по тому же снимку каталога, что и состав: опрашивать его дважды на каждую
    # служебную таблицу — заметная доля стоимости старта (замерено: +5 мс на конструктор).
    align_timestamp_columns(engine, table, in_db)
    existing = {column['name'] for column in in_db}
    missing = [column for column in table.columns if column.name not in existing]
    if not missing:
        return existing, set()
    compiler = engine.dialect.ddl_compiler(engine.dialect, None)
    with engine.begin() as conn:
        for column in missing:
            conn.execute(text(f'ALTER TABLE {compiler.preparer.format_table(table)} '
                              f'ADD COLUMN {compiler.get_column_specification(column)}'))
    logger.info("Added columns to %s: %s", table.name, ', '.join(c.name for c in missing))
    return existing, {column.name for column in missing}


# Служебные отметки времени хранятся С ПОЯСОМ. Наивная отметка — это настенное время пояса
# сессии, а оно немонотонно: в поясе с сезонным переводом осенние часы отступают назад, и час
# инкремента теряется молча (CDC-25). Значение в повторяющийся час двусмысленно по существу,
# поэтому лечится это типом, а не аккуратностью сравнений.
#
# Даты, приехавшие из 1С, остаются НАИВНЫМИ: в 1С поясов нет, это бизнес-значения, а не моменты.
# Граница между двумя видами времени проходит ровно здесь.
MERGE_TIMESTAMP_FIELDS = ('merged_on', 'inserted_on')

# Схемы, по которым обход уже делался в этом процессе (см. align_merge_timestamps).
_MERGE_TIMESTAMPS_CHECKED: set[tuple[str, str | None]] = set()


def _timestamptz_alter(engine: Engine, qualified_table: str, columns: list[str],
                       table_oid: int | None = None) -> None:
    """
    Один ALTER на все колонки таблицы, а не по одному на каждую: под не-UTC такой ALTER
    ПЕРЕПИСЫВАЕТ таблицу целиком (≈1 с на миллион строк, ACCESS EXCLUSIVE), и делать это дважды
    там, где хватает одного прохода, незачем.

    `USING` не нужен: PostgreSQL истолковывает старое наивное значение в поясе сессии — ровно так,
    как оно и записывалось. Момент сохраняется, разрыва в данных нет.

    Зависимые вьюшки (если задан table_oid) снимаются и ставятся обратно в той же транзакции:
    менять тип колонки под вьюшкой PostgreSQL не даёт, а витрины строятся именно так.
    """
    preparer = engine.dialect.identifier_preparer
    clauses = ', '.join(f'ALTER COLUMN {preparer.quote(column)} TYPE timestamptz'
                        for column in columns)
    started = time.monotonic()
    with engine.begin() as conn:
        views = _dependent_views(conn, table_oid, preparer) if table_oid is not None else []
        # Без CASCADE и без IF EXISTS намеренно: роняем строго то, что нашли и сохранили, в
        # обратном порядке зависимости. Если обход что-то упустил, DROP откажется — и транзакция
        # откатит всё. CASCADE в этом же случае снёс бы объект, восстановить который нечем.
        for view in reversed(views):                 # сначала те, кто смотрит на других
            conn.execute(text(f'DROP VIEW {view.qualified}'))
        conn.execute(text(f'ALTER TABLE {qualified_table} {clauses}'))
        _restore_views(conn, views, preparer)
    logger.info("Migrated %s (%s) to timestamptz in %.1fs%s", qualified_table,
                ', '.join(columns), time.monotonic() - started,
                f'; rebuilt {len(views)} dependent view(s)' if views else '')


def align_timestamp_columns(engine: Engine, table: Table, in_db: list | None = None) -> set[str]:
    """
    Приводит тип datetime-колонок существующей таблицы к объявленному: колонка, объявленная с
    поясом, но лежащая в БД без пояса, переводится в timestamptz. Возвращает переведённые.

    Зовётся из create_table_if_absent по той же причине, что и переезд состава колонок: таблица,
    заведённая прошлой версией библиотеки, сама себя не починит, а расхождение вылезет не у нас,
    а у пользователя — и не отказом, а молчаливой потерей часа раз в год.
    """
    if engine.dialect.name != 'postgresql':
        return set()
    aware = {column.name for column in table.columns
             if isinstance(column.type, DateTime) and column.type.timezone}
    if not aware:
        return set()
    if in_db is None:
        in_db = inspect(engine).get_columns(table.name, schema=table.schema)
    naive = [column['name'] for column in in_db
             if column['name'] in aware and isinstance(column['type'], DateTime)
             and not column['type'].timezone]
    if not naive:
        return set()
    preparer = engine.dialect.identifier_preparer
    _timestamptz_alter(engine, preparer.format_table(table), naive)
    return set(naive)


# Зависимые вьюшки: PostgreSQL не даёт менять тип колонки, на которую смотрит вьюшка
# («cannot alter type of a column used by a view or rule»), а витрины у нас именно так и строятся.
# Достаём их рекурсивно (вьюшка поверх вьюшки — наш же второй пример), вместе с тем, что теряется
# при пересоздании: владельцем, правами и комментарием.
_DEPENDENT_VIEWS = """
WITH RECURSIVE deps AS (
    SELECT r.ev_class AS oid, 1 AS depth
      FROM pg_depend d
      JOIN pg_rewrite r ON r.oid = d.objid
     WHERE d.refobjid = :table_oid AND d.classid = 'pg_rewrite'::regclass
       AND r.ev_class <> :table_oid
    UNION ALL
    SELECT r.ev_class, deps.depth + 1
      FROM deps
      JOIN pg_depend d ON d.refobjid = deps.oid AND d.classid = 'pg_rewrite'::regclass
      JOIN pg_rewrite r ON r.oid = d.objid AND r.ev_class <> deps.oid
     WHERE deps.depth < 32
)
SELECT c.oid, max(deps.depth) AS depth, c.relkind, n.nspname, c.relname,
       pg_get_viewdef(c.oid, true), pg_get_userbyid(c.relowner),
       obj_description(c.oid, 'pg_class')
  FROM deps
  JOIN pg_class c ON c.oid = deps.oid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 GROUP BY c.oid, c.relkind, n.nspname, c.relname, c.relowner
 ORDER BY depth
"""

_VIEW_GRANTS = """
SELECT acl.grantee::regrole::text, acl.privilege_type
  FROM pg_class c, aclexplode(c.relacl) acl
 WHERE c.oid = :view_oid AND acl.grantee <> c.relowner
"""


@dataclass(frozen=True)
class _DependentView:
    """Всё, что нужно, чтобы поднять вьюшку ровно такой, какой она была."""

    qualified: str
    definition: str
    owner: str
    comment: str | None
    grants: list[tuple[str, str]]


def _dependent_views(conn, table_oid: int, preparer) -> list[_DependentView]:
    views = []
    for oid, _depth, relkind, nsp, rel, definition, owner, comment in conn.execute(
            text(_DEPENDENT_VIEWS), {'table_oid': table_oid}).all():
        if relkind != 'v':
            # Материализованное представление пересоздать нельзя дёшево: это перезаполнение и
            # потеря собственных индексов. Такую таблицу не трогаем вовсе — решение за оператором.
            raise _MaterializedViewInTheWay(f'{nsp}.{rel}')
        grants = conn.execute(text(_VIEW_GRANTS), {'view_oid': oid}).all()
        views.append(_DependentView(
            qualified=f'{preparer.quote_schema(nsp)}.{preparer.quote(rel)}',
            definition=definition, owner=owner, comment=comment,
            grants=[(grantee, privilege) for grantee, privilege in grants]))
    return views


class _MaterializedViewInTheWay(Exception):
    """Над таблицей стоит материализованное представление — миграцию по ней не делаем."""


def _restore_views(conn, views: list[_DependentView], preparer) -> None:
    """Обратно в порядке зависимости: сначала те, на кого смотрят остальные."""
    for view in views:
        conn.execute(text(f'CREATE VIEW {view.qualified} AS {view.definition}'))
        conn.execute(text(f'ALTER VIEW {view.qualified} OWNER TO {preparer.quote(view.owner)}'))
        if view.comment is not None:
            conn.execute(text(f'COMMENT ON VIEW {view.qualified} IS :c'), {'c': view.comment})
        for grantee, privilege in view.grants:
            conn.execute(text(f'GRANT {privilege} ON {view.qualified} '
                              f'TO {preparer.quote(grantee)}'))


def align_merge_timestamps(engine: Engine, schema_name: str | None) -> dict[str, list[str]]:
    """
    Переводит `merged_on`/`inserted_on` таблиц СХЕМЫ в timestamptz. Возвращает {таблица: колонки}.

    Эти таблицы заводит dbmerge, а не мы, поэтому объявления, по которому можно сверить тип, нет —
    обходим схему. Остаётся в библиотеке навсегда, а не как разовый шаг: таблицы появляются
    лениво, по мере появления объектов в 1С, и заведённая старой версией (или поднятая из бэкапа)
    иначе осталась бы наивной. Цена проверки от объёма данных не зависит — единицы миллисекунд
    даже на пятистах таблицах.

    По pg_catalog, а не по information_schema: нужен фильтр relkind — у ВЬЮШЕК тип колонки не
    меняют, он идёт от базовой таблицы. Их мы вместо этого снимаем и ставим обратно, потому что
    иначе ALTER отказывается работать вовсе.

    Одна таблица — одна транзакция: DDL в PostgreSQL транзакционный, поэтому сбой посередине
    возвращает и тип колонки, и все вьюшки на место.
    """
    if engine.dialect.name != 'postgresql':
        return {}
    # Один раз на процесс и схему: за время работы процесса наивных таблиц больше не появится —
    # новые заводятся сразу с поясом (см. DBWriter: data_types для merged_on/inserted_on). А
    # компонентов, зовущих это на старте, несколько, и каждый платил бы своим запросом.
    seen = (str(engine.url), schema_name)
    if seen in _MERGE_TIMESTAMPS_CHECKED:
        return {}
    _MERGE_TIMESTAMPS_CHECKED.add(seen)
    found: dict[str, list[str]] = {}
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT c.oid, c.relname, a.attname
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
              JOIN pg_attribute a ON a.attrelid = c.oid
             WHERE n.nspname = COALESCE(:schema, current_schema())
               AND c.relkind IN ('r', 'p')
               AND a.attnum > 0 AND NOT a.attisdropped
               AND a.attname = ANY(:names)
               AND a.atttypid = 'timestamp'::regtype
             ORDER BY c.relname, a.attname"""),
            {'schema': schema_name, 'names': list(MERGE_TIMESTAMP_FIELDS)}).all()
    oids: dict[str, int] = {}
    for oid, table_name, column_name in rows:
        found.setdefault(table_name, []).append(column_name)
        oids[table_name] = oid

    preparer = engine.dialect.identifier_preparer
    migrated: dict[str, list[str]] = {}
    for table_name, columns in found.items():
        qualified = preparer.quote(table_name)
        if schema_name:
            qualified = f'{preparer.quote_schema(schema_name)}.{qualified}'
        try:
            _timestamptz_alter(engine, qualified, columns, table_oid=oids[table_name])
        except _MaterializedViewInTheWay as blocker:
            # Не отказ всей миграции: остальные таблицы схемы это не касается.
            logger.error("Cannot migrate %s to timestamptz: materialized view %s depends on it. "
                         "Drop or refresh it manually, then restart", qualified, blocker)
            continue
        migrated[table_name] = columns
    return migrated


def create_index_if_absent(engine: Engine, index: Index, table_name: str,
                           schema_name: str | None = None) -> None:
    """CREATE INDEX идемпотентно и без гонки: индекс на одну таблицу заводят все, кто в неё пишет."""
    _create_if_absent(engine, lambda: index.create(engine, checkfirst=True),
                      lambda: any(existing['name'] == index.name for existing
                                  in inspect(engine).get_indexes(table_name, schema=schema_name)),
                      f'index "{index.name}"')



EXCHANGE_NODES = "onecdc_exchange_nodes"
# Аренда узла обмена: имена колонок вынесены, потому что по ним работает общий Lease.
NODE_KEY_FIELD = 'queue_guid'
NODE_OWNER_FIELD = 'reader_owner'
NODE_HEARTBEAT_FIELD = 'reader_heartbeat_at'


def exchange_nodes_table(metadata: MetaData, schema_name: str | None) -> Table:
    """
    Кто сейчас читает изменения этого узла обмена.

    Ключ — узел (`queue_guid`), а НЕ имя плана обмена: очередь со своим ReceivedNo принадлежит
    узлу (см. ChangeReader.get_last_received_no — номер читается из строки узла), и по имени плана
    она не определяется. Два репликатора на разные узлы — независимая работа, и ключ это отражает.

    Отдельная таблица, а не колонки в существующей: состояния уровня «узел обмена» в проекте до
    сих пор не было вовсе — реестр объектов про объекты, handlers про обработчиков.
    """
    return Table(
        EXCHANGE_NODES, metadata,
        Column(NODE_KEY_FIELD, String(64), primary_key=True),
        # Только чтобы строка читалась глазами: ключ и так уникален.
        Column("exchange_name", String(255)),
        Column(NODE_OWNER_FIELD, String(255)),
        Column(NODE_HEARTBEAT_FIELD, DateTime(timezone=True)),
        schema=schema_name,
    )


def _onecdc_replicator_log_table(metadata: MetaData, schema_name: str | None) -> Table:
    return Table(
        REPLICATOR_LOG, metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("exchange", String),
        Column("object", String),
        Column("type", String),
        Column("message_no", Integer),
        Column("started_at", DateTime(timezone=True)),
        Column("finished_at", DateTime(timezone=True), nullable=True),
        Column("inserted_row_count", Integer, nullable=True),
        Column("updated_row_count", Integer, nullable=True),
        Column("deleted_row_count", Integer, nullable=True),
        Column("total_time", Numeric, nullable=True),
        schema=schema_name,
    )


# Сколько дней журнала храним по умолчанию. Строка пишется на КАЖДЫЙ объект КАЖДОГО пакета:
# при опросе раз в минуту и двух десятках объектов это миллион строк в месяц, а миллион строк —
# 95 МБ (измерено). Журнал нужен для разбора недавнего, а не для истории за годы.
DEFAULT_LOG_RETENTION_DAYS = 30

# Партия удаления. Целиком такой DELETE держал бы длинную транзакцию и раздувал таблицу; партиями
# по десять тысяч выходит 254 тысячи строк в секунду (измерено), то есть даже первая уборка на
# запущенной базе — это десятки секунд.
_LOG_CLEANUP_BATCH = 10_000

# Как часто процесс возвращается к уборке. Чаще незачем: удалять сутки строк раз в сутки дешевле,
# чем щупать таблицу каждую минуту.
_LOG_CLEANUP_PERIOD = 24 * 60 * 60.0


class ReplicatorLog:
    """Лог загрузки (onecdc_replicator_log): start() при начале, write_result() накапливает счётчики
    и/или завершает строку (finish=True)."""

    def __init__(self, engine: Engine, schema_name: str | None = None,
                 retention_days: int = DEFAULT_LOG_RETENTION_DAYS):
        self.engine = engine
        self.schema_name = _check_create_schema(engine, schema_name)
        self.table = _onecdc_replicator_log_table(MetaData(), self.schema_name)
        create_table_if_absent(engine, self.table)
        self.retention_days = retention_days
        # Индекс по времени старта нужен и уборке (по нему она находит старое), и мониторингу:
        # «что грузилось ночью» на таблице в миллионы строк иначе полный скан.
        create_index_if_absent(engine, Index(f'ix_{REPLICATOR_LOG}_started_at',
                                             self.table.c.started_at),
                               REPLICATOR_LOG, self.schema_name)
        # Частичный индекс под главный вопрос мониторинга — «что идёт прямо сейчас». Он крошечный:
        # незавершённых строк в норме единицы, а сканировать ради них миллионы завершённых незачем.
        create_index_if_absent(engine, Index(f'ix_{REPLICATOR_LOG}_unfinished',
                                             self.table.c.started_at,
                                             postgresql_where=self.table.c.finished_at.is_(None)),
                               REPLICATOR_LOG, self.schema_name)
        # Уборка сразу: процесс, который ничего не грузит (нет изменений), иначе не убрался бы
        # никогда. Нечего удалять — это индексный поиск, который ничего не находит.
        self._next_cleanup_at = 0.0
        self.cleanup_if_due()

    def cleanup_if_due(self) -> int:
        """
        Уборка не чаще раза в сутки на процесс. Зовётся из start(), то есть из того места, где
        журналом и пользуются: отдельного планировщика ради одного DELETE в сутки заводить незачем,
        а цикл про уборку знать не обязан.

        Отметку следующего раза ставим ДО работы: упавшая уборка (нет прав, заблокирована таблица)
        не должна повторяться на каждой записи журнала. И она не вправе уронить саму загрузку —
        поэтому ошибка только в лог.
        """
        if not self.retention_days or time.monotonic() < self._next_cleanup_at:
            return 0
        self._next_cleanup_at = time.monotonic() + _LOG_CLEANUP_PERIOD
        try:
            return self.cleanup()
        except DatabaseError:
            logger.warning("Could not clean up %s, will retry later", REPLICATOR_LOG,
                           exc_info=True)
            return 0

    def cleanup(self) -> int:
        """
        Удаляет строки старше retention_days. Партиями: целиком такой DELETE держал бы длинную
        транзакцию и раздувал таблицу, а на запущенной базе это миллионы строк.
        """
        cutoff = DB_NOW_WITH_TIMEZONE - timedelta(days=self.retention_days)
        old_ids = (select(self.table.c.id).where(self.table.c.started_at < cutoff)
                   .order_by(self.table.c.id).limit(_LOG_CLEANUP_BATCH).scalar_subquery())
        removed = 0
        while True:
            with self.engine.begin() as conn:
                deleted = conn.execute(
                    self.table.delete().where(self.table.c.id.in_(old_ids))).rowcount
            removed += deleted
            if deleted < _LOG_CLEANUP_BATCH:
                break
        if removed:
            logger.info("Removed %s rows older than %s days from %s", removed,
                        self.retention_days, REPLICATOR_LOG)
        return removed

    def start(self, exchange: str, obj: str, message_no: int | None, load_type: str) -> int:
        self.cleanup_if_due()
        # Счётчики стартуют с нуля — их наращивает write_result (col = col + n) по мере сохранений.
        with self.engine.begin() as conn:
            res = conn.execute(insert(self.table).values(
                exchange=exchange, object=obj, type=load_type, message_no=message_no,
                started_at=func.now(),
                inserted_row_count=0, updated_row_count=0, deleted_row_count=0, total_time=0))
            return res.inserted_primary_key[0]

    def write_result(self, log_id: int, result: mergeResult | None = None,
                     finish: bool = False) -> None:
        """
        Один UPDATE строки лога: прибавляет счётчики merge в БД (col = col + n, если задан result)
        и/или проставляет finished_at (при finish=True).

        Для изменений (одно сохранение на строку) хватает одного write_result(result, finish=True) —
        счётчики и завершение за один запрос. Полная выгрузка накапливает страницы вызовами без
        finish, а в конце ставит завершение отдельным write_result(finish=True).
        """
        t = self.table
        values = {}
        if result is not None:
            values = {
                'inserted_row_count': t.c.inserted_row_count + result.inserted_row_count,
                'updated_row_count': t.c.updated_row_count + result.updated_row_count,
                'deleted_row_count': t.c.deleted_row_count + result.deleted_row_count,
                'total_time': t.c.total_time + result.total_time,
            }
        if finish:
            values['finished_at'] = func.now()
        if not values:
            return
        with self.engine.begin() as conn:
            conn.execute(update(t).where(t.c.id == log_id).values(**values))
