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

from dbmerge import mergeResult

from sqlalchemy import (Column, DateTime, Engine, Index, Integer, MetaData, String,
                        Table, func, insert, inspect, text, update, schema, Numeric)
from sqlalchemy.exc import DatabaseError

from onecdc.common_functions import POSTGRES_MAX_IDENTIFIER
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
    existing = {column['name'] for column in inspect(engine).get_columns(
        table.name, schema=table.schema)}
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
        Column(NODE_HEARTBEAT_FIELD, DateTime),
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
        Column("started_at", DateTime),
        Column("finished_at", DateTime, nullable=True),
        Column("inserted_row_count", Integer, nullable=True),
        Column("updated_row_count", Integer, nullable=True),
        Column("deleted_row_count", Integer, nullable=True),
        Column("total_time", Numeric, nullable=True),
        schema=schema_name,
    )


class ReplicatorLog:
    """Лог загрузки (onecdc_replicator_log): start() при начале, write_result() накапливает счётчики
    и/или завершает строку (finish=True)."""

    def __init__(self, engine: Engine, schema_name: str | None = None):
        self.engine = engine
        self.schema_name = _check_create_schema(engine, schema_name)
        self.table = _onecdc_replicator_log_table(MetaData(), self.schema_name)
        create_table_if_absent(engine, self.table)

    def start(self, exchange: str, obj: str, message_no: int | None, load_type: str) -> int:
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
