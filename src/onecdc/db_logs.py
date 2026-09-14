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
                        Table, func, insert, inspect, update, schema, Numeric)
from sqlalchemy.exc import DatabaseError

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


def _check_create_schema(engine: Engine, schema_name: str | None) -> str | None:
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


def create_table_if_absent(engine: Engine, table: Table) -> None:
    """CREATE TABLE идемпотентно и без гонки на старте нескольких репликаторов (см. _create_if_absent)."""
    _create_if_absent(engine, lambda: table.create(engine, checkfirst=True),
                      lambda: inspect(engine).has_table(table.name, schema=table.schema),
                      f'table "{table.name}"')


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
