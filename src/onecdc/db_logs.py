"""
Лог-таблицы пайплайна в БД (общие хелперы на SQLAlchemy Core).

- `replicator_1c_log` — лог загрузки: строка на каждую загрузку объекта. `type` — вид загрузки
  (`changes` — пакет изменений, `full` — полная выгрузка); `message_no` — номер пакета обмена
  (NULL для полной выгрузки); `started_at`/`finished_at` (серверное `func.now()`,
  finished_at=NULL у незавершённой/упавшей); счётчики строк merge и `total_time` наращиваются
  в БД по мере сохранений (см. Replicator1CLog.write_result).

Время берётся серверным `func.now()`. Схема не задана (schema=None) — работаем в схеме БД
по умолчанию (public у PostgreSQL), как это делает и dbmerge.
"""

from dbmerge import mergeResult

from sqlalchemy import (Column, DateTime, Engine, Integer, MetaData, String,
                        Table, func, insert, update, schema, Numeric)

from cdc_1c.logging_config import get_logger

logger = get_logger(__name__)

REPLICATOR_LOG = "replicator_1c_log"

# Тип строки лога: обработка пакета изменений или полная выгрузка.
LOAD_TYPE_CHANGES = 'changes'
LOAD_TYPE_FULL = 'full'


def _check_create_schema(engine: Engine, schema_name: str | None) -> str | None:
    # schema_name=None — работаем в схеме БД по умолчанию, создавать нечего: возвращаем None,
    # не дёргая has_schema/CreateSchema с None.
    if schema_name is None:
        return None
    with engine.begin() as conn:
        if not conn.dialect.has_schema(conn, schema_name):
            logger.info(f"""Creating schema "{schema_name}".""")
            conn.execute(schema.CreateSchema(schema_name))
    return schema_name



def _replicator_log_table(metadata: MetaData, schema_name: str | None) -> Table:
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


class Replicator1CLog:
    """Лог загрузки (replicator_1c_log): start() при начале, write_result() накапливает счётчики
    и/или завершает строку (finish=True)."""

    def __init__(self, engine: Engine, schema_name: str | None = None):
        self.engine = engine
        self.schema_name = _check_create_schema(engine, schema_name)
        self.table = _replicator_log_table(MetaData(), self.schema_name)
        self.table.create(engine, checkfirst=True)

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
