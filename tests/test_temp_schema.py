"""
Оффлайн-тест схемы промежуточных таблиц (temp_schema).

Промежуточную таблицу merge создаёт dbmerge, и по умолчанию она ложится рядом с данными. Отдельная
схема нужна затем, чтобы этого не происходило: в ней по определению нет ничего ценного, поэтому
таблицу, оставшуюся после падения процесса, там видно и не жалко удалить. Проверяем, что параметр
доезжает от Replicator до dbmerge и что схема при этом создаётся сама.
"""

import pytest
from sqlalchemy import MetaData, Table, inspect, select, text

from onecdc import DataObject, NameMapper
from onecdc.db_writer import DBWriter
from onecdc.metadata_reader import MetadataObject
from onecdc.replicator import Replicator
from conftest import TEST_QUEUE_GUID

REF = "R1"


@pytest.fixture
def temp_schema(db):
    """Отдельная схема под промежуточные таблицы: её создаёт dbmerge, сносим за собой сами."""
    name = f"{db.schema}_tmp"
    try:
        yield name
    finally:
        with db.engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{name}" CASCADE'))


def _save(writer):
    meta = MetadataObject("Catalog_X", {"Ref_Key": "String", "Val": "String"},
                          {"Ref_Key": "String"}, object_key=None)
    record = {"Ref_Key": REF, "Val": "a", "is_deleted_or_empty": False, "exchange_message_no": 1}
    return writer.save("Catalog_X", DataObject(meta, [record]))


def test_merge_uses_temp_schema_and_creates_it(db, temp_schema):
    writer = DBWriter(db.engine, NameMapper(), schema=db.schema, temp_schema=temp_schema)

    _save(writer)

    # Схему создал dbmerge, данные при этом лежат в своей схеме, а промежуточная таблица за собой
    # прибрана — в temp-схеме после успешного merge пусто.
    inspector = inspect(db.engine)
    assert temp_schema in inspector.get_schema_names()
    assert inspector.get_table_names(schema=temp_schema) == []
    table = Table("Catalog_X", MetaData(), schema=db.schema, autoload_with=db.engine)
    with db.engine.connect() as conn:
        assert conn.execute(select(table.c["Ref_Key"])).scalars().all() == [REF]


def test_replicator_passes_temp_schema_to_its_components(db, temp_schema):
    rep = Replicator(odata_url="http://x", odata_auth=None, exchange_name="E",
                     queue_guid=TEST_QUEUE_GUID, engine=db.engine, db_schema=db.schema,
                     db_temp_schema=temp_schema)

    # Через writer идут данные, через metadata — реестр объектов: обоим нужна та же схема.
    assert rep.writer.temp_schema == temp_schema
    assert rep.metadata.temp_schema == temp_schema


def test_temp_schema_defaults_to_data_schema(db):
    # Не задана — поведение прежнее: промежуточная таблица ложится в схему данных (умолчание dbmerge).
    rep = Replicator(odata_url="http://x", odata_auth=None, exchange_name="E",
                     queue_guid=TEST_QUEUE_GUID, engine=db.engine, db_schema=db.schema)

    assert rep.writer.temp_schema is None
    assert rep.metadata.temp_schema is None
