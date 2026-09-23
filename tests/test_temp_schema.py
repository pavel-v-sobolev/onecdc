"""
Оффлайн-тест схемы для временных таблиц (db_temp_schema).

Смысл параметра изменился, и тест — про новый. Промежуточную таблицу merge dbmerge на PostgreSQL
заводит настоящей TEMPORARY, в сессионной схеме, и переданную схему обнуляет сам: туда она больше
не попадает, и мы её туда больше не отправляем.

Осталось одно, зато настоящее: одноразовая таблица КЛЮЧЕЙ полной выгрузки (см. full_load_keys).
TEMPORARY ей не годится — её пишут страницы из разных соединений пула, а читает конец прогона,
тогда как временная таблица видна только своей сессии. Поэтому она обычная (UNLOGGED), и отдельная
схема для неё по-прежнему полезна: в ней нет ничего ценного, и таблицу, оставшуюся после падения
процесса, видно и не жалко удалить.
"""

import pytest
from sqlalchemy import Integer, MetaData, String, Table, inspect, select, text

from onecdc import DataObject, NameMapper
from onecdc.db_writer import DBWriter
from onecdc.full_load_keys import FullLoadKeys
from onecdc.metadata_reader import MetadataObject
from onecdc.replicator import Replicator
from conftest import TEST_QUEUE_GUID

REF = "R1"


@pytest.fixture
def temp_schema(db):
    """Отдельная схема под временные таблицы; сносим за собой сами."""
    name = f"{db.schema}_tmp"
    with db.engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{name}"'))
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


def test_the_staging_table_of_a_merge_lands_nowhere(db, temp_schema):
    """
    dbmerge заводит её настоящей TEMPORARY: она живёт в сессионной схеме и исчезает с сессией.
    Раньше параметр доезжал до dbmerge и тот клал таблицу в указанную схему — теперь нет.
    """
    writer = DBWriter(db.engine, NameMapper(), schema=db.schema, temp_schema=temp_schema)

    _save(writer)

    inspector = inspect(db.engine)
    assert inspector.get_table_names(schema=temp_schema) == [], 'промежуточная таблица сюда не идёт'
    # А данные на месте, в своей схеме.
    table = Table("Catalog_X", MetaData(), schema=db.schema, autoload_with=db.engine)
    with db.engine.connect() as conn:
        assert conn.execute(select(table.c["Ref_Key"])).scalars().all() == [REF]


def test_the_keys_table_of_a_full_load_lives_in_the_temp_schema(db, temp_schema):
    """
    Ради этого параметр и остался. Таблица одноразовая: её видно на время прогона и не видно
    после — в том числе при ошибке, поэтому контекстный менеджер.
    """
    keys = FullLoadKeys(db.engine, target_table_name="Catalog_X",
                        key_columns={"Ref_Key": String()}, schema=temp_schema)

    with keys:
        tables = inspect(db.engine).get_table_names(schema=temp_schema)
        assert tables == [keys.name]
        assert keys.name.startswith('tmpkeys_'), 'имя должно быть разбираемым глазами'

    assert inspect(db.engine).get_table_names(schema=temp_schema) == [], 'таблица не прибрана'


def test_a_full_load_puts_its_keys_where_asked(db, temp_schema):
    rep = Replicator(odata_url="http://x", odata_auth=None, engine=db.engine,
                     exchange_name="E", queue_guid=TEST_QUEUE_GUID,
                     db_schema=db.schema, db_temp_schema=temp_schema)
    rep.metadata.is_loaded = True
    rep.metadata["Catalog_X"] = MetadataObject("Catalog_X", {"Ref_Key": "Guid"},
                                               {"Ref_Key": "Guid"})

    assert rep._full_load_keys("Catalog_X").schema == temp_schema
    rep.close()


def test_without_it_the_keys_table_goes_to_the_data_schema(db):
    """Не задана — кладём рядом с данными: параметр не обязателен, это гигиена, а не механика."""
    rep = Replicator(odata_url="http://x", odata_auth=None, engine=db.engine,
                     exchange_name="E", queue_guid=TEST_QUEUE_GUID, db_schema=db.schema)
    rep.metadata.is_loaded = True
    rep.metadata["Catalog_X"] = MetadataObject("Catalog_X", {"Ref_Key": "Guid"},
                                               {"Ref_Key": "Guid"})

    assert rep._full_load_keys("Catalog_X").schema == db.schema
    rep.close()
