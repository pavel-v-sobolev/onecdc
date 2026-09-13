"""
Поле неизвестного типа не выбрасывается, а хранится строкой.

Выбрасывание стоило дорого и не там, где кажется. `_read_metadata_item_key` берёт из объявленного
`Key` только те поля, что уцелели в `properties`, — поэтому поле ключа неизвестного типа выпадало
и из ключа тоже, молча. Дальше было два исхода, оба тихие:

- ключ пуст целиком: DBWriter.save возвращает None, а None означает ещё и «сохранять нечего», —
  оркестратор не различает, закрывает строку журнала и ПОДТВЕРЖДАЕТ пакет. Данные исчезают, в
  1С их больше нет, в логе одна строка WARNING;
- ключ усечён частично: две разные записи 1С получают один ключ, и в следующем пакете вторая
  затирает первую.

Строкой хранить безопасно: в m:properties 1С всё равно присылает текст, приводить его не к чему,
и упасть на вставке нечем — а данные сохраняются вместо того, чтобы пропасть.
"""

import logging

import pytest

from onecdc.metadata_reader import MetadataReader

# Тип, которого нет в type_mapping. Реальный кандидат — новый Edm.-тип будущей версии платформы
# либо нетиповое измерение регистра.
UNKNOWN_IN_KEY = """
  <EntityType Name="InformationRegister_Prices">
    <Key><PropertyRef Name="Product_Key"/><PropertyRef Name="Slot"/></Key>
    <Property Name="Product_Key" Type="Edm.Guid" Nullable="false"/>
    <Property Name="Slot" Type="Edm.Time" Nullable="false"/>
    <Property Name="Price" Type="Edm.Double" Nullable="true"/>
  </EntityType>
"""

UNKNOWN_IN_ATTRIBUTE = """
  <EntityType Name="Catalog_Tovary">
    <Key><PropertyRef Name="Ref_Key"/></Key>
    <Property Name="Ref_Key" Type="Edm.Guid" Nullable="false"/>
    <Property Name="Vremya" Type="Edm.Time" Nullable="true"/>
  </EntityType>
"""

# Ключ ссылается на поле, которого в Property нет вовсе — метаданные сломаны.
BROKEN_KEY = """
  <EntityType Name="InformationRegister_Broken">
    <Key><PropertyRef Name="Ref_Key"/><PropertyRef Name="Nowhere"/></Key>
    <Property Name="Ref_Key" Type="Edm.Guid" Nullable="false"/>
  </EntityType>
"""


def _metadata(*blocks: str) -> MetadataReader:
    """MetadataReader с подставленным ответом `$metadata` (сети нет)."""
    xml = ('<?xml version="1.0" encoding="UTF-8"?>'
           '<edmx:Edmx xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx">'
           '<edmx:DataServices><Schema>' + ''.join(blocks) + '</Schema>'
           '</edmx:DataServices></edmx:Edmx>')

    class _Response:
        ok = True
        status_code = 200
        text = xml
        content = xml.encode()

    reader = MetadataReader(odata_url="http://fake")
    import onecdc.metadata_reader as module
    original = module.requests.get
    module.requests.get = lambda *a, **kw: _Response()
    try:
        reader.get_metadata()
    finally:
        module.requests.get = original
    return reader


def test_unknown_type_in_the_key_keeps_the_key_whole():
    obj = _metadata(UNKNOWN_IN_KEY)["InformationRegister_Prices"]

    assert obj.primary_key == {"Product_Key": "Guid", "Slot": "String"}, \
        'поле ключа выпало — строки объекта нечем различать'


def test_unknown_type_in_an_attribute_becomes_a_text_column():
    obj = _metadata(UNKNOWN_IN_ATTRIBUTE)["Catalog_Tovary"]

    # Раньше поле выбрасывалось вместе со значением: колонки нет, данные пропали молча.
    assert obj["Vremya"] == "String"
    assert set(obj) == {"Ref_Key", "Vremya"}


def test_unknown_type_is_reported_as_a_warning(caplog):
    # WARNING, а не ERROR: данные сохранены, но это повод добавить тип в type_mapping, пока
    # колонок с ним мало — переехать потом не получится, dbmerge умеет только ADD COLUMN.
    with caplog.at_level(logging.WARNING):
        _metadata(UNKNOWN_IN_ATTRIBUTE)

    messages = [r.message for r in caplog.records if 'Vremya' in r.message]
    assert messages, 'о неизвестном типе надо сообщить'
    assert 'Edm.Time' in messages[0] and 'String' in messages[0]


def test_known_types_are_untouched():
    obj = _metadata(UNKNOWN_IN_KEY)["InformationRegister_Prices"]

    assert obj["Product_Key"] == "Guid"
    assert obj["Price"] == "Double"


# --- Остаточный случай: метаданным нельзя доверять ---

def test_a_key_that_cannot_be_parsed_whole_is_reported_as_an_error(caplog):
    # После того как неизвестный тип стал строкой, поле ключа может не найтись только у
    # сломанных метаданных: Key ссылается на необъявленное поле. Это не свойство данных,
    # а поломка источника — и молчать о ней нельзя, иначе вернётся ровно та же тихая потеря.
    with caplog.at_level(logging.ERROR):
        obj = _metadata(BROKEN_KEY)["InformationRegister_Broken"]

    assert obj.primary_key == {"Ref_Key": "Guid"}
    errors = [r.message for r in caplog.records if r.levelno >= logging.ERROR]
    assert any('Nowhere' in m for m in errors), 'усечённый ключ должен быть виден'


def test_a_complete_key_says_nothing(caplog):
    with caplog.at_level(logging.ERROR):
        _metadata(UNKNOWN_IN_KEY)

    assert [r.message for r in caplog.records if r.levelno >= logging.ERROR] == []


# --- Сквозной сценарий: пакет больше не подтверждается впустую ---

def test_an_object_whose_key_has_an_unknown_type_is_still_saved(db, monkeypatch):
    """
    Сквозной сценарий на боевом пайплайне. Подсовываем ключу номенклатуры тип, которого библиотека
    не знает.

    Раньше: поле выпадало из properties → выпадало из ключа → ключ пуст → save молча вернул None →
    строка журнала закрыта как успешная → пакет ПОДТВЕРЖДЁН. Данных нет ни в КХД, ни в очереди 1С.
    """
    from pathlib import Path

    import fake_1c
    from sqlalchemy import inspect

    from onecdc import Replicator

    original = MetadataReader._read_metadata_item_properties

    def key_of_an_unknown_type(self, item, item_name=None):
        if (item_name or item.get('@Name')) == 'Catalog_Номенклатура':
            for prop in item.get('Property') or []:
                if prop['@Name'] == 'Ref_Key':
                    prop['@Type'] = 'Edm.Time'
        return original(self, item, item_name)

    monkeypatch.setattr(MetadataReader, '_read_metadata_item_properties', key_of_an_unknown_type)

    with fake_1c.running_server(Path(__file__).parent / "responses" / "trade_demo_8.5") as (
            url, fake):
        repl = Replicator(odata_url=url, odata_auth=None, exchange_name="ДляODATA",
                          queue_guid=fake.queue_guid, engine=db.engine, db_schema=db.schema)
        assert repl.metadata is not None
        repl.run_once()

        assert repl.metadata['Catalog_Номенклатура'].primary_key == {'Ref_Key': 'String'}
        tables = inspect(db.engine).get_table_names(schema=db.schema)
        assert 'Catalog_Nomenklatura' in tables, \
            'объект не сохранён — а пакет при этом подтверждён, и в 1С его больше нет'
        assert fake.received_no == 1, 'пакет должен подтверждаться ПОСЛЕ успешного сохранения'


@pytest.mark.parametrize("declared", ["Edm.Time", "Edm.Decimal", "Edm.DateTimeOffset",
                                      "StandardODATA.SomeStructure"])
def test_any_unrecognised_type_still_yields_a_column(declared):
    # Ветка одна на все незнакомые типы: и на будущие Edm.-примитивы, и на структуры. Структура
    # заведёт пустую колонку — маршрутизация в парсере идёт по значению, а не по метаданным, —
    # но ключ остаётся целым в любом случае, а это здесь главное.
    block = f"""
      <EntityType Name="Catalog_X">
        <Key><PropertyRef Name="Ref_Key"/></Key>
        <Property Name="Ref_Key" Type="Edm.Guid" Nullable="false"/>
        <Property Name="Pole" Type="{declared}" Nullable="true"/>
      </EntityType>
    """
    assert _metadata(block)["Catalog_X"]["Pole"] == "String"
