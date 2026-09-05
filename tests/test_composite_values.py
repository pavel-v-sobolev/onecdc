"""
Составной тип, в котором лежат не только ссылки: «Дополнительные реквизиты» хранят в Значение и
ссылку на элемент справочника, и число (6.4), и строку — тип конкретного значения приходит в
парном Значение_Type.

Метаданные OData объявляют такое поле строкой и о составе типа молчат, поэтому библиотека считает
его ссылочным (см. GUESS_UUID_TYPES) и заводит uuid-колонку. Раньше число в неё не лезло: значение
уходило в БД как есть и роняло вставку ВСЕЙ пачки — объект не грузился вовсе. Теперь ссылки
остаются в uuid-колонке (джойны с другими таблицами не ломаются), а прочие типы уходят в соседнюю
текстовую <поле>_Value.
"""

import uuid

from onecdc.data_reader import DataReader
from onecdc.metadata_reader import COMPOSITE_VALUE_SUFFIX, MetadataObject, MetadataReader

OBJ = "Catalog_Товары_ДополнительныеРеквизиты"
REF = "621d8c1b-e663-11df-aebd-0015e9b8c48d"

_FIELDS = {"Ref_Key": "Guid", "LineNumber": "Int64", "Значение": "Guid",
           "Значение_Type": "String", "Значение" + COMPOSITE_VALUE_SUFFIX: "String"}
_PRIMARY_KEY = {"Ref_Key": "Guid", "LineNumber": "Int64"}


def _reader() -> DataReader:
    metadata = MetadataReader(odata_url="http://fake")
    metadata[OBJ] = MetadataObject(OBJ, dict(_FIELDS), dict(_PRIMARY_KEY))
    metadata.is_loaded = True
    reader = DataReader(odata_url="http://fake", metadata=metadata)
    reader.exchange_message_no = 1
    return reader


def _fields(value: str, value_type: str) -> dict:
    return _reader()._get_record_fields(
        {"d:Ref_Key": REF, "d:LineNumber": "1", "d:Значение": value, "d:Значение_Type": value_type},
        OBJ)


def test_reference_value_stays_in_uuid_column():
    fields = _fields(REF, "StandardODATA.Catalog_Номенклатура")
    assert fields["Значение"] == uuid.UUID(REF)
    assert fields["Значение" + COMPOSITE_VALUE_SUFFIX] is None


def test_primitive_value_goes_to_sibling_column():
    # Число в uuid-колонку не ложится: раньше здесь падала вставка всей пачки.
    fields = _fields("6.4", "Edm.Double")
    assert fields["Значение"] is None
    assert fields["Значение" + COMPOSITE_VALUE_SUFFIX] == "6.4"

    # Строка и дата — так же: тип виден в Значение_Type, значение хранится как есть.
    assert _fields("текст", "Edm.String")["Значение" + COMPOSITE_VALUE_SUFFIX] == "текст"


def test_unconvertible_value_becomes_null_instead_of_breaking_the_batch():
    # Поле объявлено ссылкой, парного _Type нет, значение не guid — молча ронять загрузку объекта
    # нельзя: пишем NULL, а в логе остаётся объект, поле и значение.
    metadata = MetadataReader(odata_url="http://fake")
    metadata[OBJ] = MetadataObject(OBJ, {"Ref_Key": "Guid", "Значение": "Guid"},
                                   {"Ref_Key": "Guid"})
    metadata.is_loaded = True
    reader = DataReader(odata_url="http://fake", metadata=metadata)
    reader.exchange_message_no = 1

    fields = reader._get_record_fields({"d:Ref_Key": REF, "d:Значение": "не guid"}, OBJ)
    assert fields["Значение"] is None
