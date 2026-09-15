"""
Составной тип, в котором лежат не только ссылки: «Дополнительные реквизиты» хранят в Значение и
ссылку на элемент справочника, и число (6.4), и строку — тип конкретного значения приходит в
парном Значение_Type.

Метаданные OData объявляют такое поле строкой и о составе типа молчат, поэтому решать приходится
по каждой записи, глядя в парный _Type.

Само поле хранит значение КАК ПРИШЛО, текстом, всегда. Ссылка дополнительно кладётся в соседнюю
<поле>_Guid — по ней джойнятся ключи других таблиц.

Раньше было наоборот: поле объявлялось uuid, а примитив уходил в соседнюю текстовую колонку. Это
роняло ключ: у примитива в самом поле оставался NULL, NULL в ключе заменяется нулевым guid, и две
записи с разными значениями получали ОДИН ключ — склейка либо падение всей пачки на уникальном
индексе (CDC-14).
"""

import uuid

from onecdc.data_reader import DataReader
from onecdc.metadata_reader import COMPOSITE_GUID_SUFFIX, MetadataObject, MetadataReader

OBJ = "Catalog_Товары_ДополнительныеРеквизиты"
REF = "621d8c1b-e663-11df-aebd-0015e9b8c48d"

_FIELDS = {"Ref_Key": "Guid", "LineNumber": "Int64", "Значение": "String",
           "Значение_Type": "String", "Значение" + COMPOSITE_GUID_SUFFIX: "Guid"}
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


def test_a_reference_is_stored_twice_as_text_and_as_guid():
    fields = _fields(REF, "StandardODATA.Catalog_Номенклатура")
    # Значение как пришло — в самом поле.
    assert fields["Значение"] == REF
    # И оно же ссылкой — в соседней колонке, чтобы джойнилось с ключами других таблиц.
    assert fields["Значение" + COMPOSITE_GUID_SUFFIX] == uuid.UUID(REF)


def test_a_primitive_stays_in_the_field_itself():
    # Число в uuid-колонку не ложилось: раньше здесь падала вставка всей пачки, а после первой
    # починки значение уезжало в соседнюю колонку, оставляя ключ пустым (CDC-14).
    fields = _fields("6.4", "Edm.Double")
    assert fields["Значение"] == "6.4"
    assert fields["Значение" + COMPOSITE_GUID_SUFFIX] is None

    # Строка и дата — так же: тип виден в Значение_Type, значение хранится как есть.
    assert _fields("текст", "Edm.String")["Значение"] == "текст"


def test_two_primitives_do_not_share_a_key():
    """
    Суть CDC-14: поле составного типа бывает и в первичном ключе (у независимых регистров сведений
    — в демо-базе бухгалтерии таких 81), и тогда потеря значения означает потерю идентичности.
    """
    assert _fields("A", "Edm.String")["Значение"] != _fields("B", "Edm.String")["Значение"]


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


def test_the_recorder_is_not_treated_as_a_composite_value():
    """
    Регистратор — тоже поле с парным _Type, но правило составного типа к нему НЕ применяется.

    Регистратором бывает только ссылка на документ, примитива там не бывает никогда, так что
    терять в ключе нечего. А цена применения была бы велика: Recorder входит в первичный ключ
    каждого регистра по регистратору, и смена его типа отправила бы в архив все такие таблицы
    разом, с полной перевыгрузкой каждой.
    """
    from onecdc.metadata_reader import MetadataReader as MR

    reader = MR(odata_url="http://fake")
    item = {'@Name': 'AccumulationRegister_X_RecordType',
            'Property': [{'@Name': 'Recorder', '@Type': 'Edm.String'},
                         {'@Name': 'Recorder_Type', '@Type': 'Edm.String'},
                         {'@Name': 'Объект', '@Type': 'Edm.String'},
                         {'@Name': 'Объект_Type', '@Type': 'Edm.String'}]}
    properties = reader._read_metadata_item_properties(item, 'AccumulationRegister_X')

    assert properties['Recorder'] == 'Guid', 'регистратор остаётся ссылкой'
    assert 'Recorder' + COMPOSITE_GUID_SUFFIX not in properties, 'и без соседней колонки'
    # А обычное составное поле — по новому правилу.
    assert properties['Объект'] == 'String'
    assert properties['Объект' + COMPOSITE_GUID_SUFFIX] == 'Guid'
