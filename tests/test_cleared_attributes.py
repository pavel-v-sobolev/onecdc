"""
Очистка реквизита в 1С должна доезжать до БД.

Пустой реквизит 1С отдаёт пустым элементом (<d:Артикул/>), xmltodict превращает его в None.
Раньше парсер брал из m:properties только строковые значения, и такое поле не попадало в запись
вовсе. Дальше эффект усиливался: колонки, пустой у ВСЕХ записей пачки, не было в данных, а
dbmerge строит список колонок UPDATE по фактическому составу данных — SET её не трогал.
Результат: в КХД навсегда оставалось старое значение, причём выборочно (та же очистка «срабатывала»,
если по соседству в пачке оказывалась запись с непустым значением этого поля).

Полная выгрузка не лечила: ветка парсера та же. Не замечалось и по счётчикам — версия данных
лежит в skip_compare_fields, поэтому строка считалась вообще не изменившейся.
"""

import xmltodict
from sqlalchemy import MetaData, Table, select

from onecdc import DataObject, NameMapper
from onecdc.data_reader import DataReader
from onecdc.db_writer import DBWriter
from onecdc.metadata_reader import MetadataObject, MetadataReader

OBJ = "Catalog_Номенклатура"
REF = "621d8c1b-e663-11df-aebd-0015e9b8c48d"
REF2 = "621d8c1b-e663-11df-aebd-0015e9b8c48e"

_FIELDS = {"Ref_Key": "Guid", "Description": "String", "Артикул": "String",
           "DataVersion": "String"}
_PRIMARY_KEY = {"Ref_Key": "Guid"}


def _reader() -> DataReader:
    metadata = MetadataReader(odata_url="http://fake")
    metadata[OBJ] = MetadataObject(OBJ, dict(_FIELDS), dict(_PRIMARY_KEY))
    metadata.is_loaded = True
    reader = DataReader(odata_url="http://fake", metadata=metadata)
    reader.exchange_message_no = 1
    return reader


def _properties(xml: str) -> dict:
    """m:properties из куска XML — ровно так, как их получает парсер из ответа 1С."""
    wrapped = (f'<m:properties xmlns:d="d" xmlns:m="m" xmlns:xsi="xsi">{xml}</m:properties>')
    return {k: v for k, v in xmltodict.parse(wrapped)["m:properties"].items()
            if not k.startswith("@")}


def _fields(xml: str) -> dict:
    return _reader()._get_record_fields(_properties(xml), OBJ)


# --- Разбор ---

def test_empty_element_becomes_a_present_null_field():
    # Главный случай: <d:Артикул/> — поле ДОЛЖНО быть в записи, со значением None.
    fields = _fields(f"<d:Ref_Key>{REF}</d:Ref_Key><d:Description>Стол</d:Description>"
                     f"<d:Артикул/>")
    assert "Артикул" in fields, "пустой реквизит потерян — колонки не будет и в UPDATE"
    assert fields["Артикул"] is None


def test_empty_element_written_as_closing_tag_is_the_same_case():
    # <d:Артикул></d:Артикул> xmltodict отдаёт тем же None.
    assert _fields(f"<d:Ref_Key>{REF}</d:Ref_Key><d:Артикул></d:Артикул>")["Артикул"] is None


def test_filled_value_still_parsed():
    assert _fields(f"<d:Ref_Key>{REF}</d:Ref_Key><d:Артикул>A-100</d:Артикул>")["Артикул"] == "A-100"


def test_table_part_is_not_mistaken_for_a_cleared_field():
    # Табличная часть — тоже "пустой" элемент, но с m:type="Collection(...)": колонкой стать
    # не должна, иначе в таблице объекта завелось бы поле под всю ТЧ.
    reader = _reader()
    props = _properties(
        f'<d:Ref_Key>{REF}</d:Ref_Key>'
        f'<d:Представления m:type="Collection(StandardODATA.Catalog_Номенклатура_Представления_RowType)"/>')
    assert "Представления" not in reader._get_record_fields(props, OBJ)
    assert "Представления" in reader._get_record_table_parts(props)


def test_typed_scalar_is_a_field_not_a_table_part():
    # Скаляр с явным типом в xmltodict тоже dict — но это значение, а не табличная часть.
    reader = _reader()
    props = _properties(f'<d:Ref_Key>{REF}</d:Ref_Key>'
                        f'<d:Артикул m:type="Edm.String">A-100</d:Артикул>')
    assert reader._get_record_fields(props, OBJ)["Артикул"] == "A-100"
    assert reader._get_record_table_parts(props) == {}


# --- Сквозной сценарий: запись в БД ---

def _save(db, xml_records: list[str]):
    reader = _reader()
    writer = DBWriter(db.engine, NameMapper(), schema=db.schema)
    records = [_fields(x) for x in xml_records]
    return writer.save(OBJ, DataObject(reader.metadata[OBJ], records))


def _row(db, ref=REF) -> dict:
    tbl = Table("Catalog_Nomenklatura", MetaData(), schema=db.schema, autoload_with=db.engine)
    with db.engine.connect() as conn:
        rows = [dict(r) for r in conn.execute(select(tbl)).mappings()]
    return next(r for r in rows if str(r["Ref_Key"]) == ref)


def test_clearing_the_only_string_field_reaches_the_database(db):
    # Пачка из ОДНОЙ записи: раньше колонки Artikul в данных не было вовсе и UPDATE её не трогал.
    _save(db, [(f"<d:Ref_Key>{REF}</d:Ref_Key><d:Артикул>A-100</d:Артикул>"
                f"<d:DataVersion>v1</d:DataVersion>")])
    before = _row(db)
    assert before["Artikul"] == "A-100"

    result = _save(db, [(f"<d:Ref_Key>{REF}</d:Ref_Key><d:Артикул/>"
                         f"<d:DataVersion>v2</d:DataVersion>")])

    after = _row(db)
    assert after["Artikul"] is None
    assert result.updated_row_count == 1
    # merged_on сдвинулся — иначе обработчик витрины не узнает об изменении.
    assert after["merged_on"] > before["merged_on"]


def test_clearing_does_not_depend_on_the_rest_of_the_batch(db):
    # Дефект был плавающим: очистка «срабатывала» только если у соседа по пачке поле непустое.
    # Обе записи должны очиститься одинаково, в какой бы компании ни пришли.
    _save(db, [f"<d:Ref_Key>{REF}</d:Ref_Key><d:Артикул>A-100</d:Артикул>",
               f"<d:Ref_Key>{REF2}</d:Ref_Key><d:Артикул>B-200</d:Артикул>"])

    _save(db, [f"<d:Ref_Key>{REF}</d:Ref_Key><d:Артикул/>",
               f"<d:Ref_Key>{REF2}</d:Ref_Key><d:Артикул/>"])

    assert _row(db, REF)["Artikul"] is None
    assert _row(db, REF2)["Artikul"] is None
