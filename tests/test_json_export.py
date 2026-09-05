"""
Оффлайн-тесты DataObject.to_nested_records / group_by — записи объекта с вложенными табличными
частями (для экспорта в JSON). Без 1С/Postgres.
"""

import json
import uuid
from datetime import datetime

import fake_1c  # соседний модуль в tests/
from onecdc import ChangeReader, DataObject, MetadataReader, NameMapper
from onecdc.metadata_reader import MetadataObject

REF_A = uuid.UUID("11111111-1111-1111-1111-111111111111")
REF_B = uuid.UUID("22222222-2222-2222-2222-222222222222")
REC = uuid.UUID("33333333-3333-3333-3333-333333333333")


def _doc_with_parts():
    doc = DataObject(MetadataObject("Document_Doc", {}, {"Ref_Key": "Guid"}, object_key=None), [
        {"Ref_Key": REF_A, "Дата": datetime(2026, 1, 1, 12, 0, 0)},
        {"Ref_Key": REF_B, "Дата": datetime(2026, 1, 2, 12, 0, 0)},
    ])
    part = DataObject(
        MetadataObject("Document_Doc_Товары", {}, {"Ref_Key": "Guid", "LineNumber": "Int64"},
                       object_key=["Ref_Key"]), [
            {"Ref_Key": REF_A, "LineNumber": 1, "Товар": "X"},
            {"Ref_Key": REF_A, "LineNumber": 2, "Товар": "Y"},
            {"Ref_Key": REF_B, "LineNumber": 1, "Товар": "Z"},
        ])
    doc.table_parts = {"Товары": part}   # обычно связывает контейнер при чтении
    return doc


def test_nested_records_raw():
    records = _doc_with_parts().to_nested_records(json_safe=True)
    by_ref = {r["Ref_Key"]: r for r in records}
    # ТЧ вложена списком, строки сгруппированы по Ref_Key.
    assert [r["Товар"] for r in by_ref[str(REF_A)]["Товары"]] == ["X", "Y"]
    assert [r["Товар"] for r in by_ref[str(REF_B)]["Товары"]] == ["Z"]
    # json_safe → datetime сериализован, json.dumps проходит.
    assert by_ref[str(REF_A)]["Дата"] == "2026-01-01T12:00:00"
    json.dumps(records)


def test_flat_without_parts():
    reg = DataObject(
        MetadataObject("AccumulationRegister_Reg", {}, {"Recorder": "Guid"},
                       object_key=["Recorder", "Recorder_Type"]),
        [{"Recorder": REC, "Сумма": 100}])
    records = reg.to_nested_records(json_safe=True)   # table_parts пуст → плоско
    assert records[0]["Сумма"] == 100 and "Товары" not in records[0]
    assert records == reg.to_records_mapped(json_safe=True)


def test_nested_records_mapped():
    records = _doc_with_parts().to_nested_records(NameMapper(), json_safe=True)
    assert "Tovary" in records[0]                     # ключ части транслитерирован
    assert "Data" in records[0]                       # поле Дата -> Data
    assert all("Tovar" in row for row in records[0]["Tovary"])


def test_emptied_part_yields_empty_list():
    # У REF_B табличная часть опустела: пришла фиктивная запись is_deleted_or_empty=True.
    doc = DataObject(MetadataObject("Document_Doc", {}, {"Ref_Key": "Guid"}, object_key=None), [
        {"Ref_Key": REF_A},
        {"Ref_Key": REF_B},
    ])
    part = DataObject(
        MetadataObject("Document_Doc_Товары", {}, {"Ref_Key": "Guid", "LineNumber": "Int64"},
                       object_key=["Ref_Key"]), [
            {"Ref_Key": REF_A, "LineNumber": 1, "Товар": "X"},
            {"Ref_Key": REF_B, "is_deleted_or_empty": True},
        ])
    doc.table_parts = {"Товары": part}
    by_ref = {r["Ref_Key"]: r for r in doc.to_nested_records(json_safe=True)}
    assert [r["Товар"] for r in by_ref[str(REF_A)]["Товары"]] == ["X"]
    assert by_ref[str(REF_B)]["Товары"] == []   # фиктивная запись отброшена → пустой список


def test_group_by():
    part = _doc_with_parts().table_parts["Товары"]
    grouped = part.group_by("Ref_Key", json_safe=True)
    assert set(grouped) == {str(REF_A), str(REF_B)}
    assert len(grouped[str(REF_A)]) == 2 and len(grouped[str(REF_B)]) == 1


def test_over_replay():
    with fake_1c.running_server("tests/responses/trade_demo_8.5") as (odata_url, fake):
        md = MetadataReader(odata_url)
        changes = ChangeReader(odata_url, "E", fake.queue_guid, md)
        md.get_metadata()
        changes.read_changes()                        # первый пакет: номенклатура + её ТЧ

        nom = changes["Catalog_Номенклатура"]
        records = nom.to_nested_records(json_safe=True)   # ТЧ найдены сами (связаны при чтении)

    json.dumps(records)                               # JSON-safe
    assert nom.table_parts                            # ТЧ привязались при чтении
    # у записи справочника табличные части лежат вложенными списками.
    assert all(isinstance(records[0][part_name], list) for part_name in nom.table_parts)
