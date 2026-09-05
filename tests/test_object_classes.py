"""
Оффлайн-тесты разбора классов объектов 1С (read_data_entries).

Ссылочные классы (справочники, ПВХ, планы счетов, ПВР, бизнес-процессы, задачи) устроены одинаково
и разбираются общим кодом. Объекты классов, которые мы не умеем сохранять, пропускаются с ошибкой
в логе: пакет изменений подтверждается целиком, поэтому такие изменения теряются безвозвратно.
"""

import logging

import pytest

from onecdc import DataReader, MetadataReader
from onecdc.data_reader import IS_DELETED_OR_EMPTY_FIELD
from onecdc.db_writer import save_order_key
from onecdc.metadata_reader import MetadataObject

REF = "5e51e8e9-6821-11ec-a232-00155de3390c"


def _entry(object_full_name: str, properties: dict) -> dict:
    return {"category": {"@term": f"StandardODATA.{object_full_name}"},
            "content": {"m:properties": properties}}


def _reader(*objects: str) -> DataReader:
    metadata = MetadataReader(odata_url="http://fake")
    for name in objects:
        metadata[name] = MetadataObject(
            name, {"Ref_Key": "Guid", "Description": "String", "DeletionMark": "Boolean"},
            {"Ref_Key": "Guid"})
    metadata.is_loaded = True
    reader = DataReader(odata_url="http://fake", metadata=metadata)
    reader.exchange_message_no = 7
    return reader


@pytest.mark.parametrize("object_name", [
    "ChartOfCharacteristicTypes_Vidy",
    "ChartOfAccounts_Hozraschetnyi",
    "ChartOfCalculationTypes_Osnovnye",
    "BusinessProcess_Soglasovanie",
    "Task_Poruchenie",
])
def test_reference_classes_are_parsed_like_catalogs(object_name):
    reader = _reader(object_name)

    reader.read_data_entries([_entry(object_name, {"d:Ref_Key": REF, "d:Description": "X",
                                                   "d:DeletionMark": "false"})])

    data = reader[object_name].data
    assert data["Description"] == ["X"]
    assert data[IS_DELETED_OR_EMPTY_FIELD] == [False]


def test_accounting_register_is_parsed_like_accumulation_register(caplog):
    # Регистр бухгалтерии в OData устроен как регистраторный регистр накопления: Recorder на уровне
    # entry + коллекция RecordSet, поэтому разбирается тем же кодом.
    name = "AccountingRegister_Hozraschetnyi"
    metadata = MetadataReader(odata_url="http://fake")
    metadata[name] = MetadataObject(
        name, {"Recorder": "Guid", "Recorder_Type": "String", "LineNumber": "Int64",
               "AccountDr_Key": "Guid", "AccountCr_Key": "Guid", "Summa": "Double"},
        {"Recorder": "Guid", "LineNumber": "Int64", "Recorder_Type": "String"},
        ["Recorder", "Recorder_Type"])
    metadata.is_loaded = True
    reader = DataReader(odata_url="http://fake", metadata=metadata)
    reader.exchange_message_no = 7

    with caplog.at_level(logging.ERROR):
        reader.read_data_entries([_entry(name, {
            "d:Recorder": REF,
            "d:Recorder_Type": "StandardODATA.Document_AvansovyjOtchet",
            "d:RecordSet": {"d:element": [{"d:LineNumber": "1", "d:AccountDr_Key": REF,
                                           "d:AccountCr_Key": REF, "d:Summa": "1650"}]},
        })])

    data = reader[name].data
    assert data["LineNumber"] == [1]
    assert data["Summa"] == [1650]
    # Регистратор лежит на уровне entry и в строки набора проставляется разбором.
    assert [str(v) for v in data["Recorder"]] == [REF]
    assert data["Recorder_Type"] == ["Document_AvansovyjOtchet"]
    assert "unsupported" not in caplog.text


def test_unsupported_class_is_skipped_and_reported(caplog):
    # Регистр расчёта не поддерживается: у него своя структура записи (периоды действия, вытеснение).
    # Уровень WARNING, а не ERROR: пакет всё равно подтверждается, цикл репликации не останавливается.
    reader = _reader()

    with caplog.at_level(logging.WARNING):
        parsed = reader.read_data_entries([
            _entry("CalculationRegister_Nachisleniya", {"d:Recorder": REF}),
            _entry("CalculationRegister_Nachisleniya", {"d:Recorder": REF}),
        ])

    assert "CalculationRegister_Nachisleniya" not in reader   # не сохранён
    assert parsed["CalculationRegister_Nachisleniya"] == 2     # но сосчитан
    text = caplog.text
    assert "unsupported" in text and "lost" in text
    assert "2 entries" in text                                 # один лог на пакет, с количеством
    assert [r.levelname for r in caplog.records if "unsupported" in r.getMessage()] == ["WARNING"]


def test_reference_classes_are_saved_before_documents():
    # Документы ссылаются на ссылочные объекты, регистры — на документы.
    assert save_order_key("ChartOfAccounts_X") < save_order_key("Document_X")
    assert save_order_key("Task_X") < save_order_key("Document_X")
    assert save_order_key("Document_X") < save_order_key("AccumulationRegister_X")
