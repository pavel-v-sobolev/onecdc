"""
Регистраторный регистр, у которого регистратором может быть только один тип документа: 1С отдаёт
поле регистратора как Recorder_Key (Edm.Guid) и не отдаёт Recorder_Type — в отличие от составного
регистратора (Recorder + Recorder_Type).

Проверяем, что такой регистр разбирается как регистраторный (в т.ч. пустой набор = удалённый),
а не как независимый регистр сведений, и что поля ключа не уезжают в NULL.
"""

import uuid
from datetime import datetime

import pytest

from onecdc.data_reader import (DataReader, EMPTY_UUID, EXCHANGE_MESSAGE_NO_FIELD,
                                IS_DELETED_OR_EMPTY_FIELD)
from onecdc.metadata_reader import MetadataObject, MetadataReader

REG = "InformationRegister_Forecast"
REC = "410d24b4-8774-11f1-abae-6cb3117b9496"

# Поля регистра: ключ — регистратор + период + измерение-ссылка; PlannedQuantity вне ключа.
_FIELDS = {"Recorder_Key": "Guid", "Period": "DateTime", "Customer_Key": "Guid",
           "LineNumber": "Int64", "PlannedQuantity": "Int64"}
_PRIMARY_KEY = {"Recorder_Key": "Guid", "Period": "DateTime", "Customer_Key": "Guid"}


def _reader(properties: dict, primary_key: dict | None = None,
            object_key: list[str] | None = None) -> DataReader:
    """DataReader с подставленными метаданными регистра (без сети)."""
    metadata = MetadataReader(odata_url="http://fake")
    metadata[REG] = MetadataObject(REG, dict(properties), dict(primary_key or _PRIMARY_KEY),
                                     object_key=object_key)
    metadata.is_loaded = True
    reader = DataReader(odata_url="http://fake", metadata=metadata)
    reader.exchange_message_no = 7
    return reader


def _row(**overrides) -> dict:
    row = {"d:Recorder_Key": REC, "d:Period": "2026-01-01T00:00:05",
           "d:Customer_Key": "5e51e8e9-6821-11ec-a232-00155de3390c",
           "d:LineNumber": "1", "d:PlannedQuantity": "50"}
    row.update(overrides)
    return row


def test_inactive_record_is_flagged_as_not_counted():
    # Active=False — запись не участвует в итогах 1С, значит и у нас должна быть погашена
    # тем же флагом, что и удалённые: is_deleted_or_empty универсален (см. README_DB.md).
    fields = dict(_FIELDS, Active="Boolean")
    reader = _reader(fields)
    reader._get_register_records(REG, {"d:Recorder_Key": REC, "d:RecordSet": {"d:element": [
        _row(**{"d:Active": "true"}),
        _row(**{"d:LineNumber": "2", "d:Active": "false"}),
    ]}})

    data = reader[REG].data
    assert data["Active"] == [True, False]
    assert data[IS_DELETED_OR_EMPTY_FIELD] == [False, True]


def test_missing_active_does_not_flag_the_record():
    # Active в данных нет (объекты, старые выгрузки) — строку не гасим.
    reader = _reader(dict(_FIELDS, Active="Boolean"))
    reader._get_register_records(REG, {"d:Recorder_Key": REC,
                                       "d:RecordSet": {"d:element": [_row()]}})

    assert reader[REG].data[IS_DELETED_OR_EMPTY_FIELD] == [False]


def test_record_set_with_recorder_key():
    # Непустой набор: движения разбираются как обычно, Recorder_Key приходит из строк набора.
    reader = _reader(_FIELDS)
    reader._get_register_records(REG, {
        "d:Recorder_Key": REC,
        "d:RecordSet": {"@m:type": f"Collection(StandardODATA.{REG}_RowType)",
                        "d:element": [_row()]}})

    data = reader[REG].data
    assert data["Recorder_Key"] == [uuid.UUID(REC)]
    assert data["Period"] == [datetime(2026, 1, 1, 0, 0, 5)]
    assert data[IS_DELETED_OR_EMPTY_FIELD] == [False]
    assert data[EXCHANGE_MESSAGE_NO_FIELD] == [7]


def test_recorder_key_taken_from_entry_when_absent_in_rows():
    # Прямое чтение набора записей: Recorder_Key есть только на уровне entry, внутри строк
    # набора 1С его не повторяет. Без подстановки строки уезжают в БД без ключа.
    reader = _reader(_FIELDS)
    row = _row()
    del row["d:Recorder_Key"]
    reader._get_register_records(REG, {
        "d:Recorder_Key": REC,
        "d:RecordSet": {"d:element": [row, dict(row, **{"d:LineNumber": "2"})]}})

    data = reader[REG].data
    assert data["Recorder_Key"] == [uuid.UUID(REC), uuid.UUID(REC)]
    assert data["LineNumber"] == [1, 2]
    assert data[IS_DELETED_OR_EMPTY_FIELD] == [False, False]


def test_composite_recorder_taken_from_entry_when_absent_in_rows():
    # То же для составного регистратора: в строки набора подставляются и Recorder, и Recorder_Type.
    fields = {"Recorder": "Guid", "Recorder_Type": "String", "Period": "DateTime"}
    reader = _reader(fields, primary_key={"Recorder": "Guid", "Recorder_Type": "String",
                                          "Period": "DateTime"})
    reader._get_register_records(REG, {
        "d:Recorder": REC, "d:Recorder_Type": "StandardODATA.Document_Order",
        "d:RecordSet": {"d:element": [{"d:Period": "2026-01-01T00:00:05"}]}})

    data = reader[REG].data
    assert data["Recorder"] == [uuid.UUID(REC)]
    assert data["Recorder_Type"] == ["Document_Order"]
    assert data["Period"] == [datetime(2026, 1, 1, 0, 0, 5)]


@pytest.mark.parametrize("record_set", [
    {"@m:type": f"Collection(StandardODATA.{REG}_RowType)"},   # <d:RecordSet .../> без элементов
    {"@m:type": f"Collection(StandardODATA.{REG}_RowType)", "d:element": None},
    None,                                                       # <d:RecordSet/>
])
def test_empty_record_set_with_recorder_key_is_deleted(record_set):
    # Пустой набор = набор записей удалён: фиктивная строка с реальным регистратором,
    # полным (не NULL) ключом и is_deleted_or_empty=True — раньше отсюда прилетал Period=NULL.
    reader = _reader(_FIELDS)
    reader._get_register_records(REG, {"d:Recorder_Key": REC, "d:RecordSet": record_set})

    data = reader[REG].data
    assert data["Recorder_Key"] == [uuid.UUID(REC)]
    assert data["Period"] == [datetime(1, 1, 1)]
    assert data["Customer_Key"] == [EMPTY_UUID]
    assert data[IS_DELETED_OR_EMPTY_FIELD] == [True]
    assert data[EXCHANGE_MESSAGE_NO_FIELD] == [7]


def test_empty_record_set_with_composite_recorder_is_deleted():
    # Составной регистратор (Recorder + Recorder_Type) — прежнее поведение сохраняется.
    fields = {"Recorder": "Guid", "Recorder_Type": "String", "Period": "DateTime"}
    reader = _reader(fields, primary_key={"Recorder": "Guid", "Recorder_Type": "String",
                                          "Period": "DateTime"})
    reader._get_register_records(REG, {
        "d:Recorder": REC, "d:Recorder_Type": "StandardODATA.Document_Order",
        "d:RecordSet": {"@m:type": f"Collection(StandardODATA.{REG}_RowType)"}})

    data = reader[REG].data
    assert data["Recorder"] == [uuid.UUID(REC)]
    assert data["Recorder_Type"] == ["Document_Order"]
    assert data["Period"] == [datetime(1, 1, 1)]
    assert data[IS_DELETED_OR_EMPTY_FIELD] == [True]


def test_independent_register_still_flat():
    # Независимый регистр сведений: RecordSet отсутствует, поля лежат прямо в properties.
    fields = {"Period": "DateTime", "Customer_Key": "Guid", "PlannedQuantity": "Int64"}
    reader = _reader(fields, primary_key={"Period": "DateTime", "Customer_Key": "Guid"})
    reader._get_register_records(REG, {
        "d:Period": "2026-01-01T00:00:05",
        "d:Customer_Key": "5e51e8e9-6821-11ec-a232-00155de3390c",
        "d:PlannedQuantity": "50"})

    data = reader[REG].data
    assert data["Period"] == [datetime(2026, 1, 1, 0, 0, 5)]
    assert data[IS_DELETED_OR_EMPTY_FIELD] == [False]


def test_empty_reference_in_key_is_not_null():
    # Пустая ссылка 1С в поле ключа остаётся нулевым GUID (колонка ключа NOT NULL),
    # а вне ключа по-прежнему превращается в NULL.
    fields = dict(_FIELDS, Manager_Key="Guid")
    reader = _reader(fields)
    reader._get_register_records(REG, {
        "d:Recorder_Key": REC,
        "d:RecordSet": {"d:element": [_row(**{
            "d:Customer_Key": "00000000-0000-0000-0000-000000000000",
            "d:Manager_Key": "00000000-0000-0000-0000-000000000000"})]}})

    data = reader[REG].data
    assert data["Customer_Key"] == [EMPTY_UUID]   # в ключе
    assert data["Manager_Key"] == [None]          # вне ключа


def test_object_key_uses_recorder_key():
    # scoped-удаление группы должно идти по Recorder_Key, иначе набор регистратора не заменяется.
    metadata = MetadataReader(odata_url="http://fake")
    assert metadata._get_object_key(REG, _FIELDS, _PRIMARY_KEY) == ["Recorder_Key"]
    assert metadata._get_object_key(
        REG, {"Recorder": "Guid", "Recorder_Type": "String"}, {}) == ["Recorder", "Recorder_Type"]


def test_empty_key_measure_gets_type_default_instead_of_null():
    """
    Пустое измерение в ключе 1С присылает пустым элементом (в json — null) либо не присылает вовсе:
    так приходит «ИдентификаторСтроки» у регистра сумм документов. В целевой таблице поля ключа
    NOT NULL, поэтому без дефолта такая запись роняла вставку ВСЕЙ пачки — регистр не грузился.
    """
    fields = dict(_FIELDS, ИдентификаторСтроки="String")
    primary_key = dict(_PRIMARY_KEY, ИдентификаторСтроки="String")
    reader = _reader(fields, primary_key=primary_key)

    # Поля ИдентификаторСтроки в записи нет вовсе — 1С не прислала пустое измерение.
    reader._get_register_records(REG, {"d:Recorder_Key": REC, "d:RecordSet": {"d:element": [_row()]}})

    assert reader[REG].data["ИдентификаторСтроки"] == ['']
    # Ключ-регистратор при этом по-прежнему приходит из entry, а не забивается нулевым guid.
    assert reader[REG].data["Recorder_Key"] == [uuid.UUID(REC)]
