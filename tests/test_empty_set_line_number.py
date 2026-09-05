"""
Оффлайн-тесты ключа фиктивной записи (опустевшая табличная часть / удалённый набор движений).

Номер строки у неё — 1, а не дефолтный 0: 1С нумерует строки набора подряд с единицы, поэтому
наполнившийся набор перезапишет фиктивную запись своей первой строкой, и та не осядет в таблице
навсегда. Остальные поля ключа по-прежнему заполняются дефолтами по типу.
"""

import uuid

from onecdc import DataReader, MetadataReader
from onecdc.data_reader import (EXCHANGE_MESSAGE_NO_FIELD, IS_DELETED_OR_EMPTY_FIELD,
                                LINE_NUMBER_FIELD)
from onecdc.metadata_reader import MetadataObject

REF = uuid.UUID("11111111-1111-1111-1111-111111111111")
REC = "410d24b4-8774-11f1-abae-6cb3117b9496"
PART = "Document_Doc_Tovary"
REG = "AccumulationRegister_Reg"


def _reader(name: str, properties: dict, primary_key: dict) -> DataReader:
    metadata = MetadataReader(odata_url="http://fake")
    metadata[name] = MetadataObject(name, dict(properties), dict(primary_key))
    metadata.is_loaded = True
    reader = DataReader(odata_url="http://fake", metadata=metadata)
    reader.exchange_message_no = 7
    return reader


def test_empty_table_part_record_takes_first_line_number():
    reader = _reader(PART, {"Ref_Key": "Guid", "LineNumber": "Int64", "Tovar": "String"},
                     {"Ref_Key": "Guid", "LineNumber": "Int64"})

    record = reader._make_empty_table_part_record(PART, REF)

    assert record["Ref_Key"] == REF
    assert record[LINE_NUMBER_FIELD] == 1   # реальная строка 1 перезапишет фиктивную
    assert record[IS_DELETED_OR_EMPTY_FIELD] is True
    assert record[EXCHANGE_MESSAGE_NO_FIELD] == 7


def test_deleted_register_record_takes_first_line_number():
    reader = _reader(REG, {"Recorder_Key": "Guid", "LineNumber": "Int64", "Period": "DateTime"},
                     {"Recorder_Key": "Guid", "LineNumber": "Int64"})

    record = reader._make_deleted_register_record(REG, {"d:Recorder_Key": REC})

    assert record["Recorder_Key"] == uuid.UUID(REC)
    assert record[LINE_NUMBER_FIELD] == 1
    assert record[IS_DELETED_OR_EMPTY_FIELD] is True


def test_key_without_line_number_is_untouched():
    # Независимый регистр сведений: номера строки в ключе нет — остальные поля как были, дефолтами.
    reader = _reader(PART, {"Ref_Key": "Guid", "Code": "String"},
                     {"Ref_Key": "Guid", "Code": "String"})

    record = reader._make_empty_table_part_record(PART, REF)

    assert LINE_NUMBER_FIELD not in record
    assert record["Code"] == ''
