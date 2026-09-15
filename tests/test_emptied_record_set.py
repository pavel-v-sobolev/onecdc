"""
Опустевший набор движений обязан ГАСИТЬ ресурсы, а не только помечать строки.

Пометка — первая линия обороны: по поднятому `merged_on` витрина замечает, что группа изменилась, и
пересчитывает её. Вторая линия — обнуление числовых ресурсов в `NULL`: `SUM` игнорирует `NULL`,
поэтому итог остаётся верным даже в запросе, забывшем фильтр по `is_deleted_or_empty`. Ровно это
обещает README_DB, и обещание должно выполняться всегда, а не через раз.

Раньше выполнялось через раз: список гашения собирается из полей записи, а фиктивная запись
опустевшего набора несла только ключ. Если рядом в пакете оказывался живой набор того же регистра,
DataObject дополнял надгробие его колонками — и гашение внезапно срабатывало. Приезжало одно
надгробие — суммы распроведённого документа оставались в таблице. Причём именно распроведение
одного документа и даёт пакет с единственным пустым набором (CDC-23).
"""

import xmltodict
from sqlalchemy import MetaData, Table, select

from onecdc import NameMapper
from onecdc.data_reader import DataReader
from onecdc.db_writer import DBWriter
from onecdc.metadata_reader import MetadataObject, MetadataReader

REG = "AccumulationRegister_R"
REC = "6a85159f-8ba8-11dd-89d9-00055dcfc5ca"
OTHER = "7b96260a-9cb9-22ee-9aea-11166edfd6db"

_META = MetadataObject(
    REG, {"Recorder": "Guid", "Recorder_Type": "String", "LineNumber": "Int64",
          "Summa": "Double", "Kolichestvo": "Double", "Kommentarij": "String"},
    {"Recorder": "Guid", "Recorder_Type": "String", "LineNumber": "Int64"},
    object_key=["Recorder", "Recorder_Type"],
    dimensions=[], resources=["Summa", "Kolichestvo", "Kommentarij"])


def _read(entries_xml: str):
    """Разбор как в бою: через read_data_entries, а не сборкой записи руками."""
    metadata = MetadataReader(odata_url="http://fake")
    metadata[REG] = _META
    metadata.is_loaded = True
    metadata.get_metadata = lambda: None
    reader = DataReader(odata_url="http://fake", metadata=metadata)
    reader.exchange_message_no = 1
    feed = xmltodict.parse(f'<feed xmlns="a" xmlns:d="d" xmlns:m="m">{entries_xml}</feed>',
                           force_list=('d:element', 'entry'))['feed']
    reader.read_data_entries(feed['entry'])
    return reader[REG]


def _entry(recorder: str, rows_xml: str = '') -> str:
    """entry регистра: набор движений внутри d:RecordSet; пустой RecordSet = набор удалён."""
    return (f'<entry><category term="StandardODATA.{REG}"/><content><m:properties>'
            f'<d:Recorder>{recorder}</d:Recorder>'
            f'<d:Recorder_Type>StandardODATA.Document_D</d:Recorder_Type>'
            f'<d:RecordSet m:type="Collection(StandardODATA.{REG}_RecordType)">{rows_xml}'
            f'</d:RecordSet></m:properties></content></entry>')


def _row(line: int, summa: int) -> str:
    return (f'<d:element><d:LineNumber>{line}</d:LineNumber><d:Summa>{summa}</d:Summa>'
            f'<d:Kolichestvo>10</d:Kolichestvo>'
            f'<d:Kommentarij>текст</d:Kommentarij></d:element>')


def _rows(writer):
    table = Table(REG, MetaData(), schema=writer.schema, autoload_with=writer.engine)
    with writer.engine.connect() as conn:
        return {(str(r["Recorder"])[:8], r["LineNumber"]): r
                for r in conn.execute(select(table)).mappings()}


def _writer(db):
    return DBWriter(db.engine, NameMapper(), schema=db.schema)


def test_resources_are_cleared_when_the_set_is_emptied_alone(db):
    w = _writer(db)
    w.save(REG, _read(_entry(REC, _row(1, 100) + _row(2, 200))))

    # Документ распровели: набор пуст, и в пакете больше ничего нет.
    w.save(REG, _read(_entry(REC)))

    rows = _rows(w)
    for line in (1, 2):
        row = rows[(REC[:8], line)]
        assert row["is_deleted_or_empty"] is True
        assert row["Summa"] is None, 'сумма распроведённого документа осталась бы в SUM'
        assert row["Kolichestvo"] is None


def test_the_result_does_not_depend_on_what_else_is_in_the_packet(db):
    # Тот же случай, но рядом приехал живой набор другого регистратора. Раньше только здесь
    # гашение и срабатывало — DataObject дополнял надгробие колонками соседа.
    w = _writer(db)
    w.save(REG, _read(_entry(REC, _row(1, 100) + _row(2, 200))))

    w.save(REG, _read(_entry(OTHER, _row(1, 500)) + _entry(REC)))

    rows = _rows(w)
    assert rows[(REC[:8], 1)]["Summa"] is None
    assert rows[(REC[:8], 2)]["Summa"] is None
    # Чужой набор не задет.
    assert rows[(OTHER[:8], 1)]["Summa"] == 500
    assert rows[(OTHER[:8], 1)]["is_deleted_or_empty"] is False


def test_a_string_resource_is_left_alone(db):
    # Строковые ресурсы не гасим: суммировать их некому, а NULL стёр бы содержимое надгробия —
    # по нему видно, что это была за строка.
    w = _writer(db)
    w.save(REG, _read(_entry(REC, _row(1, 100))))

    w.save(REG, _read(_entry(REC)))

    assert _rows(w)[(REC[:8], 1)]["Kommentarij"] == "текст"


def test_a_refilled_set_brings_its_values_back(db):
    # Надгробие не оседает навсегда: номер строки у него 1, и первая настоящая строка его
    # перезаписывает — со своими значениями и снятым флагом.
    w = _writer(db)
    w.save(REG, _read(_entry(REC, _row(1, 100) + _row(2, 200))))
    w.save(REG, _read(_entry(REC)))

    w.save(REG, _read(_entry(REC, _row(1, 700))))

    row = _rows(w)[(REC[:8], 1)]
    assert row["Summa"] == 700
    assert row["is_deleted_or_empty"] is False
