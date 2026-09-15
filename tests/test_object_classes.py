"""
Оффлайн-тесты разбора классов объектов 1С (read_data_entries).

Ссылочные классы (справочники, ПВХ, планы счетов, ПВР, бизнес-процессы, задачи) устроены одинаково
и разбираются общим кодом. Объекты классов, которые мы не умеем сохранять, пропускаются с ошибкой
в логе: пакет изменений подтверждается целиком, поэтому такие изменения теряются безвозвратно.
"""

import logging
from contextlib import contextmanager

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


@contextmanager
def caplog_at_error():
    """Перехват записей уровня ERROR без фикстуры caplog: тест параметризован, и держать в нём
    ещё и фикстуру ради одного списка записей ни к чему."""
    records = []

    class _Catcher(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = _Catcher(level=logging.ERROR)
    logger = logging.getLogger('onecdc')
    logger.addHandler(handler)
    try:
        yield records
    finally:
        logger.removeHandler(handler)


@pytest.mark.parametrize("object_name", [
    "ChartOfCharacteristicTypes_Vidy",
    "ChartOfAccounts_Hozraschetnyi",
    "ChartOfCalculationTypes_Osnovnye",
    "BusinessProcess_Soglasovanie",
    "Task_Poruchenie",
])
def test_unverified_reference_classes_are_not_claimed_as_supported(object_name):
    """
    Планы видов характеристик, планы счетов, планы видов расчёта, бизнес-процессы и задачи из
    поддерживаемых УБРАНЫ.

    Раньше они числились поддерживаемыми, и этот же тест закреплял обратное — что они разбираются
    как справочники. Доказывал он при этом только диспетчеризацию: и метаданные, и entry в нём
    собраны руками из трёх полей. Живых ответов 1С этих классов нет ни в записанных ответах, ни на
    доступном контуре (демо УТ их в OData не публикует), то есть подтвердить обещание нечем.

    Громкий пропуск честнее тихого сохранения неизвестно чего: пользователь увидит CHANGES LOST и
    уберёт объект из плана обмена, а не найдёт через полгода перекошенную таблицу. Класс вернётся
    в SUPPORTED_TYPES вместе с записанными ответами 1С и тестом на них.
    """
    reader = _reader(object_name)

    with caplog_at_error() as records:
        parsed = reader.read_data_entries([_entry(object_name, {"d:Ref_Key": REF})])

    assert object_name not in reader, 'класс не должен сохраняться'
    assert parsed[object_name] == 1, 'но сосчитан быть должен: пакет подтверждается по прочитанным'
    assert any("CHANGES LOST" in r.getMessage() for r in records), 'пропуск обязан быть громким'


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
    #
    # Уровень ERROR, а не WARNING. Формально это состав плана обмена, а не сбой прогона — но цена
    # молча исчезающие изменения: пакет подтверждается целиком, 1С их больше не пришлёт, а дочитать
    # полной выгрузкой нельзя, этот класс мы не умеем разбирать вовсе. Предупреждение раз в пакет
    # никто не читает.
    reader = _reader()

    with caplog.at_level(logging.WARNING):
        parsed = reader.read_data_entries([
            _entry("CalculationRegister_Nachisleniya", {"d:Recorder": REF}),
            _entry("CalculationRegister_Nachisleniya", {"d:Recorder": REF}),
        ])

    assert "CalculationRegister_Nachisleniya" not in reader   # не сохранён
    assert parsed["CalculationRegister_Nachisleniya"] == 2     # но сосчитан
    text = caplog.text
    assert "CHANGES LOST" in text
    assert "2 entries" in text                                 # один лог на пакет, с количеством
    assert "Catalog" in text, 'в сообщении должен быть перечень поддерживаемых классов'
    assert [r.levelname for r in caplog.records if "CHANGES LOST" in r.getMessage()] == ["ERROR"]


def test_catalogs_are_saved_before_documents_and_registers():
    # Документы ссылаются на справочники (по *_Key), регистры — на документы (Recorder), поэтому
    # родителей сохраняем раньше.
    assert save_order_key("Catalog_X") < save_order_key("Document_X")
    assert save_order_key("Document_X") < save_order_key("AccumulationRegister_X")
    # Класс, который мы не сохраняем, в порядке не участвует — он уходит в конец как неизвестный.
    assert save_order_key("Task_X") == save_order_key("Unknown_X")


def test_a_packet_of_only_unsupported_objects_does_not_stall_the_plan(db, monkeypatch, caplog):
    """
    Второй исход того же явления, противоположный первому и куда хуже.

    Раньше подтверждение слалось по числу РАЗОБРАННЫХ объектов, а пакет из одних неподдерживаемых
    классов даёт ноль — и был неотличим от пустого. Тот же SelectChanges уходил каждые 60 секунд,
    очередь стояла забитой, репликация ВСЕГО плана не двигалась, а в логе была одна строка.

    Сохранить эти изменения нельзя ни так, ни эдак, значит остановка ничего не спасает — а стоит
    всего плана. Подтверждаем по числу ПРОЧИТАННЫХ entry.
    """
    from pathlib import Path

    import fake_1c
    from onecdc import Replicator
    from onecdc import change_reader as change_reader_module

    real_parse = change_reader_module.parse_odata

    def all_unsupported(body, root, context, force_list=()):
        parsed = real_parse(body, root, context, force_list)
        if root == 'feed' and isinstance(parsed, dict):
            for entry in parsed.get('entry') or []:
                entry['category']['@term'] = 'StandardODATA.Constant_KursValyuty'
        return parsed

    with fake_1c.running_server(Path(__file__).parent / "responses" / "trade_demo_8.5") as (
            url, fake):
        repl = Replicator(odata_url=url, odata_auth=None, exchange_name="ДляODATA",
                          queue_guid=fake.queue_guid, engine=db.engine, db_schema=db.schema)
        try:
            monkeypatch.setattr(change_reader_module, 'parse_odata', all_unsupported)
            with caplog.at_level(logging.ERROR):
                repl.run_once()

            assert fake.received_no == 1, 'пакет не подтверждён — очередь встала, план стоит'
            assert 'CHANGES LOST' in caplog.text, 'потеря обязана быть громкой'
        finally:
            repl.close()


def test_the_chart_of_characteristic_types_is_read_for_metadata_but_not_loaded(db):
    """
    План видов характеристик — единственный класс, который читается метаданными, но не сохраняется.

    Убрать его из метаданных вместе с остальными было нельзя: по ним регистр бухгалтерии ищет, в
    каком плане лежит вид субконто (DataReader._find_ext_dimension_chart перебирает метаданные), и
    без этого ключом субконто остался бы голый Guid. Отсюда развилка, которую легко потерять при
    следующей правке списка классов, — поэтому она под тестом.
    """
    from conftest import TEST_QUEUE_GUID
    from onecdc import Replicator
    from onecdc.metadata_reader import METADATA_ONLY_TYPES

    chart = "ChartOfCharacteristicTypes_VidySubkonto"
    assert chart.startswith(METADATA_ONLY_TYPES)

    repl = Replicator(odata_url="http://x", odata_auth=None, exchange_name="План",
                      queue_guid=TEST_QUEUE_GUID, engine=db.engine, db_schema=db.schema)
    try:
        repl.metadata[chart] = MetadataObject(chart, {"Ref_Key": "Guid"}, {"Ref_Key": "Guid"})
        repl.metadata["Catalog_X"] = MetadataObject("Catalog_X", {"Ref_Key": "Guid"},
                                                    {"Ref_Key": "Guid"})
        repl.metadata.is_loaded = True

        # Метаданные есть — регистру бухгалтерии есть где искать вид субконто.
        assert chart in repl.metadata

        # Но выгружать его мы не беремся: ни в списке объектов, ни через full_load.
        assert chart not in repl.list_objects()
        assert "Catalog_X" in repl.list_objects()
        with pytest.raises(ValueError, match='metadata only'):
            repl.full_load(chart)
    finally:
        repl.close()
