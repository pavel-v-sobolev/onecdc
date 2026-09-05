"""
Оффлайн-тесты расписания полной выгрузки (FullLoadCron). Без живой 1С: метаданные подставляются
в репликатор готовыми, full_load подменяется записывающей заглушкой. Проверяются разрешение имён
(латиница из БД ↔ имена 1С), вычисление границ периода, claim от параллельной выгрузки того же
объекта и поведение самого цикла (остановка, живучесть после ошибки).
"""

from datetime import date, datetime, timedelta

import pytest

from onecdc import FullLoadCron
from onecdc.metadata_reader import MetadataObject
from onecdc.replicator import Replicator
from onecdc.stop_signal import StopSignal
from conftest import TEST_QUEUE_GUID

OBJECT_1C = "Document_ЗаказКлиента"
TABLE = "Document_ZakazKlienta"


def _replicator(db, calls, rows_modified=0, fully_loaded=True):
    """
    Репликатор с готовыми метаданными и заглушкой full_load, пишущей вызовы в calls.

    fully_loaded — выгружался ли объект целиком раньше. По умолчанию да: первый прогон читает
    объект целиком, игнорируя период (см. FullLoadCron), и тесты границ проверяли бы не то.
    """
    rep = Replicator(odata_url="http://x", odata_auth=None, exchange_name="E",
                     queue_guid=TEST_QUEUE_GUID, engine=db.engine, db_schema=db.schema)
    rep.metadata.is_loaded = True
    rep.metadata.was_fully_loaded = lambda object_name: fully_loaded
    properties = {"Ref_Key": "Guid", "Date": "DateTime", "ДатаОтгрузки": "DateTime"}
    rep.metadata[OBJECT_1C] = MetadataObject(OBJECT_1C, properties, {"Ref_Key": "Guid"})
    # Реестр объектов: в бою его создаёт первая синхронизация метаданных, и без него не работает
    # захват объекта под выгрузку (см. full_load_claim).
    rep.metadata._sync_objects([OBJECT_1C])

    def fake_full_load(object_name, batch_size=1000, date_field=None,
                       date_from=None, date_to=None, **kwargs):
        calls.append({"object_name": object_name, "batch_size": batch_size,
                      "date_field": date_field, "date_from": date_from, "date_to": date_to,
                      **kwargs})
        return rows_modified

    rep.full_load = fake_full_load
    return rep


def test_resolves_table_name_to_1c_name(db):
    # Настраивают расписание по тому, что видно в БД, — латиницей; в 1С уходит имя 1С.
    calls = []
    cron = FullLoadCron(_replicator(db, calls), TABLE, cron="0 3 * * *")
    cron.run_once()
    assert calls[0]["object_name"] == OBJECT_1C
    assert calls[0]["date_field"] is None


def test_accepts_1c_names_as_is(db):
    # Кому привычнее имена 1С — пишет их: транслитерация не единственный вход.
    calls = []
    cron = FullLoadCron(_replicator(db, calls), OBJECT_1C, cron="0 3 * * *",
                        date_field="ДатаОтгрузки", date_to=date(2026, 6, 30))
    cron.run_once()
    assert calls[0]["object_name"] == OBJECT_1C
    assert calls[0]["date_field"] == "ДатаОтгрузки"


def test_resolves_transliterated_date_field(db):
    calls = []
    cron = FullLoadCron(_replicator(db, calls), TABLE, cron="0 3 * * *",
                        date_field="DataOtgruzki", date_from=date(2026, 6, 1))
    cron.run_once()
    assert calls[0]["date_field"] == "ДатаОтгрузки"


def test_unknown_names_raise(db):
    calls = []
    rep = _replicator(db, calls)
    with pytest.raises(ValueError, match="not found in 1C metadata"):
        FullLoadCron(rep, "Document_NoSuchThing", cron="0 3 * * *").run_once()
    with pytest.raises(ValueError, match="not found in Document"):
        FullLoadCron(rep, TABLE, cron="0 3 * * *", date_field="NoSuchField").run_once()
    assert calls == []


def test_sliding_window_is_computed_at_run_time(db):
    # timedelta — смещение назад от сегодняшней даты, поэтому окно едет вместе с процессом.
    calls = []
    cron = FullLoadCron(_replicator(db, calls), TABLE, cron="0 3 * * *",
                        date_field="Date", date_from=timedelta(days=3))
    cron.run_once()
    assert calls[0]["date_from"] == date.today() - timedelta(days=3)
    assert calls[0]["date_to"] is None


def test_fixed_bounds_are_passed_as_is(db):
    calls = []
    cron = FullLoadCron(_replicator(db, calls), TABLE, cron="0 3 * * *", date_field="Date",
                        date_from=date(2026, 6, 1), date_to=datetime(2026, 6, 30, 23, 59, 59))
    cron.run_once()
    assert calls[0]["date_from"] == date(2026, 6, 1)
    assert calls[0]["date_to"] == datetime(2026, 6, 30, 23, 59, 59)


def test_marking_is_not_configurable(db):
    # Пометкой пропавших строк расписание не управляет вовсе: выключить её значило бы тихо сломать
    # витрины, а удаления у независимого регистра больше видеть неоткуда.
    with pytest.raises(TypeError):
        FullLoadCron(_replicator(db, []), TABLE, cron="0 3 * * *", mark_missing=False)


def test_first_run_reads_everything_ignoring_the_period(db):
    # Объект, которого нет в плане обмена, в пакете изменений не появляется, поэтому полную
    # выгрузку по флагу ему никто не закажет (require_full_load_if_new зовут только из пакета).
    # Базу собирает первый прогон расписания — и границы на нём не применяются, иначе скользящее
    # окно взяло бы один свежий хвост, а история не приехала бы никогда.
    calls = []
    rep = _replicator(db, calls, fully_loaded=False)
    cron = FullLoadCron(rep, TABLE, cron="0 3 * * *",
                        date_field="Date", date_from=timedelta(days=3))
    cron.run_once()
    assert calls[0]["date_from"] is None and calls[0]["date_to"] is None

    # Дальше — обычный режим: отметка о выгрузке лежит в БД, поэтому перезапуск процесса историю
    # заново не перечитывает.
    rep.metadata.was_fully_loaded = lambda object_name: True
    cron.run_once()
    assert calls[1]["date_from"] == date.today() - timedelta(days=3)


def test_first_run_is_recorded_so_it_happens_once(db):
    # full_load реестр не трогает — отметку ставит фоновый воркер. Расписание, собравшее базу само,
    # обязано отметиться, иначе читало бы всю историю на КАЖДОМ срабатывании.
    calls = []
    rep = _replicator(db, calls, fully_loaded=False)
    marked = []
    rep.metadata.mark_full_loaded = lambda name, **kw: marked.append((name, kw))

    FullLoadCron(rep, TABLE, cron="0 3 * * *", date_field="Date",
                 date_from=timedelta(days=3)).run_once()

    assert calls[0]["date_from"] is None, 'первый прогон — объект целиком'
    assert marked and marked[0][0] == OBJECT_1C, 'и он отмечен как полная выгрузка'


def test_period_run_does_not_claim_to_be_a_full_load(db):
    # Обычный прогон за период объект целиком не читал: объявив его выгруженным, он отменил бы
    # фоновую выгрузку, которая базу ещё не собрала.
    calls = []
    rep = _replicator(db, calls, fully_loaded=True)
    marked = []
    rep.metadata.mark_full_loaded = lambda name, **kw: marked.append(name)

    FullLoadCron(rep, TABLE, cron="0 3 * * *", date_field="Date",
                 date_from=timedelta(days=3)).run_once()

    assert calls[0]["date_from"] == date.today() - timedelta(days=3)
    assert marked == [], 'реестр не трогаем'


def test_first_run_keeps_the_period_when_asked(db):
    # История бывает и не нужна: объект огромный, а интересен только свежий хвост.
    calls = []
    rep = _replicator(db, calls, fully_loaded=False)
    FullLoadCron(rep, TABLE, cron="0 3 * * *",
                 date_field="Date", date_from=timedelta(days=3),
                 full_history_on_first_run=False).run_once()
    assert calls[0]["date_from"] == date.today() - timedelta(days=3)


def test_first_run_without_a_period_is_an_ordinary_run(db):
    # Расписание без границ и так читает объект целиком — флагу нечего снимать.
    calls = []
    FullLoadCron(_replicator(db, calls, fully_loaded=False), TABLE, cron="0 3 * * *").run_once()
    assert calls[0]["date_from"] is None and calls[0]["date_to"] is None


def test_config_errors_raise_in_constructor(db):
    rep = _replicator(db, [])
    with pytest.raises(ValueError, match="crontab"):
        FullLoadCron(rep, TABLE, cron="каждую ночь")
    with pytest.raises(ValueError, match="require date_field"):
        FullLoadCron(rep, TABLE, cron="0 3 * * *", date_from=timedelta(days=1))


def test_skips_run_while_object_is_already_loading(db):
    # Фоновая выгрузка репликатора (или второе расписание) держит объект — срабатывание
    # пропускается, а не встаёт в очередь: следующее прочитает тот же период заново.
    calls = []
    rep = _replicator(db, calls)
    cron = FullLoadCron(rep, TABLE, cron="0 3 * * *")
    with rep.claim_full_load(OBJECT_1C) as claimed:
        assert claimed
        assert cron.run_once() == 0
        assert calls == []
    # Claim снят вместе с блоком — следующий прогон отрабатывает.
    cron.run_once()
    assert len(calls) == 1


def test_run_forever_runs_on_schedule_and_stops(db, monkeypatch):
    # Ждать реальную минуту незачем: проверяем, что цикл дожидается срабатывания и выходит по
    # max_runs, а не то, как спит StopSignal.
    calls = []
    cron = FullLoadCron(_replicator(db, calls), TABLE, cron="* * * * *")
    monkeypatch.setattr(StopSignal, "wait", lambda self, seconds: None)
    cron.run_forever(max_runs=2)
    assert len(calls) == 2


def test_failed_run_does_not_kill_the_schedule(db, monkeypatch):
    calls = []
    rep = _replicator(db, calls)
    original = rep.full_load

    def failing_full_load(object_name, **kwargs):
        if not calls:
            calls.append({"object_name": object_name, "failed": True})
            raise RuntimeError("1C is down")
        return original(object_name, **kwargs)

    rep.full_load = failing_full_load
    cron = FullLoadCron(rep, TABLE, cron="* * * * *")
    monkeypatch.setattr(StopSignal, "wait", lambda self, seconds: None)
    cron.run_forever(max_runs=2)
    assert len(calls) == 2 and calls[0]["failed"] and calls[1]["object_name"] == OBJECT_1C


def test_request_stop_before_first_run(db, monkeypatch):
    calls = []
    cron = FullLoadCron(_replicator(db, calls), TABLE, cron="* * * * *")
    monkeypatch.setattr(StopSignal, "wait", lambda self, seconds: cron.request_stop())
    cron.run_forever()
    assert calls == []
