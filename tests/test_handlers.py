"""
Оффлайн-тесты пользовательских обработчиков (onecdc.handlers). Без 1С и без реальных merge:
проверяется механика — нормализация объявленных обработчиков, накопление отметок, окно
(last_run_at, boundary], влияние незавершённых merge на верхнюю границу, реакция на результат merge
и на упавший обработчик.

База — локальный PostgreSQL, каждому тесту своя схема (см. conftest.py).

Часы БД в большинстве тестов подменяются счётчиком: окна и границы проверяются на точных значениях,
а привязка к реальному времени сделала бы утверждения плавающими.
"""

import time
from dataclasses import replace
from datetime import timedelta

import pytest
from dbmerge import mergeResult
from sqlalchemy import func, select

from onecdc import Handler
from onecdc.handlers import (EPOCH, SOURCE_CHANGES, SOURCE_DB_SIGNAL, SOURCE_FULL_LOAD, HandlerLoop,
                             HandlerSignals, WriteTracker, as_handler)
from onecdc.name_mapper import NameMapper
from conftest import TEST_QUEUE_GUID


class Spy(Handler):
    """Обработчик-шпион: запоминает контексты вызовов, при желании падает."""

    ON = ["Catalog_X"]

    def __init__(self, name=None, on=None, on_full_load=True, min_interval=0, fail=False):
        super().__init__(name)
        if on is not None:
            self.ON = list(on)
        self.ON_FULL_LOAD = on_full_load
        self.MIN_INTERVAL = min_interval
        self.fail = fail
        self.calls = []
        self.setup_calls = 0

    def setup(self, context):
        self.setup_calls += 1

    def handle(self, context):
        self.calls.append(context)
        if self.fail:
            raise RuntimeError('handler failed on purpose')


def _runner(db, handler):
    """HandlerLoop на одного обработчика — как в боевом коде, с настоящим реестром merge."""
    runner = HandlerLoop(engine=db.engine, schema=db.schema, handler=handler)
    return runner, runner.writes


def _replicator(db, exchange="План", **kwargs):
    from onecdc.replicator import Replicator
    kwargs.setdefault("db_schema", db.schema)
    return Replicator(odata_url="http://x", odata_auth=None, exchange_name=exchange,
                      queue_guid=TEST_QUEUE_GUID, engine=db.engine, **kwargs)


def _runner_for(db, handler):
    """HandlerLoop на боевом пути."""
    return HandlerLoop(engine=db.engine, schema=db.schema, handler=handler)


def _signal(db, object_name, source=SOURCE_CHANGES):
    """Сигнал так, как его подаёт репликатор: флагом в handlers. Прямого вызова у цикла нет —
    он и не должен ничего знать о том, кто его разбудил."""
    HandlerSignals(db.engine, db.schema).signal(object_name, source)


def _last_run_at(runner, name):
    return _state(runner, name).last_run_at


def _replicator_signal(rep, table_name, result, source=SOURCE_CHANGES):
    """
    Сигнал так, как его подаёт репликатор: выходом из блока реестра идущих merge, одной
    транзакцией со снятием строки (см. WriteTracker._finish). Отдельного «просто просигналить»
    у репликатора нет намеренно — именно связка строка+сигнал переживает обрыв.
    """
    with rep.writes.track(table_name, source) as tracked:
        tracked.result = result


def _state(runner, name):
    """Строка состояния обработчика из handlers."""
    with runner.engine.connect() as conn:
        return conn.execute(select(runner.table)
                            .where(runner.table.c.name == name)).one()


def _result(inserted=0, updated=0, deleted=0, added_fields=None):
    return mergeResult(total_row_count=0, inserted_row_count=inserted, updated_row_count=updated,
                       deleted_row_count=deleted, total_time=0.0, temp_insert_time=0.0,
                       insert_time=0.0, update_time=0.0, delete_time=0.0,
                       added_fields=added_fields or {})


def test_as_handler_accepts_instances_modules_and_functions(db):
    # Обязательного базового класса нет: нужны имя, ON и вызываемое handle.
    assert as_handler(Spy(name='spy')).name == 'spy'

    def send_to_queue(context):
        pass
    send_to_queue.ON = ["Catalog_X"]
    assert as_handler(send_to_queue).name == 'send_to_queue'

    from config.handlers import zakazy_klientov as module_handler
    assert as_handler(module_handler.ZakazyKlientov()).on == frozenset(
        ["AccumulationRegister_ZakazyKlientov", "Catalog_Nomenklatura"])


def test_as_handler_rejects_broken_declarations(db):
    class NoOn(Handler):
        def handle(self, context):
            pass

    with pytest.raises(AttributeError, match="ON"):
        as_handler(NoOn())

    # Класс вместо экземпляра — типичная опечатка, и она должна ловиться на старте.
    with pytest.raises(TypeError, match="instance"):
        as_handler(Spy)

    with pytest.raises(AttributeError, match="handle"):
        as_handler(object())

    # Имя объекта 1С вместо имени таблицы: подписка бы просто не сработала, и молча. Точное имя
    # таблицы ошибка не называет намеренно — оно закреплено в БД, а базы в этой точке нет
    # (см. name_mapper): вместо этого отправляет в реестр.
    with pytest.raises(ValueError, match="object_full_name_en"):
        as_handler(Spy(on=["AccumulationRegister_ЗаказыКлиентов"]))


def test_handlers_are_signalled_by_table_name(db, monkeypatch):
    # Обработчик пишет SQL по таблицам, поэтому и подписывается на имя таблицы. Оркестратор знает
    # объект под именем 1С и переводит его сам.


    spy = Spy(on=["Catalog_Nomenklatura"])
    rep = _replicator(db)
    runner = _runner_for(db, spy)
    runner.run_if_pending()
    spy.calls.clear()

    assert rep._handler_key("Catalog_Номенклатура") == "Catalog_Nomenklatura"
    _replicator_signal(rep, rep._handler_key("Catalog_Номенклатура"), _result(updated=1),
                         SOURCE_CHANGES)
    runner.run_if_pending()
    assert len(spy.calls) == 1
    assert spy.calls[0].objects == frozenset({"Catalog_Nomenklatura"})


def test_db_now_drops_the_time_zone(db):
    # PostgreSQL now() отдаёт timestamptz, драйвер — offset-aware datetime. А merged_on, started_at
    # и onecdc_handlers.last_run_at лежат в колонках без пояса и читаются offset-naive. Сравнить их в
    # Python нельзя, и HandlerLoop падал на boundary <= last_run_at с «can't compare offset-naive
    # and offset-aware datetimes». Приведение делает сама БД (DB_NOW_WITHOUT_TIMEZONE).
    from onecdc.db_writer import DBWriter

    with db.engine.connect() as conn:
        aware = conn.scalar(select(func.now()))
    assert aware.tzinfo is not None, 'иначе тест ничего не проверяет'

    now = DBWriter(engine=db.engine, name_mapper=NameMapper(), schema=db.schema).db_now()
    assert now.tzinfo is None
    assert now == aware.replace(tzinfo=None).replace(microsecond=now.microsecond), \
        'смещение отбрасываем, а не переводим в UTC'

    # И граница окна сравнивается с отметкой из колонки без пояса, ничего не роняя.
    tracker = WriteTracker(db.engine, db.schema, 'План1')
    try:
        assert tracker.boundary(["Catalog_X"]) > EPOCH
    finally:
        tracker.close()


def test_failure_before_handle_keeps_the_handler_in_the_queue(db):
    # Падение ДО вызова handle (например, при расчёте границы) не должно съедать грязные отметки:
    # иначе обработчик перестанет вставать в очередь до следующего изменения, а в handlers не
    # появится last_error — со стороны БД он будет выглядеть исправным.
    class BrokenMerges:
        def boundary(self, object_names):
            raise RuntimeError('БД недоступна')

    spy = Spy()
    runner, _ = _runner(db, spy)
    runner._writes = BrokenMerges()

    runner.run_if_pending()

    assert spy.calls == []
    with runner.engine.connect() as conn:
        error = conn.execute(select(runner.table.c.last_error)
                             .where(runner.table.c.name == spy.name)).scalar()
    assert error and 'БД недоступна' in error
    assert runner._dirty_objects, 'отметки должны вернуться в очередь'


def test_handlers_are_told_apart_by_name(db):
    # Имя — ключ состояния в handlers: два одноимённых поделили бы одну отметку last_run_at,
    # поэтому параметризованным экземплярам имя задают явно.
    assert as_handler(Spy()).name == 'Spy'
    assert as_handler(Spy(name='second')).name == 'second'

    first, second = _runner_for(db, Spy()), _runner_for(db, Spy(name='second'))
    first.run_if_pending()
    assert _last_run_at(first, 'Spy') is not None
    assert _last_run_at(second, 'second') is None, 'у каждого своя отметка'


def test_setup_runs_once_before_the_first_handle(db):
    # Разовая подготовка отдельно от handle: полная выгрузка сигналит постранично, и DDL на каждый
    # вызов брал бы блокировки на пустом месте.
    spy = Spy()
    runner, _ = _runner(db, spy)

    runner.run_if_pending()
    assert spy.setup_calls == 1
    assert len(spy.calls) == 1

    _signal(db, "Catalog_X", SOURCE_CHANGES)
    runner.run_if_pending()
    assert spy.setup_calls == 1, 'setup не повторяется'
    assert len(spy.calls) == 2


def test_first_run_starts_from_scratch_and_advances_window(db):
    # Новый обработчик считается грязным сразу: его состояние (грязные отметки) живёт в памяти и
    # перезапуск процесса не переживает, поэтому после старта он обязан отработать хотя бы раз.
    spy = Spy()
    runner, _ = _runner(db, spy)

    runner.run_if_pending()
    assert len(spy.calls) == 1
    assert spy.calls[0].last_run_at == EPOCH, 'первый прогон идёт с начала времён'
    first_boundary = spy.calls[0].boundary
    assert _last_run_at(runner, spy.name) == first_boundary

    # Без сигнала повторно не вызываем: обработчик всё равно выбирает данные сам, и пустое окно ему
    # ничего не даст.
    runner.run_if_pending()
    assert len(spy.calls) == 1

    _signal(db, "Catalog_X", SOURCE_CHANGES)
    runner.run_if_pending()
    assert len(spy.calls) == 2
    # Следующее окно начинается ровно там, где закончилось предыдущее — без дыр и без нахлёста.
    assert spy.calls[1].last_run_at == first_boundary
    assert spy.calls[1].boundary > first_boundary
    assert spy.calls[1].objects == frozenset({"Catalog_X"})


def test_changed_since_covers_all_sources(db):
    # Строка витрины собирается JOIN-ом нескольких источников, и свежим может стать любой из них.
    from sqlalchemy import Column, DateTime, MetaData, Table

    spy = Spy()
    runner, _ = _runner(db, spy)
    runner.run_if_pending()
    context = spy.calls[0]

    t = Table('m', MetaData(), Column('a', DateTime), Column('b', DateTime))
    sql = str(spy.changed_since(context, t.c.a, t.c.b))
    assert sql.count('>') == 2, sql
    assert ' OR ' in sql
    # Верхней границы нет намеренно: от пропуска строк защищает значение context.boundary,
    # а не WHERE.
    assert '<=' not in sql, sql


def test_signals_are_coalesced(db):
    # Сигнал — это «объект стал грязным», а не «запусти»: тысяча страниц полной выгрузки даёт
    # один прогон, а не тысячу.
    spy = Spy(on=["Catalog_X", "Catalog_Y"])
    runner, _ = _runner(db, spy)
    runner.run_if_pending()          # стартовый прогон
    spy.calls.clear()

    for _ in range(50):
        _signal(db, "Catalog_X", SOURCE_FULL_LOAD)
    _signal(db, "Catalog_Y", SOURCE_CHANGES)

    runner.run_if_pending()
    assert len(spy.calls) == 1
    # Флаг в таблице не несёт ни имени изменившейся таблицы, ни источника: обработчик знает только
    # «что-то из моего ON изменилось». Выбирать данные он всё равно обязан по окну.
    assert spy.calls[0].objects == frozenset({"Catalog_X", "Catalog_Y"})
    assert spy.calls[0].sources == frozenset({SOURCE_DB_SIGNAL})


def test_on_full_load_is_published_and_respected_by_the_replicator(db):
    # Обработчик по метке update_requested_at источник не различает, поэтому решение «вызывать ли на
    # бэкфилле» принимает тот, кто флаг поднимает. Значит on_full_load обязан доехать до него через
    # ту же таблицу — иначе ON_FULL_LOAD=False просто не работал бы.
    quiet = Spy(name='quiet', on=["Catalog_X"], on_full_load=False)
    loud = Spy(name='loud', on=["Catalog_X"])
    _runner_for(db, quiet)
    _runner_for(db, loud)

    assert _state(_runner_for(db, quiet), 'quiet').on_full_load is False
    assert _state(_runner_for(db, loud), 'loud').on_full_load is True

    signals = HandlerSignals(db.engine, db.schema)
    assert signals.subscribers("Catalog_X", SOURCE_FULL_LOAD) == ['loud']
    assert sorted(signals.subscribers("Catalog_X", SOURCE_CHANGES)) == ['loud', 'quiet']


def test_on_full_load_false_ignores_backfill(db):
    spy = Spy(on_full_load=False)
    runner, _ = _runner(db, spy)
    runner.run_if_pending()
    spy.calls.clear()

    _signal(db, "Catalog_X", SOURCE_FULL_LOAD)
    runner.run_if_pending()
    assert spy.calls == [], 'страницы полной выгрузки такому обработчику не адресованы'

    _signal(db, "Catalog_X", SOURCE_CHANGES)
    runner.run_if_pending()
    assert len(spy.calls) == 1


def test_boundary_is_pinned_by_unfinished_merge(db):
    # Главная гарантия: merged_on пишется внутри merge-транзакции, а коммитится позже, поэтому
    # строка может быть в прошлом и при этом невидимой. Граница окна прижимается к старту такого
    # merge, и его строки достаются следующему окну, а не теряются.
    spy = Spy()
    runner, tracker = _runner(db, spy)
    runner.run_if_pending()
    spy.calls.clear()

    _signal(db, "Catalog_X", SOURCE_CHANGES)
    with tracker.track("Catalog_X") as merge:
        runner.run_if_pending()
        assert spy.calls[0].boundary == merge.started_at

    # Merge завершился — граница снова доходит до «сейчас».
    _signal(db, "Catalog_X", SOURCE_CHANGES)
    runner.run_if_pending()
    assert spy.calls[1].boundary > merge.started_at


def test_unrelated_merge_does_not_hold_the_window(db):
    # Граница считается по объектам из ON: долгая выгрузка чужой таблицы не должна тормозить
    # обработчик, который её не читает.
    spy = Spy()
    runner, tracker = _runner(db, spy)
    runner.run_if_pending()
    spy.calls.clear()

    _signal(db, "Catalog_X", SOURCE_CHANGES)
    with tracker.track("Catalog_OTHER") as other:
        runner.run_if_pending()
        assert spy.calls[0].boundary > other.started_at


def test_handler_waits_when_window_would_be_inverted(db):
    # Merge, начавшийся раньше прошлого прогона, делает окно пустым: брать нечего, но и отметку
    # терять нельзя — обработчик остаётся грязным и дождётся своего.
    spy = Spy()
    runner, tracker = _runner(db, spy)

    with tracker.track("Catalog_X"):
        runner.run_if_pending()                       # стартовый прогон: last_run_at = старт merge
        assert len(spy.calls) == 1
        _signal(db, "Catalog_X", SOURCE_CHANGES)
        runner.run_if_pending()
        assert len(spy.calls) == 1, 'окно не сдвинулось — вызывать не с чем'

    runner.run_if_pending()
    assert len(spy.calls) == 2, 'merge завершился — накопленная отметка отработала'


def test_failed_handler_keeps_window_and_records_error(db):
    spy = Spy()
    runner, _ = _runner(db, spy)
    runner.run_if_pending()          # первый прогон — пересборка, она проходит
    before = _last_run_at(runner, spy.name)
    spy.calls.clear()

    spy.fail = True
    _signal(db, "Catalog_X", SOURCE_CHANGES)
    runner._next_allowed_at = 0.0
    runner.run_if_pending()

    assert len(spy.calls) == 1
    assert _last_run_at(runner, spy.name) == before, 'упавший обработчик окно не двигает'
    with runner.engine.connect() as conn:
        error = conn.execute(select(runner.table.c.last_error)
                             .where(runner.table.c.name == spy.name)).scalar()
    assert 'handler failed on purpose' in error
    # Отметка возвращена — повтор произойдёт сам, без нового изменения (пауза RETRY_DELAY снята).
    runner._next_allowed_at = 0.0
    runner.run_if_pending()
    assert len(spy.calls) == 2


def test_a_disabled_handler_catches_up_when_enabled(db):
    # Выключенный не вызывается — но сигнал ему СТАВИТСЯ и копится. Раньше он не ставился вовсе
    # (подписки читались с WHERE enabled), а окно за простой всё равно копилось: last_run_at не
    # двигается. Получалось худшее из двух — и окно накопилось, и браться за него обработчик
    # решал по случайному поводу, при первом изменении после включения.
    spy = Spy()
    runner, _ = _runner(db, spy)
    runner.run_if_pending()
    spy.calls.clear()
    before = _last_run_at(runner, spy.name)

    with runner.engine.begin() as conn:
        conn.execute(runner.table.update().where(runner.table.c.name == spy.name)
                     .values(enabled=False))
    _signal(db, "Catalog_X", SOURCE_CHANGES)
    runner.run_if_pending()
    assert spy.calls == [], 'выключенный обработчик не вызывается'
    assert _state(runner, spy.name).update_requested_at is not None, \
        'сигнал выключенному должен копиться, а не испаряться'

    with runner.engine.begin() as conn:
        conn.execute(runner.table.update().where(runner.table.c.name == spy.name)
                     .values(enabled=True))
    runner.run_if_pending()

    assert len(spy.calls) == 1, 'включение должно догнать простой само, а не ждать изменения'
    assert spy.calls[0].last_run_at == before, 'окно за простой никуда не делось'


def test_full_rebuild_request_opens_the_window_and_is_cleared_after_success(db):
    spy = Spy()
    runner, _ = _runner(db, spy)
    runner.run_if_pending()
    assert spy.calls[0].full_rebuild is True, 'первый прогон — это тоже сборка с нуля'
    spy.calls.clear()
    first = _state(runner, spy.name)
    assert first.last_run_at is not None
    assert first.last_full_rebuild_dt is not None, 'метрики пишутся и без явного заказа'
    assert first.last_full_rebuild_minutes is not None

    HandlerSignals(db.engine, db.schema).request_full_rebuild("Catalog_X", "new column")
    assert _state(runner, spy.name).full_rebuild_is_required

    _signal(db, "Catalog_X", SOURCE_CHANGES)
    runner.run_if_pending()
    assert spy.calls[0].last_run_at == EPOCH, 'пересборка = окно с начала времён'
    assert spy.calls[0].full_rebuild is True

    state = _state(runner, spy.name)
    assert not state.full_rebuild_is_required, 'после успеха требование снимается'
    assert state.last_full_rebuild_dt >= first.last_full_rebuild_dt
    assert state.last_full_rebuild_minutes is not None, 'сколько заняла пересборка, минуты'
    assert state.last_run_at is not None


def test_full_rebuild_request_alone_starts_the_handler(db):
    # Флаг ставят руками в таблице, и никакого сигнала об изменении за ним не приходит. Если бы
    # заказ не будил обработчик сам, он лежал бы без дела до ближайшего изменения подписанных
    # объектов — а его может не быть неделями.
    spy = Spy()
    runner, _ = _runner(db, spy)
    runner.run_if_pending()
    spy.calls.clear()

    runner.run_if_pending()
    assert spy.calls == [], 'без изменений и без заказа обработчик не вызывают'

    with runner.engine.begin() as conn:
        conn.execute(runner.table.update()
                     .where(runner.table.c.name == spy.name)
                     .values(full_rebuild_is_required=True))

    runner.run_if_pending()
    assert len(spy.calls) == 1, 'один только заказ пересборки должен запустить обработчик'
    assert spy.calls[0].full_rebuild is True
    assert not _state(runner, spy.name).full_rebuild_is_required


def test_full_rebuild_requested_during_a_run_is_not_lost(db):
    # Заказ пересборки может прийти в середине прогона (новая колонка приехала пакетом, пока
    # обработчик считал). Записать поверх него свой результат — значит молча его отменить.
    spy = Spy()
    runner, _ = _runner(db, spy)
    runner.run_if_pending()
    spy.calls.clear()
    before = _last_run_at(runner, spy.name)
    original = runner.handler

    def handle_and_request_rebuild(context):
        original.handle(context)
        HandlerSignals(db.engine, db.schema).request_full_rebuild(
            "Catalog_X", "new column arrived mid-run")

    runner.handler = replace(original, handle=handle_and_request_rebuild)
    _signal(db, "Catalog_X", SOURCE_CHANGES)
    runner.run_if_pending()

    state = _state(runner, spy.name)
    assert state.full_rebuild_is_required, 'заказ пересборки пережил успешный прогон'
    assert state.last_run_at == before, 'граница не сдвинулась — прогон будет повторён'

    runner.handler = original
    spy.calls.clear()
    runner.run_if_pending()
    assert spy.calls[0].last_run_at == EPOCH, 'и следующий прогон идёт с начала времён'
    assert spy.calls[0].full_rebuild is True


def test_replicator_signals_only_on_real_changes(db, monkeypatch):
    # Гейт «вызывать только если merge реально что-то сделал» живёт в оркестраторе: 1С регистрирует
    # изменение объекта на любую перезапись, и пустых прогонов в пакете больше, чем содержательных.


    spy = Spy()
    rep = _replicator(db)
    runner = _runner_for(db, spy)
    runner.run_if_pending()
    spy.calls.clear()

    _replicator_signal(rep, "Catalog_X", _result(), SOURCE_CHANGES)
    runner.run_if_pending()
    assert spy.calls == [], 'нулевой прогон merge обработчику ничего не даёт'

    _replicator_signal(rep, "Catalog_X", _result(updated=1), SOURCE_CHANGES)
    runner.run_if_pending()
    assert len(spy.calls) == 1

    # Новая колонка пересборку витрины больше НЕ заказывает: витрину строит код обработчика, и
    # про колонку, которой вчера не было, он ничего не знает. Обычный сигнал при этом проходит —
    # строки-то изменились.
    spy.calls.clear()
    _replicator_signal(rep, "Catalog_X", _result(inserted=1, added_fields={'Новый': 'String'}),
                       SOURCE_CHANGES)
    runner.run_if_pending()
    assert len(spy.calls) == 1
    assert spy.calls[0].last_run_at != EPOCH, 'полная пересборка заказана без просьбы человека'
    assert not _state(runner, spy.name).full_rebuild_is_required


# --- несколько планов обмена: общий HandlerLoop --------------------------------------------

def test_signals_from_several_replicators_reach_one_handler(db):
    # Несколько планов обмена — несколько Replicator, а обработчик один. Связывает их только БД:
    # репликаторы ставят update_requested_at, обработчик её видит. Объекты обработчика им не
    # нужны — потому он и может жить в другом процессе.
    spy = Spy(on=["Catalog_Nomenklatura", "Document_ZakazKlienta"])
    runner = _runner_for(db, spy)
    rep1 = _replicator(db, "План1")
    rep2 = _replicator(db, "План2")

    runner.run_if_pending()          # стартовый прогон
    spy.calls.clear()

    _replicator_signal(rep1, "Catalog_Nomenklatura", _result(updated=1), SOURCE_CHANGES)
    _replicator_signal(rep2, "Document_ZakazKlienta", _result(inserted=1), SOURCE_CHANGES)
    assert _state(runner, spy.name).update_requested_at is not None

    runner.run_if_pending()
    assert len(spy.calls) == 1
    assert _state(runner, spy.name).update_requested_at is None, 'метка снята успешным прогоном'


def test_boundary_covers_writes_of_another_process(db):
    # Главная гарантия всей затеи: незакоммиченный merge ЧУЖОГО репликатора прижимает границу окна
    # обработчику. Иначе строки этого merge, у которых merged_on уже в прошлом, оказались бы левее
    # записанной отметки и потерялись бы молча. Реестр общий, потому что лежит в БД, — обработчик
    # видит его хоть из другого контейнера.
    spy = Spy(on=["Catalog_Nomenklatura", "Document_ZakazKlienta"])
    # Цикл БЕЗ подсунутого реестра: берёт общий из БД, как в бою.
    runner = HandlerLoop(engine=db.engine, schema=db.schema, handler=spy)
    rep1 = _replicator(db, "План1")
    rep2 = _replicator(db, "План2")

    runner.run_if_pending()
    spy.calls.clear()

    _replicator_signal(rep2, "Document_ZakazKlienta", _result(updated=1), SOURCE_CHANGES)
    with rep1.writes.track("Catalog_Nomenklatura") as merge:
        runner.run_if_pending()
    assert spy.calls[0].boundary == merge.started_at

    # Merge завершился — граница снова доходит до «сейчас».
    _replicator_signal(rep2, "Document_ZakazKlienta", _result(updated=1), SOURCE_CHANGES)
    runner.run_if_pending()
    assert spy.calls[1].boundary > merge.started_at


def test_run_forever_stops_on_request(db):
    # У обработчика та же форма, что у репликатора: блокирующий run_forever, который завершается
    # по request_stop() или по общему сигналу процесса.
    import threading

    spy = Spy()
    runner = _runner_for(db, spy)
    loop = threading.Thread(target=runner.run_forever, kwargs={'poll_interval': 0.05})
    loop.start()
    try:
        deadline = time.monotonic() + 5
        while not spy.calls and time.monotonic() < deadline:
            time.sleep(0.01)
        assert spy.calls, 'стартовый прогон должен состояться сам'
    finally:
        runner.request_stop()
        loop.join(timeout=5)
    assert not loop.is_alive(), 'цикл обязан выйти по request_stop'


def test_stop_signal_is_process_wide(db):
    # SIGTERM означает «останавливаемся целиком». Репликаторы при нескольких планах крутятся в
    # отдельных потоках, где signal.signal бросает ValueError, — но флаг им всё равно нужен.
    import threading

    from onecdc.stop_signal import StopSignal, handle_stop_signal

    made = {}
    thread = threading.Thread(target=lambda: made.update(stop=StopSignal()))
    thread.start()
    thread.join()

    from_worker = made['stop']
    from_main = StopSignal()
    assert from_worker.requested is False, 'конструктор не должен падать вне главного потока'

    handle_stop_signal(15, None)
    assert from_worker.requested and from_main.requested, 'флаг взводится всем живым циклам'


def test_request_stop_ends_run_forever(db, monkeypatch):
    # Точечная остановка: цикл в рабочем потоке своего перехвата сигналов не имеет.
    from onecdc.replicator import Replicator

    rep = _replicator(db)
    monkeypatch.setattr(Replicator, "run_once",
                        lambda self, notify_changes=True, handlers=None: self.request_stop())

    rep.run_forever(interval=0)           # выйдет сам: run_once просит остановиться


def test_handler_publishes_its_subscriptions(db):
    # Обработчик объявляет себя сам: репликатор узнаёт о подписке только из update_on, поэтому
    # обработчик может жить в другом процессе и его код репликатору не нужен.
    spy = Spy(on=["Catalog_Nomenklatura", "Document_ZakazKlienta"])
    runner = _runner_for(db, spy)

    assert sorted(_state(runner, spy.name).update_on) == ["Catalog_Nomenklatura",
                                                          "Document_ZakazKlienta"]

    # Подписка живёт в коде, поэтому таблица обязана догонять её при каждом старте.
    spy.ON = ["Catalog_Nomenklatura"]
    _runner_for(db, spy)
    assert _state(runner, spy.name).update_on == ["Catalog_Nomenklatura"]


def test_replicator_signals_without_knowing_the_handler(db):
    # Ключевое: репликатору не передавали ни циклов, ни обработчиков — только общая БД.
    spy = Spy(on=["Catalog_Nomenklatura"])
    runner = _runner_for(db, spy)
    runner.run_if_pending()
    spy.calls.clear()

    rep = _replicator(db)

    _replicator_signal(rep, "Catalog_Nomenklatura", _result(updated=1), SOURCE_CHANGES)
    runner.run_if_pending()

    assert len(spy.calls) == 1
    assert SOURCE_DB_SIGNAL in spy.calls[0].sources


def test_update_flag_survives_a_failed_run(db):
    # Флаг снимается только успешным прогоном: иначе изменение, о котором сообщил репликатор,
    # потерялось бы вместе с упавшим прогоном.
    spy = Spy(on=["Catalog_Nomenklatura"], fail=True)
    runner = _runner_for(db, spy)
    rep = _replicator(db)

    _replicator_signal(rep, "Catalog_Nomenklatura", _result(updated=1), SOURCE_CHANGES)
    runner.run_if_pending()

    assert _state(runner, spy.name).update_requested_at is not None, 'упавший прогон метку не снимает'


def test_dead_replicator_does_not_freeze_the_boundary(db):
    # Строки брошенного процесса не должны морозить границу навсегда: раз процесса нет, его
    # транзакции откатились. Отсекаем по отметке живости.
    from onecdc.handlers import MERGE_HEARTBEAT_TTL, WriteTracker

    tracker = WriteTracker(db.engine, db.schema, 'dead-replicator')
    tracked = tracker.track("Catalog_Nomenklatura")
    assert tracker.boundary(["Catalog_Nomenklatura"]) == tracked.started_at

    # Отматываем отметку живости за TTL — как будто процесс умер и перестал её обновлять.
    with db.engine.begin() as conn:
        conn.execute(tracker.table.update().values(
            heartbeat_at=tracked.started_at - timedelta(seconds=MERGE_HEARTBEAT_TTL + 60)))

    assert tracker.boundary(["Catalog_Nomenklatura"]) > tracked.started_at


def test_abandoned_row_is_signalled_and_removed(db):
    # Брошенная строка — единственный след того, что запись шла, а сигнал не дошёл. Раньше такие
    # строки просто удалялись по таймауту, молча унося этот след. Теперь их разбирают: сигналят
    # таблице и удаляют.
    from onecdc.handlers import MERGE_HEARTBEAT_TTL, WriteTracker

    delivered = []
    tracker = WriteTracker(
        db.engine, db.schema, 'План1',
        deliver_signal=lambda conn, obj, source, result, forced: delivered.append(
            (obj, source, forced)))
    tracker.close()
    tracker.track("Catalog_Nomenklatura", 'changes')     # merge начался и оборвался
    with db.engine.begin() as conn:
        conn.execute(tracker.table.update().values(
            heartbeat_at=func.now() - timedelta(seconds=MERGE_HEARTBEAT_TTL + 60)))

    assert tracker.deliver_abandoned() == 1

    assert delivered == [("Catalog_Nomenklatura", 'changes', True)], \
        'сигнал должен уйти безусловно: что сделал прерванный merge, неизвестно'
    with db.engine.connect() as conn:
        assert conn.execute(select(tracker.table)).all() == []
    assert tracker.deliver_abandoned() == 0, 'разобранная строка не должна сигналить повторно'


def test_second_process_of_the_same_exchange_does_not_delete_live_writes(db):
    # Штатная раскладка из README: репликатор и расписание — разные процессы ОДНОГО плана обмена.
    # Владельцем строки было имя обмена, и старт второго процесса удалял живые строки первого:
    # граница переставала их ждать, обработчик брал её как «сейчас» и молча перешагивал строки,
    # которые первый вот-вот закоммитит. Сырая таблица при этом выглядит здоровой, витрина — нет.
    from onecdc.handlers import WriteTracker

    def all_rows():
        with db.engine.connect() as conn:
            return conn.execute(select(tracker.table)).all()

    tracker = WriteTracker(db.engine, db.schema, 'План1')
    second = None
    try:
        tracked = tracker.track("Catalog_Nomenklatura")

        second = WriteTracker(db.engine, db.schema, 'План1')

        assert len(all_rows()) == 1, 'второй процесс удалил живой merge первого'
        # И главное — граница у второго прижата к чужому незавершённому merge, а не к «сейчас».
        assert second.boundary(["Catalog_Nomenklatura"]) == tracked.started_at
    finally:
        tracker.close()
        if second is not None:
            second.close()


def test_two_processes_of_the_same_exchange_get_different_owners(db):
    # Владелец уникален на экземпляр — на этом держится и уборка, и отметка живости, и первичный
    # ключ строки (раньше id был f'{owner}:{счётчик}', а счётчик в обоих процессах шёл с нуля).
    from onecdc.handlers import WriteTracker

    first = WriteTracker(db.engine, db.schema, 'План1')
    second = WriteTracker(db.engine, db.schema, 'План1')
    try:
        assert first.owner != second.owner
        assert first.owner.startswith('План1:'), 'по строке в БД должно быть видно, чей это обмен'

        first.track("Catalog_Nomenklatura")
        second.track("Catalog_Nomenklatura")
        with db.engine.connect() as conn:
            rows = conn.execute(select(first.table)).all()
        assert len({r.id for r in rows}) == 2, 'строки столкнулись на первичном ключе'
    finally:
        first.close()
        second.close()


def test_heartbeat_does_not_revive_writes_of_a_dead_twin(db):
    # Отметку живости процесс продлевает только СВОИМ строкам. Пока владельцем было имя обмена,
    # живой процесс вечно продлевал брошенную строку умершего тёзки — и граница окна оставалась
    # прижатой к ней навсегда: витрина переставала догонять источник совсем.
    from onecdc.handlers import MERGE_HEARTBEAT_TTL, WriteTracker

    # Порядок важен: оба процесса поднялись, и только потом один умер. Если поднимать выжившего
    # последним, дефект спрячется за уборкой при старте — она удалит строку тёзки, и тест пройдёт
    # по неверной причине.
    alive = WriteTracker(db.engine, db.schema, 'План1')
    dead = WriteTracker(db.engine, db.schema, 'План1')
    try:
        dead_write = dead.track("Catalog_Nomenklatura")
        dead.close()
        # Процесс умер: отметку живости его строке больше никто не обновляет.
        with db.engine.begin() as conn:
            conn.execute(dead.table.update().values(
                heartbeat_at=func.now() - timedelta(seconds=MERGE_HEARTBEAT_TTL + 60)))

        alive.heartbeat()

        # Строка тёзки осталась протухшей, граница её не ждёт. Иначе витрина не догонит источник
        # уже никогда: граница прижата к чужому merge, которого нет, а снять её некому.
        assert alive.boundary(["Catalog_Nomenklatura"]) > dead_write.started_at
    finally:
        alive.close()
        dead.close()


def test_merge_heartbeat_works_without_a_replication_loop(db, monkeypatch):
    # Отметку живости раньше вёл только run_forever репликатора. Но строки реестра появляются и в
    # одиночном run_once, и в вызванном руками full_load — там цикла нет вовсе, и merge, идущий
    # дольше MERGE_HEARTBEAT_TTL, признавался бы брошенным: обработчик перешагнул бы его строки.
    from onecdc import write_tracker as tracker_module
    from onecdc.handlers import MERGE_HEARTBEAT_TTL

    monkeypatch.setattr(tracker_module, 'MERGE_HEARTBEAT_PERIOD', 0.05)
    tracker = WriteTracker(db.engine, db.schema, 'План1')
    try:
        with tracker.track("Catalog_Nomenklatura") as tracked:
            # Отматываем отметку далеко в прошлое: если её никто не обновляет, merge считается
            # брошенным и границу больше не держит.
            with db.engine.begin() as conn:
                conn.execute(tracker.table.update().values(
                    heartbeat_at=tracked.started_at - timedelta(seconds=MERGE_HEARTBEAT_TTL + 60)))

            deadline = time.monotonic() + 10
            beat = None
            while time.monotonic() < deadline:
                with db.engine.connect() as conn:
                    beat = conn.scalar(select(tracker.table.c.heartbeat_at))
                if beat is not None and beat >= tracked.started_at:
                    break
                time.sleep(0.05)

            assert beat is not None and beat >= tracked.started_at, \
                'отметку живости никто не обновил — merge будет признан брошенным'
            # И граница по-прежнему прижата к этому merge, а не ушла в «сейчас».
            assert tracker.boundary(["Catalog_Nomenklatura"]) == tracked.started_at
    finally:
        tracker.close()


def test_heartbeat_thread_is_started_once(db, monkeypatch):
    # Проверка «поток есть» и его создание обязаны быть неделимыми: два merge, стартовавших
    # одновременно, иначе заводят по потоку каждый.
    import threading

    from onecdc import write_tracker as tracker_module

    monkeypatch.setattr(tracker_module, 'MERGE_HEARTBEAT_PERIOD', 0.05)
    tracker = WriteTracker(db.engine, db.schema, 'План1')
    started = threading.Barrier(4)

    def track_one():
        started.wait()
        with tracker.track("Catalog_Nomenklatura"):
            time.sleep(0.2)

    before = set(threading.enumerate())
    threads = [threading.Thread(target=track_one) for _ in range(3)]
    for thread in threads:
        thread.start()
    started.wait()
    time.sleep(0.1)
    # Имя потока — heartbeat:<owner>, а owner уникален на экземпляр (см. instance_owner),
    # поэтому сверяем по префиксу, а не по имени обмена целиком.
    new_heartbeats = [t for t in set(threading.enumerate()) - before
                      if t.name == f'heartbeat:{tracker.owner}']
    for thread in threads:
        thread.join()

    try:
        assert len(new_heartbeats) == 1, 'на каждый merge завели по потоку'
    finally:
        tracker.close()


def test_the_heartbeat_thread_goes_away_when_nothing_is_in_flight(db, monkeypatch):
    # Реестры живут долго (столько же, сколько процесс), и оставлять при каждом по спящему потоку
    # незачем: поток нужен ровно пока есть что продлевать.
    import threading

    from onecdc import write_tracker as tracker_module

    monkeypatch.setattr(tracker_module, 'MERGE_HEARTBEAT_PERIOD', 0.01)
    tracker = WriteTracker(db.engine, db.schema, 'План1')
    try:
        with tracker.track("Catalog_Nomenklatura"):
            assert tracker._heartbeat_thread is not None

        deadline = time.monotonic() + 5
        while tracker._heartbeat_thread is not None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert tracker._heartbeat_thread is None, 'поток остался висеть после последнего merge'

        # И поднимается заново со следующим merge — гашение не одноразовое.
        with tracker.track("Catalog_Nomenklatura"):
            assert tracker._heartbeat_thread is not None
            assert any(t.name == f'heartbeat:{tracker.owner}' for t in threading.enumerate())
    finally:
        tracker.close()


def test_constructors_install_signal_handlers(db, monkeypatch):
    # Типовая точка входа отправляет все run_forever в пул потоков, а из рабочего потока перехват
    # поставить нельзя. Тогда его не ставит никто: SIGTERM убивает процесс, SIGINT вешает его
    # намертво. Поэтому перехват ставится в конструкторе — он-то вызывается из главного потока.
    import signal as signal_module

    from onecdc import stop_signal

    monkeypatch.setattr(stop_signal, '_handlers_installed', False)
    previous = {sig: signal_module.getsignal(sig)
                for sig in (signal_module.SIGTERM, signal_module.SIGINT)}
    try:
        _runner_for(db, Spy())
        for sig in previous:
            assert signal_module.getsignal(sig) is stop_signal.handle_stop_signal, \
                f'{sig!r} не перехвачен конструктором'
    finally:
        for sig, previous_handler in previous.items():
            signal_module.signal(sig, previous_handler)


class BlockSpy(Handler):
    """Обработчик с пересборкой по блокам: три года, по блоку на год."""

    ON = ["Catalog_X"]
    BLOCKS = ['2024', '2025', '2026']

    def __init__(self, name=None, fail_at=None):
        super().__init__(name)
        self.fail_at = fail_at
        self.calls = []          # контексты обычных прогонов
        self.blocks = []         # метки завершённых блоков
        self.rebuild_from = []   # с чем пришла каждая пересборка

    def handle(self, context):
        self.calls.append(context)

    def rebuild(self, context):
        self.rebuild_from.append(context.rebuild_from)
        for label in self.BLOCKS:
            if context.rebuild_from and label <= context.rebuild_from:
                continue
            if label == self.fail_at:
                raise RuntimeError(f'block {label} failed on purpose')
            self.blocks.append(label)
            yield label


def test_rebuild_goes_block_by_block(db):
    # Смысл всей затеи: пересборка нарезана обработчиком, и библиотека идёт по его блокам.
    spy = BlockSpy()
    runner = _runner_for(db, spy)
    runner.run_if_pending()

    assert spy.blocks == ['2024', '2025', '2026']
    assert spy.rebuild_from == [None], 'первая пересборка начинается с начала'
    state = _state(runner, spy.name)
    assert state.rebuild_cursor is None, 'курсор снимается по завершении'
    assert state.last_full_rebuild_dt is not None
    assert state.last_full_rebuild_minutes is not None


def test_changes_are_applied_between_rebuild_blocks(db):
    # Ради этого пересборка и разбита на блоки: витрина не стоит холодной всю пересборку.
    # Изменение, пришедшее в середине, применяется, не дожидаясь последнего блока.
    spy = BlockSpy()
    runner = _runner_for(db, spy)

    signalled = []
    original_rebuild = runner.handler.rebuild

    def rebuild_with_a_change_midway(context):
        for label in original_rebuild(context):
            if label == '2024':
                # Пока идёт пересборка, репликатор сохранил изменение подписанной таблицы.
                _signal(db, "Catalog_X", SOURCE_CHANGES)
            signalled.append((label, len(spy.calls)))
            yield label

    runner.handler = replace(runner.handler, rebuild=rebuild_with_a_change_midway)
    runner.run_if_pending()

    assert spy.blocks == ['2024', '2025', '2026']
    # signalled снимается ДО того, как драйвер применит изменения за этот блок, поэтому
    # ненулевой счётчик на строке '2025' означает: инкремент прошёл именно МЕЖДУ блоками,
    # а не после всей пересборки. Ради этого блоки и заведены.
    by_label = dict(signalled)
    assert by_label['2024'] == 0, 'до сигнала обработчик по окну не вызывали'
    assert by_label['2025'] >= 1, 'изменение ждало конца пересборки, а не применилось между блоками'
    assert len(spy.calls) >= 1
    assert spy.calls[0].last_run_at > EPOCH, 'это инкремент, а не ещё одна пересборка'
    assert spy.calls[0].full_rebuild is False
    assert _state(runner, spy.name).update_requested_at is None, 'метка снята успешным прогоном'


def test_rebuild_resumes_from_the_cursor_after_a_restart(db):
    # Генератор блоков живёт в памяти и перезапуска не переживает, поэтому место остановки
    # хранится в БД: пересборка на десятки минут не должна начинаться заново из-за рестарта.
    spy = BlockSpy(fail_at='2026')
    runner = _runner_for(db, spy)
    runner.run_if_pending()

    assert spy.blocks == ['2024', '2025'], 'дошли до падающего блока'
    state = _state(runner, spy.name)
    assert state.rebuild_cursor == '2025', 'место остановки записано'
    assert 'block 2026 failed' in state.last_error

    # Новый процесс: свой HandlerLoop, состояние только из БД. Предыдущий ушёл штатно и отпустил
    # аренду имени — иначе сменщик ждал бы истечения её TTL, как ждал бы после падения.
    runner.close()
    resumed = BlockSpy()
    restarted = _runner_for(db, resumed)
    restarted.run_if_pending()

    assert resumed.rebuild_from == ['2025'], 'обработчику вернули место остановки'
    assert resumed.blocks == ['2026'], 'сделанные блоки заново не считаются'
    assert _state(restarted, resumed.name).rebuild_cursor is None


def test_rebuild_requested_during_a_rebuild_is_not_swallowed(db):
    # Заказ мог прийти из-за новой колонки уже после того, как часть блоков посчиталась старой
    # логикой. Снять его завершением текущей пересборки — значит оставить витрину неполной.
    spy = BlockSpy()
    runner = _runner_for(db, spy)
    original_rebuild = runner.handler.rebuild

    def rebuild_with_a_request_midway(context):
        for label in original_rebuild(context):
            if label == '2024':
                HandlerSignals(db.engine, db.schema).request_full_rebuild(
                    "Catalog_X", "new column arrived mid-rebuild")
            yield label

    runner.handler = replace(runner.handler, rebuild=rebuild_with_a_request_midway)
    runner.run_if_pending()

    state = _state(runner, spy.name)
    assert state.full_rebuild_is_required, 'заказ пережил завершение текущей пересборки'
    assert state.rebuild_cursor is None, 'но текущая — закончена'


def test_handler_without_blocks_rebuilds_in_one_go(db):
    # Нарезать пересборку не обязательно: не объявил rebuild — прежнее поведение, один проход.
    spy = Spy()
    runner = _runner_for(db, spy)
    runner.run_if_pending()

    assert len(spy.calls) == 1
    assert spy.calls[0].full_rebuild is True
    assert spy.calls[0].last_run_at == EPOCH
    assert _state(runner, spy.name).rebuild_cursor is None


def test_signal_arriving_during_a_run_is_not_swallowed(db):
    """
    Сигнал, пришедший ПОСЕРЕДИНЕ прогона, обязан завести обработчика ещё раз.

    С булевым флагом это не работало: поднять true над true нельзя, следа не остаётся, и успешный
    прогон снимал флаг вместе с непрочитанным сигналом — изменение терялось до следующего.
    Метка времени сдвигается вправо от границы окна, и снять её прогон уже не вправе.
    """
    class SignallingSpy(Spy):
        """Обработчик, во время работы которого репликатор сообщает о новом изменении."""

        def handle(self, context):
            super().handle(context)
            if len(self.calls) == 1:          # только на первом прогоне, иначе цикл был бы вечным
                _signal(db, "Catalog_X")

    spy = SignallingSpy()
    runner = _runner_for(db, spy)
    runner.run_if_pending()                   # первый прогон: стартовый + сигнал внутри него
    spy.calls.clear()

    _signal(db, "Catalog_X")
    runner.run_if_pending()
    assert len(spy.calls) == 1, 'обработчик должен быть вызван по сигналу'
    assert _state(runner, spy.name).update_requested_at is not None, \
        'сигнал, пришедший во время прогона, снят вместе с обработанными'

    # И он действительно заводит следующий прогон, а не просто висит меткой.
    runner.run_if_pending()
    assert len(spy.calls) == 2
    assert _state(runner, spy.name).update_requested_at is None, \
        'после прогона, который его прочитал, метка должна сняться'


def test_full_rebuild_from_a_full_load_page_skips_opted_out_handlers(db):
    """
    Заказ пересборки уважает on_full_load так же, как обычный сигнал.

    Пересборку теперь заказывает только человек (автоматического повода больше нет), но заказать
    её может и тот, кто разгребает бэкфилл. Для отправщика во внешнюю систему пересборка — это
    повторная отправка всего с начала времён, ровно то, чего ON_FULL_LOAD=False и избегает.
    """
    quiet = Spy(name='quiet', on=["Catalog_X"], on_full_load=False)
    loud = Spy(name='loud', on=["Catalog_X"])
    runner = _runner_for(db, quiet)
    _runner_for(db, loud)

    signals = HandlerSignals(db.engine, db.schema)
    signals.request_full_rebuild("Catalog_X", "new column", SOURCE_FULL_LOAD)

    assert not _state(runner, 'quiet').full_rebuild_is_required, \
        'бэкфилл не должен заказывать пересборку тому, кто от бэкфилла отписался'
    assert _state(runner, 'loud').full_rebuild_is_required

    # Живое изменение — другое дело: там колонка касается всех подписчиков.
    signals.request_full_rebuild("Catalog_X", "new column", SOURCE_CHANGES)
    assert _state(runner, 'quiet').full_rebuild_is_required


def test_replicator_passes_the_source_to_the_signal(db):
    """Тот же гейт на боевом пути: источник доезжает от репликатора до handlers."""
    quiet = Spy(name='quiet', on=["Catalog_X"], on_full_load=False)
    runner = _runner_for(db, quiet)
    rep = _replicator(db)

    _replicator_signal(rep, "Catalog_X", _result(updated=1), SOURCE_FULL_LOAD)
    assert _state(runner, 'quiet').update_requested_at is None, \
        'бэкфилл не должен будить того, кто от бэкфилла отписался'

    _replicator_signal(rep, "Catalog_X", _result(updated=1), SOURCE_CHANGES)
    assert _state(runner, 'quiet').update_requested_at is not None


def test_a_handler_looks_again_once_the_replicator_can_see_it(db, monkeypatch):
    """
    Слепое окно регистрации. Репликатор держит подписки в кэше до SUBSCRIPTIONS_TTL, поэтому
    изменения, сохранённые сразу после старта обработчика, сигнала не получают: в кэше его ещё
    нет. Раньше такие строки ждали следующего изменения этой таблицы — недели, без единой ошибки
    в логе. Теперь обработчик смотрит своё окно ещё раз, когда кэш точно перечитан.
    """
    import onecdc.handlers as handlers_module

    monkeypatch.setattr(handlers_module, 'SUBSCRIPTIONS_TTL', 0.0)
    spy = Spy()
    runner, _ = _runner(db, spy)
    runner.run_if_pending()          # стартовый прогон
    spy.calls.clear()
    # Изменение, попавшее в слепое окно: сигнала по нему не было.
    assert _state(runner, spy.name).update_requested_at is None

    runner._catch_up_at = 0.0        # TTL истёк — подписки репликатора перечитаны
    runner.run_if_pending()

    assert len(spy.calls) == 1, 'догонялка не сработала — строки слепого окна ждут неизвестно чего'
    # И только один раз: она разовая, в холостую каждый проход не бегает.
    spy.calls.clear()
    runner.run_if_pending()
    assert spy.calls == []


def test_the_replicator_signals_a_disabled_handler_too(db):
    # Решение «не запускаться» принимает сам цикл, первым же делом. Отметка выключенному не
    # вредна — она копится и будет прочитана при включении.
    quiet = Spy(name='quiet', on=["Catalog_X"])
    runner = _runner_for(db, quiet)
    with runner.engine.begin() as conn:
        conn.execute(runner.table.update().where(runner.table.c.name == 'quiet')
                     .values(enabled=False))

    _signal(db, "Catalog_X", SOURCE_CHANGES)

    assert _state(runner, 'quiet').update_requested_at is not None
