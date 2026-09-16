"""
Остановка процесса должна быть предсказуемой по времени.

`docker stop` даёт `stop_grace_period` (в примере compose — 60 секунд) и добивает `SIGKILL`.
Значит цена всякой задержки на остановке — незакрытый merge: staging-таблица и строка в реестре
незавершённых записей, которую витрина ждёт до TTL. Восстановление есть (брошенную строку разбирает
deliver_abandoned, выгрузка идемпотентна), но платить за него на каждой выкатке незачем.
"""

import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from onecdc import stop_signal as stop_module
from onecdc.stop_signal import StopSignal, handle_stop_signal, stop_requested


def test_a_signal_arriving_before_any_loop_is_not_swallowed(monkeypatch):
    """
    Сигнал приходит и тогда, когда цикла ещё нет: конструкторы, загрузка метаданных, первый
    run_once, ручной full_load.

    Раньше обработчик в этом случае не делал НИЧЕГО — а перехват уже заменил поведение по
    умолчанию. То есть с библиотекой SIGTERM переставал завершать процесс, и `docker stop` добивал
    `SIGKILL` по истечении grace period. Хуже, чем не ставить перехват вовсе.
    """
    import weakref

    raised = []
    monkeypatch.setattr(stop_module.signal, 'raise_signal', raised.append)
    monkeypatch.setattr(stop_module.signal, 'signal', lambda *a: None)
    # Пустой список циклов — условие теста. Подменяем его, а не полагаемся на то, что в процессе
    # не осталось живых: в общем прогоне их держат объекты соседних тестов.
    monkeypatch.setattr(stop_module, '_stop_signals', weakref.WeakSet())

    handle_stop_signal(signal.SIGTERM, None)

    assert raised == [signal.SIGTERM], 'сигнал проглочен: процесс не остановится и не умрёт'


def test_a_loop_created_after_the_signal_is_born_stopped():
    # SIGTERM за миг до run_forever иначе терялся бы совсем.
    stop_module._stop_requested = True
    try:
        assert StopSignal().requested is True
    finally:
        stop_module.reset_stop_request()


def test_the_host_handler_is_called_too():
    # Библиотека не вправе молча забрать остановку у приложения-хозяина.
    called = []
    stop = StopSignal()
    stop_module._previous_handlers[signal.SIGTERM] = lambda signum, frame: called.append(signum)
    try:
        handle_stop_signal(signal.SIGTERM, None)
        assert called == [signal.SIGTERM]
        assert stop.requested
    finally:
        stop_module._previous_handlers.pop(signal.SIGTERM, None)
        stop_module.reset_stop_request()


def test_queued_work_is_cancelled_not_drained():
    """
    Выход из `with ThreadPoolExecutor` — это shutdown(wait=True): пул дожидался ВСЕХ поставленных
    заданий, включая ещё не начатые. А ставятся они разом, по всему списку заказов на выгрузку —
    десяток объектов первичной выгрузки это часы, которых grace period не покрывает.
    """
    started, finished = [], []
    release = threading.Event()

    def task(n):
        started.append(n)
        release.wait(5)
        finished.append(n)

    executor = ThreadPoolExecutor(max_workers=1)
    try:
        for i in range(5):
            executor.submit(task, i)
        while not started:
            time.sleep(0.01)
    finally:
        release.set()
        executor.shutdown(wait=True, cancel_futures=True)

    assert len(finished) == 1, 'очередь доработана целиком вместо отмены'
    assert len(started) == 1


def test_a_full_load_stops_at_a_page_boundary(db, monkeypatch):
    """
    Идущая выгрузка обязана прерваться сама — иначе ждать пришлось бы весь её прогон, а он длится
    часами. Прерванная НЕ считается успешной: заказ остаётся, после перезапуска прогон начнётся
    заново, и повторно тратится только время на перечитывание страниц.
    """
    from onecdc import DataObject
    from onecdc.data_reader import DataReader
    from onecdc.metadata_reader import MetadataObject
    from onecdc.replicator import FullLoadStopped

    from conftest import TEST_QUEUE_GUID
    from onecdc import Replicator

    repl = Replicator(odata_url="http://x", odata_auth=None, exchange_name="E",
                      queue_guid=TEST_QUEUE_GUID, engine=db.engine, db_schema=db.schema)
    try:
        meta = MetadataObject("Catalog_X", {"Ref_Key": "Guid"}, {"Ref_Key": "Guid"},
                              object_key=None)
        repl.metadata["Catalog_X"] = meta
        repl.metadata.is_loaded = True
        pages = {'read': 0}

        def fake_read(self, object_name, top=None, key_fields=None, extra_filter=None, skip=None):
            pages['read'] += 1
            if pages['read'] == 2:
                stop_module._stop_requested = True     # сигнал пришёл посреди прогона
            self.clear()
            self[object_name] = DataObject(meta, [{"Ref_Key": f"{pages['read']:08d}-0000-0000-"
                                                              f"0000-000000000000"}])
            return 1

        monkeypatch.setattr(DataReader, "read_object", fake_read)
        monkeypatch.setattr(DataReader, "read_date_bound", lambda *a, **k: None)

        with pytest.raises(FullLoadStopped):
            repl.full_load("Catalog_X", batch_size=1)

        assert pages['read'] == 2, 'выгрузка не прервалась на границе страницы'
    finally:
        stop_module.reset_stop_request()
        repl.close()


def test_stop_requested_is_visible_without_a_loop():
    # Ручной full_load крутится без цикла, и своего StopSignal у него нет.
    assert stop_requested() is False
    stop_module._stop_requested = True
    try:
        assert stop_requested() is True
    finally:
        stop_module.reset_stop_request()


def test_installing_twice_does_not_make_the_handler_call_itself():
    """
    Установка бывает повторной: сработал конструктор, а потом хозяин позвал install явно.

    Тогда `signal.getsignal` возвращает НАС, и запись этого «прежним» превращала обработчик в
    вызов самого себя — остановка кончалась RecursionError вместо остановки. Запоминаем
    первоначального владельца сигнала.
    """
    original = stop_module._previous_handlers.get(signal.SIGTERM)
    try:
        # Предусловие делаем явным: сперва наш обработчик обязан ВСТАТЬ, иначе повторная установка
        # запомнит SIG_DFL и тест ничего не проверит.
        stop_module.install_signal_handlers()
        assert signal.getsignal(signal.SIGTERM) is handle_stop_signal

        stop_module._handlers_installed = False      # хозяин зовёт установку повторно
        stop_module.install_signal_handlers()

        assert stop_module._previous_handlers.get(signal.SIGTERM) is not handle_stop_signal
        stop = StopSignal()
        handle_stop_signal(signal.SIGTERM, None)      # не должно уйти в рекурсию
        assert stop.requested
    finally:
        if original is not None:
            stop_module._previous_handlers[signal.SIGTERM] = original
        stop_module.reset_stop_request()
