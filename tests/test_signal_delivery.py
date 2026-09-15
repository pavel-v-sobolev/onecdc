"""
Доставка сигнала обработчику переживает обрыв между записью данных и сигналом.

Merge и сигнал — разные транзакции, и сбой между ними раньше терял уведомление насовсем. Повтор
пакета его не восстанавливал, и это не очевидно: повтор восстанавливает ДАННЫЕ, но уничтожает
основание для сигнала — второй merge не находит отличий, `rows_modified` равен нулю, сигнала нет,
а пакет после этого подтверждается и из очереди 1С исчезает.

Теперь строка в onecdc_writes_in_process живёт от начала merge до ДОСТАВЛЕННОГО сигнала: снимается
она одной транзакцией с ним, а оставшаяся строка — улика, по которой сигнал доставит разбор.
"""

from datetime import timedelta

import pytest
from sqlalchemy import func, select

from onecdc.handlers import HandlerSignals, SOURCE_CHANGES, SOURCE_FULL_LOAD
from onecdc.write_tracker import MERGE_HEARTBEAT_TTL, WriteTracker

TABLE = "Catalog_Nomenklatura"


def _tracker(db, delivered: list, fail_on_signal: bool = False) -> WriteTracker:
    def deliver(conn, object_name, source, result, forced):
        if fail_on_signal:
            raise RuntimeError("connection lost right after commit")
        delivered.append((object_name, source, forced))

    return WriteTracker(db.engine, db.schema, 'План1', deliver_signal=deliver)


def _rows(tracker):
    with tracker.engine.connect() as conn:
        return conn.execute(select(tracker.table)).mappings().all()


def _age_out(tracker):
    """
    Делает строки брошенными — как будто процесс, начавший запись, исчез.

    Владельца подменяем, а не только отматываем отметку: поток отметки живости продлевает строки
    условием `owner = свой`, и, останови мы его хоть трижды, он мог бы оказаться уже внутри
    запроса и освежить строку после нас. С чужим владельцем он не найдёт ничего.
    """
    with tracker.engine.begin() as conn:
        conn.execute(tracker.table.update().values(
            owner='ghost',
            heartbeat_at=func.now() - timedelta(seconds=MERGE_HEARTBEAT_TTL + 60)))


# --- Штатный путь ---

def test_row_is_removed_together_with_the_signal(db):
    delivered = []
    tracker = _tracker(db, delivered)
    try:
        with tracker.track(TABLE, SOURCE_CHANGES) as tracked:
            tracked.result = _merge_result(updated=1)
            assert _rows(tracker), 'пока merge идёт, строка держит границу окна'

        assert delivered == [(TABLE, SOURCE_CHANGES, False)]
        assert _rows(tracker) == [], 'строка снимается вместе с сигналом'
    finally:
        tracker.close()


def test_the_merge_result_reaches_the_delivery(db):
    # Решает, будить ли обработчика, вызывающий (Replicator._deliver_signal: 1С регистрирует
    # объект на любую перезапись, и пакет полон записей, идентичных тем, что уже в БД). Реестр
    # лишь доносит до него результат merge — и снимает строку в любом случае: обязанности нет.
    seen = []
    tracker = WriteTracker(
        db.engine, db.schema, 'План1',
        deliver_signal=lambda conn, obj, source, result, forced: seen.append(result))
    try:
        with tracker.track(TABLE, SOURCE_CHANGES) as tracked:
            tracked.result = _merge_result(updated=0)
        assert seen[0].updated_row_count == 0
        assert _rows(tracker) == []
    finally:
        tracker.close()


# --- Обрыв ---

def test_a_row_left_by_an_interrupted_write_stops_being_renewed(db):
    # Ключевой момент: процесс ЖИВ и продолжает работать, но брошенную строку больше не продлевает.
    # Раньше heartbeat обновлял все строки своего владельца, поэтому такая строка не протухла бы
    # никогда, а граница окна обработчика замёрзла бы вместе с ней навсегда.
    tracker = _tracker(db, [], fail_on_signal=True)
    try:
        with pytest.raises(RuntimeError), tracker.track(TABLE, SOURCE_CHANGES) as tracked:
            tracked.result = _merge_result(updated=1)

        assert _rows(tracker), 'улика должна остаться'
        before = _rows(tracker)[0]['heartbeat_at']

        tracker.heartbeat()     # процесс жив и продолжает продлевать свои merge

        assert _rows(tracker)[0]['heartbeat_at'] == before, \
            'брошенную строку продлевать нельзя — она никогда не протухнет'
    finally:
        tracker.close()


def test_exception_inside_the_block_leaves_the_row_as_evidence(db):
    tracker = _tracker(db, [], fail_on_signal=True)
    try:
        with pytest.raises(RuntimeError), tracker.track(TABLE, SOURCE_FULL_LOAD) as tracked:
            tracked.result = _merge_result(updated=1)

        row = _rows(tracker)[0]
        assert row['object_name'] == TABLE
        assert row['signal_source'] == SOURCE_FULL_LOAD, \
            'без источника отложенный сигнал не пройдёт фильтр on_full_load'
    finally:
        tracker.close()


def test_abandoned_row_delivers_the_signal_it_owed(db):
    delivered = []
    broken = _tracker(db, [], fail_on_signal=True)
    with pytest.raises(RuntimeError), broken.track(TABLE, SOURCE_CHANGES) as tracked:
        tracked.result = _merge_result(updated=1)
    broken.close()
    _age_out(broken)

    # Следующий цикл (или другой процесс) разбирает улику.
    fresh = _tracker(db, delivered)
    try:
        assert fresh.deliver_abandoned() == 1
        assert delivered == [(TABLE, SOURCE_CHANGES, True)]
        assert _rows(fresh) == []
    finally:
        fresh.close()


def test_a_row_of_a_vanished_process_is_delivered_too(db):
    # kill -9: выход из блока не отработал вовсе, строка осталась с протухшей отметкой. Признак
    # тот же, разбор тот же — отдельной ветки для этого не нужно.
    delivered = []
    gone = _tracker(db, [])
    gone.track(TABLE, SOURCE_CHANGES)
    gone.close()
    _age_out(gone)

    fresh = _tracker(db, delivered)
    try:
        assert fresh.deliver_abandoned() == 1
        assert delivered == [(TABLE, SOURCE_CHANGES, True)]
    finally:
        fresh.close()


def test_a_live_write_is_not_mistaken_for_an_interrupted_one(db):
    # Идущий merge держит свежую отметку — разбор обязан пройти мимо, иначе он снял бы границу
    # окна с записи, которая ещё не закоммичена.
    delivered = []
    tracker = _tracker(db, delivered)
    try:
        with tracker.track(TABLE, SOURCE_CHANGES) as tracked:
            assert tracker.deliver_abandoned() == 0
            assert _rows(tracker), 'строка идущего merge на месте'
            tracked.result = _merge_result(updated=1)
    finally:
        tracker.close()


def _merge_result(updated: int):
    from dbmerge import mergeResult
    return mergeResult(total_row_count=updated, inserted_row_count=0, updated_row_count=updated,
                       deleted_row_count=0, total_time=0.0, temp_insert_time=0.0,
                       insert_time=0.0, update_time=0.0, delete_time=0.0)


# --- Сквозной сценарий: обрыв в боевом пайплайне ---

def test_interrupted_packet_still_reaches_the_handler(db, monkeypatch):
    """
    Сценарий целиком: merge закоммичен, сигнал упал, пакет не подтверждён. На повторе merge уже
    ничего не меняет — раньше на этом обработчик терял изменение навсегда. Теперь улика от первой
    попытки разбирается в начале цикла, и сигнал доходит.
    """
    from pathlib import Path

    import fake_1c
    from onecdc import Replicator
    from onecdc.handlers import HandlerLoop

    class Spy:
        ON = ["Catalog_Nomenklatura"]
        name = "spy"

        def handle(self, context):
            pass

    signals = HandlerSignals(db.engine, db.schema)

    def requested():
        with db.engine.connect() as conn:
            return conn.execute(select(signals.table.c.update_requested_at)
                                .where(signals.table.c.name == "spy")).scalar()

    with fake_1c.running_server(Path(__file__).parent / "responses" / "trade_demo_8.5") as (
            odata_url, fake):
        HandlerLoop(db.engine, db.schema, Spy())
        repl = Replicator(odata_url=odata_url, odata_auth=None, exchange_name="ДляODATA",
                          queue_guid=fake.queue_guid, engine=db.engine, db_schema=db.schema)

        monkeypatch.setattr(HandlerSignals, "signal",
                            lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("lost")))
        with pytest.raises(RuntimeError):
            repl.run_once()

        assert requested() is None, 'предпосылка: сигнал не дошёл'
        assert fake.received_no == 0, 'предпосылка: пакет не подтверждён'
        # Данные при этом записаны — именно поэтому повтор merge ничего не изменит.
        assert repl.writes.deliver_abandoned() == 0, 'улика ещё свежая, разбирать рано'

        monkeypatch.undo()
        _age_out(repl.writes)
        repl.run_once()

        assert requested() is not None, 'сигнал не восстановлен после обрыва'
