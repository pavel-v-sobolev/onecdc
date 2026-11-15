"""Интеграция нового журнала и витрин; реальные транзакции PostgreSQL, без 1С."""

from datetime import datetime
from pathlib import Path
import runpy
import threading
from unittest.mock import patch

import pytest
from dbmerge import MergeTracker
from sqlalchemy import (Boolean, Column, Integer, MetaData, Numeric, String, Table,
                        insert, select, update, delete)

from config.handlers.orders_by_customer import OrdersByCustomer
from onecdc import DataObject, NameMapper
from onecdc.db_writer import DBWriter
from onecdc.marts import (rebuild_requested, request_rebuild, request_stop, reset_stop,
                          run_mart)
from onecdc.metadata_reader import MetadataObject
from test_mark_missing import CATALOG, _pages, _record, _replicator


@pytest.fixture(autouse=True)
def reset_mart_stop():
    reset_stop()
    yield
    reset_stop()


@pytest.fixture
def tracker(db):
    with MergeTracker(db.engine, schema=db.schema) as tracker:
        yield tracker


def test_writer_tracks_changes_noops_and_full_load(db, tracker):
    writer = DBWriter(db.engine, NameMapper(), schema=db.schema, tracker=tracker)
    meta = MetadataObject('Catalog_X', {'Ref_Key': 'String', 'Val': 'String'},
                          {'Ref_Key': 'String'}, object_key=None)

    def save(value, **kwargs):
        return writer.save('Catalog_X', DataObject(meta, [{
            'Ref_Key': 'a', 'Val': value, 'is_deleted_or_empty': False,
            'exchange_message_no': 1}]), **kwargs)

    assert save('first').inserted_row_count == 1
    assert tracker.last_merge('Catalog_X', schema=db.schema).scope == 'changes'
    save('first')
    assert not tracker.last_merge('Catalog_X', schema=db.schema).changed
    result = save('second', full_load_started_at=datetime(2100, 1, 1))
    assert result.updated_row_count == 1
    record = tracker.last_merge('Catalog_X', schema=db.schema)
    assert record.changed and record.scope == 'full_load'


def test_mark_missing_is_tracked_even_when_full_load_has_no_pages(db, tracker, monkeypatch):
    rep = _replicator(db)
    rep.writer.tracker = tracker
    try:
        rep.writer.save(CATALOG, DataObject(rep.metadata[CATALOG], [
            _record(Ref_Key='gone', Val='old')]))
        _pages(rep, CATALOG, monkeypatch, [[]])
        rep.full_load(CATALOG)
        record = tracker.last_merge(CATALOG, schema=db.schema)
        assert record.changed and record.scope == 'full_load'
    finally:
        rep.close()


def _orders(db, tracker):
    table = Table('orders', MetaData(), Column('id', Integer, primary_key=True),
                  Column('customer_id', String(255)), Column('amount', Numeric),
                  Column('is_deleted_or_empty', Boolean, nullable=False, default=False),
                  schema=db.schema)
    table.create(db.engine)
    handler = OrdersByCustomer(db.engine, destination=(db.schema, 'totals'),
                               sources=[(db.schema, 'orders')])

    def write(statement):
        def operation():
            with db.engine.begin() as conn:
                return conn.execute(statement).rowcount
        tracker.exec(operation, destination=(db.schema, 'orders'), scope='changes')

    return table, handler, write


def _totals(db):
    table = Table('totals', MetaData(), schema=db.schema, autoload_with=db.engine)
    with db.engine.connect() as conn:
        return dict(conn.execute(select(table.c.customer_id, table.c.total)).all())


def test_totals_handle_updates_moves_soft_and_hard_deletes(db, tracker):
    orders, handler, write = _orders(db, tracker)
    loop = handler.loop(tracker)
    try:
        write(insert(orders).values([
            {'id': 1, 'customer_id': 'a', 'amount': 10},
            {'id': 2, 'customer_id': 'a', 'amount': 20}]))
        assert loop.run_if_pending()
        assert _totals(db) == {'a': 30}
        assert not loop.run_if_pending()
        write(update(orders).where(orders.c.id == 1).values(amount=15))
        assert loop.run_if_pending()
        assert _totals(db) == {'a': 35}
        write(update(orders).where(orders.c.id == 1).values(customer_id='b'))
        assert loop.run_if_pending()
        assert _totals(db) == {'a': 20, 'b': 15}
        write(update(orders).where(orders.c.id == 2).values(is_deleted_or_empty=True))
        assert loop.run_if_pending()
        assert _totals(db) == {'b': 15}
        write(delete(orders))
        assert loop.run_if_pending()
        assert _totals(db) == {}
    finally:
        loop.close()


def test_rebuild_failure_keeps_resume_state_and_watermark(db, tracker):
    orders, handler, write = _orders(db, tracker)
    write(insert(orders).values(id=1, customer_id='a', amount=10))
    loop = handler.loop(tracker)
    try:
        assert loop.run_if_pending()
        before = tracker.watermark('totals', schema=db.schema)
        request_rebuild(db.engine, handler.destination, schema=db.schema)
        with patch.object(handler, 'process_changes', side_effect=RuntimeError('fail')):
            with pytest.raises(RuntimeError, match='fail'):
                loop.run_if_pending()
        # dbmerge consumes the request by persisting an active rebuild cursor.
        # A failed block must resume even though the request flag is already clear.
        assert loop._rebuild_cursor() == ''
        assert tracker.watermark('totals', schema=db.schema) == before
        assert loop.run_if_pending()
        assert not rebuild_requested(db.engine, handler.destination, schema=db.schema)
        assert _totals(db) == {'a': 10}
    finally:
        loop.close()


def test_new_rebuild_request_during_rebuild_is_not_lost(db, tracker):
    _, handler, _ = _orders(db, tracker)
    request_rebuild(db.engine, handler.destination, schema=db.schema)
    loop = handler.loop(tracker)
    original = handler.process_changes

    def rebuild(ctx):
        original(ctx)
        request_rebuild(db.engine, handler.destination, schema=db.schema)

    try:
        with patch.object(handler, 'process_changes', side_effect=rebuild):
            assert loop.run_if_pending()
        assert rebuild_requested(db.engine, handler.destination, schema=db.schema)
        assert loop.run_if_pending()
        assert not rebuild_requested(db.engine, handler.destination, schema=db.schema)
    finally:
        loop.close()


def test_rebuild_requests_are_business_schema_isolated(db):
    request_rebuild(db.engine, ('first', 'totals'), schema=db.schema)
    assert rebuild_requested(db.engine, ('first', 'totals'), schema=db.schema)
    assert not rebuild_requested(db.engine, ('second', 'totals'), schema=db.schema)


def test_run_mart_supports_functions_and_preserves_borrowed_tracker(db, tracker):
    calls = []

    def process(ctx):
        calls.append(ctx)

    process.destination = (db.schema, 'function_mart')
    process.sources = [(db.schema, 'orders')]
    request_rebuild(db.engine, process.destination, schema=db.schema)
    assert run_mart(process, db.engine, tracker=tracker, run_forever=False)
    assert len(calls) == 1
    # The external tracker still accepts writes after the loop exits.
    tracker.exec(lambda: True, destination=(db.schema, 'orders'))


def test_stop_is_sticky_for_new_loops(db, tracker):
    _, handler, _ = _orders(db, tracker)
    request_rebuild(db.engine, handler.destination, schema=db.schema)
    request_stop()
    assert not run_mart(handler, tracker=tracker, run_forever=False)
    assert rebuild_requested(db.engine, handler.destination, schema=db.schema)
    reset_stop()
    assert run_mart(handler, tracker=tracker, run_forever=False)


def test_owned_tracker_closes_when_loop_setup_fails(db):
    from dbmerge import mart

    _, handler, _ = _orders(db, None)
    created = []

    def create(*args, **kwargs):
        tracker = MergeTracker(*args, **kwargs)
        created.append(tracker)
        return tracker

    with patch.object(mart, 'MergeTracker', side_effect=create):
        with patch.object(handler, 'loop', side_effect=RuntimeError('setup')):
            with pytest.raises(RuntimeError, match='setup'):
                run_mart(handler, schema=db.schema, run_forever=False)
        assert len(created) == 1 and created[0]._closed.is_set()


def test_mart_destination_has_exclusive_lease(db, tracker):
    _, handler, _ = _orders(db, tracker)
    request_rebuild(db.engine, handler.destination, schema=db.schema)
    first, second = handler.loop(tracker), handler.loop(tracker)
    try:
        assert first._lease.acquire(first.name)
        assert not second.run_if_pending()
        first._lease.release(first.name)
        assert second.run_if_pending()
    finally:
        first.close()
        second.close()


def test_writer_failure_leaves_unfinished_operation_for_recovery(db, tracker):
    writer = DBWriter(db.engine, NameMapper(), schema=db.schema, tracker=tracker)

    class FailingMerge:
        table_name = 'orders'
        schema = db.schema

        def exec(self):
            raise RuntimeError('write failed')

    with pytest.raises(RuntimeError, match='write failed'):
        writer._exec_merge(FailingMerge(), scope='changes')
    with db.engine.connect() as conn:
        row = conn.execute(select(tracker.table).where(
            tracker.table.c.row_kind == 'operation')).mappings().one()
    assert row['finished_at'] is None and row['scope'] == 'changes'
    assert tracker.watermark('orders', schema=db.schema) is None


def test_replicator_passes_tracker_without_owning_it(db, tracker):
    from onecdc import Replicator

    rep = Replicator(odata_url='http://x', odata_auth=None, engine=db.engine,
                     db_schema=db.schema, tracker=tracker)
    try:
        assert rep.writer.tracker is tracker
    finally:
        rep.close()
    tracker.exec(lambda: True, destination=(db.schema, 'orders'))


def test_runner_import_has_no_startup_side_effects(monkeypatch):
    config = Path(__file__).resolve().parent.parent / 'config'
    monkeypatch.syspath_prepend(str(config))
    with patch('sqlalchemy.create_engine') as create, patch('signal.signal') as signals:
        namespace = runpy.run_path(str(config / 'runner_marts.py'))
    create.assert_not_called()
    signals.assert_not_called()
    assert callable(namespace['main'])


def test_runner_stops_other_workers_and_propagates_startup_failure(monkeypatch):
    config = Path(__file__).resolve().parent.parent / 'config'
    monkeypatch.syspath_prepend(str(config))
    namespace = runpy.run_path(str(config / 'runner_marts.py'))
    # main's globals are distinct from the dict returned by run_path.
    globals_ = namespace['main'].__globals__
    stopped = threading.Event()

    def fail(*args, **kwargs):
        raise RuntimeError('worker startup')

    monkeypatch.setitem(globals_, 'run_mart', fail)
    monkeypatch.setitem(globals_, 'request_stop', lambda *args: stopped.set())
    with patch('signal.signal'):
        with pytest.raises(RuntimeError, match='worker startup'):
            namespace['main']()
    assert stopped.is_set()
