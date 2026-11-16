"""Интеграция нового журнала и витрин; реальные транзакции PostgreSQL, без 1С."""

from datetime import datetime
from pathlib import Path
import runpy
import subprocess
import sys
import importlib
import threading
from unittest.mock import patch

import pytest
from dbmerge import MergeTracker, MartHandler, IncorrectParameter
from sqlalchemy import (Boolean, Column, Integer, MetaData, Numeric, String, Table,
                        insert, select, update, delete)

from config.handlers.orders_by_customer import OrdersByCustomer
from onecdc import DataObject, NameMapper
from onecdc.db_writer import DBWriter
from dbmerge import rebuild_requested, request_rebuild, request_stop, reset_stop
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
    handler = OrdersByCustomer(db.engine, tracker=tracker, destination=(db.schema, 'totals'),
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
    loop = handler.loop()
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
    loop = handler.loop()
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
    loop = handler.loop()
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


def test_mart_preserves_caller_owned_tracker(db, tracker):
    _, handler, _ = _orders(db, tracker)
    assert handler.run_once(rebuild=True)
    tracker.exec(lambda: True, destination=(db.schema, 'orders'))


def test_compatibility_imports_are_dbmerge_api():
    from onecdc import marts
    assert marts.MartHandler is MartHandler
    assert marts.request_stop is request_stop
    assert not hasattr(marts, 'run_mart')


def test_stop_is_sticky_for_new_loops(db, tracker):
    _, handler, _ = _orders(db, tracker)
    request_rebuild(db.engine, handler.destination, schema=db.schema)
    request_stop()
    assert not handler.run_once()
    assert rebuild_requested(db.engine, handler.destination, schema=db.schema)
    reset_stop()
    assert handler.run_once()


def test_mart_requires_caller_owned_tracker(db):
    with pytest.raises(IncorrectParameter):
        _orders(db, None)


def test_mart_destination_has_exclusive_lease(db, tracker):
    _, handler, _ = _orders(db, tracker)
    request_rebuild(db.engine, handler.destination, schema=db.schema)
    first, second = handler.loop(), handler.loop()
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


def test_replicator_creates_and_closes_default_tracker(db):
    from onecdc import Replicator

    rep = Replicator(odata_url='http://x', odata_auth=None, engine=db.engine,
                     db_schema=db.schema)
    try:
        assert rep.tracker is rep.writer.tracker
        assert rep.tracker.schema == db.schema
        assert rep._owns_tracker
        meta = MetadataObject('Catalog_X', {'Ref_Key': 'String', 'Val': 'String'},
                              {'Ref_Key': 'String'}, object_key=None)
        rep.writer.save('Catalog_X', DataObject(meta, [{
            'Ref_Key': 'a', 'Val': 'value', 'is_deleted_or_empty': False,
            'exchange_message_no': 1}]))
        assert rep.tracker.last_merge('Catalog_X', schema=db.schema).changed
    finally:
        rep.close()
    assert rep.tracker._closed.is_set()
    rep.close()  # repeated close is safe


def test_external_tracker_keeps_separate_schema_and_remains_open(db):
    from onecdc import Replicator
    from sqlalchemy import inspect, text

    tracking_schema = db.schema + '_tracking'
    try:
        with MergeTracker(db.engine, schema=tracking_schema) as tracker:
            rep = Replicator(odata_url='http://x', odata_auth=None, engine=db.engine,
                             db_schema=db.schema, tracker=tracker)
            try:
                assert rep.tracker is tracker and rep.writer.tracker is tracker
                assert not rep._owns_tracker
                meta = MetadataObject('Catalog_X', {'Ref_Key': 'String', 'Val': 'String'},
                                      {'Ref_Key': 'String'}, object_key=None)
                rep.writer.save('Catalog_X', DataObject(meta, [{
                    'Ref_Key': 'a', 'Val': 'value', 'is_deleted_or_empty': False,
                    'exchange_message_no': 1}]))
                record = tracker.last_merge('Catalog_X', schema=db.schema)
                assert record.changed and record.scope == 'changes'
                assert 'dbmerge_tracking' not in inspect(db.engine).get_table_names(schema=db.schema)
                assert 'Catalog_X' in inspect(db.engine).get_table_names(schema=db.schema)
            finally:
                rep.close()
            assert not tracker._closed.is_set()
            tracker.exec(lambda: True, destination=(db.schema, 'orders'))
    finally:
        with db.engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{tracking_schema}" CASCADE'))


@pytest.mark.parametrize('closed', [False, True])
def test_replicator_rejects_invalid_or_closed_tracker(db, closed):
    from onecdc import Replicator

    tracker = MergeTracker(db.engine, schema=db.schema) if closed else object()
    if closed:
        tracker.close()
    with pytest.raises(ValueError, match='open MergeTracker'):
        Replicator(odata_url='http://x', odata_auth=None, engine=db.engine,
                   db_schema=db.schema, tracker=tracker)


def test_runner_import_has_no_startup_side_effects(monkeypatch):
    config = Path(__file__).resolve().parent.parent / 'config'
    monkeypatch.syspath_prepend(str(config))
    with patch('sqlalchemy.create_engine') as create, patch('signal.signal') as signals:
        namespace = runpy.run_path(str(config / 'runner_marts.py'))
    create.assert_not_called()
    signals.assert_not_called()
    assert callable(namespace['main'])


def test_mart_example_and_runner_do_not_import_onecdc():
    root = Path(__file__).resolve().parent.parent
    subprocess.run([sys.executable, '-c',
                    "import runpy, sys; "
                    "sys.path.insert(0, 'config'); "
                    "import handlers; "
                    "[getattr(handlers, n) for n in handlers.__all__]; "
                    "runpy.run_path('config/runner_marts.py'); "
                    "assert not any(n == 'onecdc' or n.startswith('onecdc.') "
                    "for n in sys.modules)"], cwd=root, check=True)


@pytest.mark.parametrize('module_name,class_name', [
    ('zakazy_klientov', 'ZakazyKlientov'),
    ('zakazy_klientov_grouped', 'ZakazyKlientovGrouped'),
])
def test_postgres_examples_track_and_borrow_pass_lease(db, tracker, monkeypatch,
                                                     module_name, class_name):
    from datetime import datetime
    from decimal import Decimal
    import uuid
    from sqlalchemy import text
    from test_clickhouse_handlers import _source_tables, _movement, _nomenklatura, _document

    _source_tables(db)
    order, item = uuid.uuid4(), uuid.uuid4()
    _nomenklatura(db, item, 'A')
    _movement(db, order, 1, datetime(2026, 6, 7), 5, item)
    _document(db, order, 'Z-1', datetime(2026, 6, 7))
    if class_name == 'ZakazyKlientov':
        with db.engine.begin() as conn:
            conn.execute(text(f'INSERT INTO "{db.schema}"."AccumulationRegister_ZakazyKlientov" '
                              '("Recorder", "Recorder_Type", "LineNumber", "is_deleted_or_empty") '
                              'VALUES (:r, :t, 1, true)'),
                         {'r': uuid.uuid4(), 't': 'Document_ЗаказКлиента'})
    module = importlib.import_module(f'config.handlers.{module_name}')
    original = module.dbmerge
    calls = []

    def guarded_merge(*args, **kwargs):
        assert kwargs['tracker'] is tracker
        handle = kwargs['lease_handle']
        assert handle is not None
        handle.validate(tracker)
        calls.append(handle)
        return original(*args, **kwargs)

    monkeypatch.setattr(module, 'dbmerge', guarded_merge)
    handler = getattr(module, class_name)(db.engine, tracker=tracker, schema=db.schema)
    assert handler.run_once(rebuild=True)
    assert calls
    assert tracker.last_merge(class_name, schema=db.schema).changed
    with db.engine.connect() as conn:
        assert conn.scalar(text(f'SELECT sum("Zakazano") FROM "{db.schema}"."{class_name}"')) == Decimal(5)
        if class_name == 'ZakazyKlientov':
            assert conn.scalar(text(f'SELECT count(*) FROM "{db.schema}"."{class_name}" '
                                    'WHERE "Period" IS NULL')) == 1
    # Change the catalog; the regular changes path must borrow its own pass handle too.
    def change_catalog():
        with db.engine.begin() as conn:
            conn.execute(text(f'UPDATE "{db.schema}"."Catalog_Nomenklatura" '
                              'SET "Code" = \'B\', merged_on = clock_timestamp()'))
        return True

    tracker.exec(change_catalog, destination=(db.schema, 'Catalog_Nomenklatura'))
    before = len(calls)
    assert handler.run_once()
    assert len(calls) > before
    with db.engine.connect() as conn:
        assert conn.scalar(text(f'SELECT "Artikul" FROM "{db.schema}"."{class_name}" '
                                'WHERE "Zakazano" = 5')) == 'B'


def test_example_lost_pass_lease_does_not_advance_watermark(db, tracker, monkeypatch):
    from dbmerge import LeaseLost
    from config.handlers import orders_by_customer as module

    orders, handler, write = _orders(db, tracker)
    write(insert(orders).values(id=1, customer_id='a', amount=10))
    original = module.dbmerge

    def lose_lease(*args, **kwargs):
        handle = kwargs['lease_handle']
        handle._lease.release(handle.name)
        return original(*args, **kwargs)

    monkeypatch.setattr(module, 'dbmerge', lose_lease)
    with pytest.raises(LeaseLost):
        handler.run_once()
    assert tracker.watermark('totals', schema=db.schema) is None
    assert _totals(db) == {}


def test_runner_stops_other_workers_and_propagates_startup_failure(monkeypatch):
    config = Path(__file__).resolve().parent.parent / 'config'
    monkeypatch.syspath_prepend(str(config))
    namespace = runpy.run_path(str(config / 'runner_marts.py'))
    # main's globals are distinct from the dict returned by run_path.
    globals_ = namespace['main'].__globals__
    stopped = threading.Event()

    def fail(*args, **kwargs):
        raise RuntimeError('worker startup')

    monkeypatch.setattr(globals_['OrdersByCustomer'], 'run_forever', fail)
    monkeypatch.setitem(globals_, 'request_stop', lambda *args: stopped.set())
    with patch('signal.signal'):
        with pytest.raises(RuntimeError, match='worker startup'):
            namespace['main']()
    assert stopped.is_set()
