"""
Живой smoke-тест Replicator против реального сервера 1С (тестовая база торговли) и
dev-Postgres. Параметры подключения берутся из debug_trade.py — там они и правятся, чтобы контуры не
разъезжались (раньше здесь стояла своя копия, и адрес 1С успел разойтись с отладочным).

Гоняет полный цикл Replicator.run_once(notify_changes=False): read → save без notify, чтобы
не списывать изменения из очереди обмена 1С и оставить прогон повторяемым. Проверяет, что
метаданные читаются с живой 1С и изменения сохраняются в БД без ошибок.

Требует доступной 1С и Postgres (помечен маркером integration). Не настроен контур — тест
пропускается; недоступен сам стенд — падает, и это разные вещи. Запуск:
  - через pytest: `uv run pytest tests/test_cdc_run_once.py`;
  - напрямую, без pytest: `uv run python tests/test_cdc_run_once.py`.
"""

import pytest
import uuid
from dbmerge import MergeTracker
from sqlalchemy import create_engine, inspect, select, text

from onecdc import Replicator

# Тестовый/dev-контур, не боевой (debug_trade.py лежит рядом и импортируется как обычный модуль).
from debug_trade import CONTOUR, DB_SCHEMA, DB_URL, EXCHANGE_NAME, ODATA_AUTH, ODATA_URL, QUEUE_GUID
from debug_config import contour


@pytest.mark.integration
def test_run_once_against_live_1c():
    # Не настроен контур — пропускаем, а не падаем: маркер integration отсеивает тест после
    # сбора, и «нет настроек» — это не провал проверки, а её отсутствие.
    if not CONTOUR.is_configured:
        pytest.skip(CONTOUR.why_not)

    repl = Replicator(
        odata_url=ODATA_URL,
        odata_auth=ODATA_AUTH,
        exchange_name=EXCHANGE_NAME,
        queue_guid=QUEUE_GUID,
        engine=create_engine(DB_URL),
        db_schema=DB_SCHEMA,
    )

    try:
        # Полный цикл read → save без notify: изменения не списываются из очереди, прогон
        # повторяемый.
        repl.run_once(notify_changes=False)

        # Метаданные реально прочитаны; если были изменения — в целевой схеме есть таблицы.
        assert len(repl.metadata) > 0
        if len(repl.changes) > 0:
            assert inspect(repl.engine).get_table_names(schema=DB_SCHEMA)
    finally:
        # Отпускаем аренду узла обмена явно. Без этого повторный прогон в ближайшие 15 минут
        # пропустил бы цикл (узел числился бы за завершившимся процессом) и прошёл бы вхолостую,
        # не проверив ничего, — а выглядел бы успешным.
        repl.close()


@pytest.mark.integration
@pytest.mark.parametrize('contour_name', ['trade_demo1', 'buh_demo1'])
def test_live_1c_with_dbmerge_tracker(contour_name):
    """Два повторяемых CDC-прохода с новым журналом, без подтверждения пакета.

    Данные пишутся в отдельную временную схему и удаляются после теста.
    Если пакет содержит источники примеров, выполняем также rebuild витрин.
    """
    settings = contour(contour_name)
    if not settings.is_configured:
        pytest.skip(settings.why_not)
    engine = create_engine(settings.db_url, pool_pre_ping=True)
    schema = f'onecdc_live_{uuid.uuid4().hex[:12]}'
    try:
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        with MergeTracker(engine, schema=schema) as tracker:
            repl = Replicator(odata_url=settings.odata_url, odata_auth=settings.odata_auth,
                              exchange_name=settings.exchange_name, queue_guid=settings.queue_guid,
                              engine=engine, db_schema=schema, tracker=tracker,
                              automatic_full_load=False)
            try:
                for attempt in range(2):
                    repl.run_once(notify_changes=False)
                    assert repl.metadata.is_loaded, 'CDC cycle skipped (exchange node busy?)'
                    assert len(repl.metadata) > 0
                    assert not repl.changes.failed_objects, repl.changes.failed_objects
                    with engine.connect() as conn:
                        operations = conn.execute(select(tracker.table).where(
                            tracker.table.c.row_kind == 'operation')).mappings().all()
                    assert all(row['finished_at'] is not None for row in operations)
                    if repl.changes.rows_read():
                        assert operations, 'source writes did not reach dbmerge journal'
                    print(f'{contour_name}: pass={attempt + 1}, '
                          f'metadata={len(repl.metadata)}, rows={repl.changes.rows_read()}, '
                          f'journal_operations={len(operations)}')

                from config.handlers import ZakazyKlientov, ZakazyKlientovGrouped
                from dbmerge import reset_stop
                tables = set(inspect(engine).get_table_names(schema=schema))
                if contour_name == 'trade_demo1':
                    required = set(ZakazyKlientovGrouped.sources)
                    for source in sorted(required - tables):
                        modified = repl.full_load(source, skip_if_busy=False)
                        print(f'{contour_name}: full_load {source}, modified={modified}')
                        record = tracker.last_merge(source, schema=schema)
                        assert record is not None and record.scope == 'full_load'
                    tables = set(inspect(engine).get_table_names(schema=schema))
                    assert required <= tables
                reset_stop()
                try:
                    for cls in (ZakazyKlientov, ZakazyKlientovGrouped):
                        if not set(cls.sources) <= tables:
                            print(f'{contour_name}: {cls.__name__} not exercised; '
                                  f'missing sources: {sorted(set(cls.sources) - tables)}')
                            continue
                        mart = cls(engine, tracker=tracker, schema=schema)
                        assert mart.run_once(rebuild=True)
                        assert tracker.watermark(cls.__name__, schema=schema) is not None
                        with engine.connect() as conn:
                            actual = conn.execute(text(
                                f'SELECT count(*), sum("Zakazano") '
                                f'FROM "{schema}"."{cls.__name__}"')).one()
                            expected = conn.execute(text(
                                f'SELECT count(*), sum("Zakazano") '
                                f'FROM "{schema}"."{cls.__name__}_view"')).one()
                        assert actual == expected
                        assert not mart.run_once(), 'no source changes: mart should stay idle'
                        print(f'{contour_name}: {cls.__name__} rebuild passed, '
                              f'rows={actual[0]}, total={actual[1]}; idle pass verified')
                finally:
                    reset_stop()
            finally:
                repl.close()
    finally:
        try:
            with engine.begin() as conn:
                conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        finally:
            engine.dispose()


if __name__ == "__main__":
    # Обычный запуск без pytest: `uv run python tests/test_cdc_run_once.py`.
    import logging

    logging.basicConfig(level=logging.INFO)
    test_run_once_against_live_1c()
    print("OK: цикл read → save отработал (notify отключён, изменения остались в очереди).")
