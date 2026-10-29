"""
Примеры витрин в ClickHouse из config/handlers/*_clickhouse.py — на настоящих базах: исходные
таблицы в PostgreSQL (схема теста, см. conftest.py), витрина в ClickHouse. ClickHouse читает схему
теста сам, через базу с движком PostgreSQL, — как в боевой раскладке.

Обработчики запускаются через HandlerLoop, как в боевом коде: первый прогон идёт пересборкой по
блокам, следующие — по окну после сигнала. Витрина читается с FINAL — так её читает и потребитель.

Адрес ClickHouse — ONECDC_TEST_CLICKHOUSE_URL. Адрес PostgreSQL, по которому до него дотягивается
сам ClickHouse, — ONECDC_TEST_CLICKHOUSE_POSTGRES (host:port); по умолчанию он тот же, что у тестов:
в отдельных контейнерах он будет другим. Каждый тест получает свои базы и удаляет их после.
Нет сервера или драйвера clickhouse-connect — тесты пропускаются: ClickHouse нужен только этим
примерам, а не самой библиотеке.
"""

import os
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import Engine, create_engine, make_url, text

from config.handlers import ZakazyKlientovClickHouse, ZakazyKlientovGroupedClickHouse
from conftest import TEST_DB_URL
from onecdc.handlers import SOURCE_CHANGES, HandlerLoop, HandlerSignals

CLICKHOUSE_URL = os.environ.get(
    "ONECDC_TEST_CLICKHOUSE_URL", "clickhousedb://default:@localhost:8123/default")
_PG = make_url(TEST_DB_URL)
CLICKHOUSE_POSTGRES = os.environ.get("ONECDC_TEST_CLICKHOUSE_POSTGRES",
                                     f"{_PG.host}:{_PG.port or 5432}")

ORDER = 'Document_ЗаказКлиента'


@dataclass(frozen=True)
class ClickHouseDB:
    engine: Engine
    database: str
    # База с движком PostgreSQL, которая смотрит на схему теста.
    source_database: str


@pytest.fixture
def clickhouse(db):
    pytest.importorskip("clickhouse_connect")
    engine = create_engine(CLICKHOUSE_URL)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as error:
        engine.dispose()
        pytest.skip(f"ClickHouse is not available at {CLICKHOUSE_URL}: {error}")

    database = f"onecdc_test_{uuid.uuid4().hex[:8]}"
    source_database = f"{database}_pg"
    with engine.begin() as conn:
        conn.execute(text(f"CREATE DATABASE `{database}`"))
        conn.execute(text(
            f"CREATE DATABASE `{source_database}` ENGINE = PostgreSQL('{CLICKHOUSE_POSTGRES}', "
            f"'{_PG.database}', '{_PG.username}', '{_PG.password}', '{db.schema}')"))
    try:
        yield ClickHouseDB(engine=engine, database=database, source_database=source_database)
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS `{source_database}`"))
            conn.execute(text(f"DROP DATABASE IF EXISTS `{database}`"))
        engine.dispose()


@pytest.fixture
def loops():
    """Циклы теста: их потоки отметки живости гасятся до того, как фикстура db снесёт схему."""
    started = []
    yield started
    for loop in started:
        loop.close()


def _source_tables(db):
    """Те колонки таблиц репликатора, которые читают представления примеров."""
    s = db.schema
    with db.engine.begin() as conn:
        conn.execute(text(f"""
CREATE TABLE "{s}"."AccumulationRegister_ZakazyKlientov" (
    "Recorder" uuid, "Recorder_Type" varchar, "LineNumber" int8, "Period" timestamp,
    "RecordType" varchar, "Nomenklatura_Key" uuid, "Zakazano" numeric,
    "is_deleted_or_empty" boolean NOT NULL DEFAULT false,
    "merged_on" timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY ("Recorder", "Recorder_Type", "LineNumber"));
CREATE TABLE "{s}"."Catalog_Nomenklatura" (
    "Ref_Key" uuid PRIMARY KEY, "Code" varchar,
    "merged_on" timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE "{s}"."Document_ZakazKlienta" (
    "Ref_Key" uuid PRIMARY KEY, "Number" varchar, "Date" timestamp,
    "merged_on" timestamptz NOT NULL DEFAULT clock_timestamp());
"""))


def _movement(db, recorder, line, period, quantity, nomenklatura, record_type='Receipt'):
    _sql(db, 'INSERT INTO "{s}"."AccumulationRegister_ZakazyKlientov" ("Recorder", "Recorder_Type", '
             '"LineNumber", "Period", "RecordType", "Nomenklatura_Key", "Zakazano") '
             'VALUES (:r, :t, :line, :period, :rt, :n, :q)',
         r=recorder, t=ORDER, line=line, period=period, rt=record_type, n=nomenklatura, q=quantity)


def _nomenklatura(db, ref, code):
    _sql(db, 'INSERT INTO "{s}"."Catalog_Nomenklatura" ("Ref_Key", "Code") VALUES (:ref, :code)',
         ref=ref, code=code)


def _document(db, ref, number, when):
    _sql(db, 'INSERT INTO "{s}"."Document_ZakazKlienta" ("Ref_Key", "Number", "Date") '
             'VALUES (:ref, :number, :when)', ref=ref, number=number, when=when)


def _sql(db, sql, **params):
    with db.engine.begin() as conn:
        conn.execute(text(sql.format(s=db.schema)), params)


def _loop(db, handler, loops):
    loop = HandlerLoop(engine=db.engine, schema=db.schema, handler=handler)
    loops.append(loop)
    return loop


def _run(db, loop, *changed_tables):
    """
    Сигнал, как его подаёт репликатор, и один оборот цикла. Упавший обработчик цикл не пробрасывает,
    а записывает в last_error — поэтому проверяем её, иначе тест сверял бы витрину прошлого прогона.
    """
    signals = HandlerSignals(db.engine, db.schema)
    for table in changed_tables:
        signals.signal(table, SOURCE_CHANGES)
    loop.run_if_pending()
    with db.engine.connect() as conn:
        error = conn.execute(text(f'SELECT last_error FROM "{db.schema}".onecdc_handlers '
                                  'WHERE name = :name'), {"name": loop.name}).scalar()
    assert error is None, error


def _mart(clickhouse, table, columns):
    """
    Витрина с FINAL. DateTime без пояса clickhouse-connect отдаёт с поясом сервера, а в 1С время
    настенное — поэтому пояс снимается: сравнивается то, что видно на часах.
    """
    with clickhouse.engine.connect() as conn:
        rows = conn.execute(text(f"SELECT {', '.join(columns)} FROM `{clickhouse.database}`.`{table}` "
                                 f"FINAL ORDER BY ALL"))
        return [tuple(v.replace(tzinfo=None) if isinstance(v, datetime) else v for v in row)
                for row in rows]


# --- построчная витрина -----------------------------------------------------------------------------

def test_row_mart_follows_the_register_and_a_late_catalog(db, clickhouse, loops):
    _source_tables(db)
    order, older_order, nomenklatura = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    _movement(db, order, 1, datetime(2026, 6, 7, 10, 15, 30), 5, nomenklatura)
    _movement(db, order, 2, datetime(2026, 6, 7, 10, 15, 30), 2, nomenklatura, 'Expense')
    _movement(db, older_order, 1, datetime(2026, 5, 1), Decimal('1.125'), nomenklatura)

    loop = _loop(db, ZakazyKlientovClickHouse(clickhouse.engine, clickhouse.source_database,
                                              clickhouse.database), loops)
    columns = ["Recorder", "LineNumber", "Period", "Zakazano", "Artikul", "is_deleted_or_empty"]

    # Первый прогон — пересборка по месяцам. Номенклатура ещё не загружена: артикула нет.
    _run(db, loop)
    assert sorted(_mart(clickhouse, "ZakazyKlientov", columns)) == sorted([
        (order, 1, datetime(2026, 6, 7, 10, 15, 30), Decimal('5'), None, False),
        (order, 2, datetime(2026, 6, 7, 10, 15, 30), Decimal('-2'), None, False),
        (older_order, 1, datetime(2026, 5, 1), Decimal('1.125'), None, False),
    ]), 'Period доезжает тем же временем 1С, без сдвига на пояс сервера ClickHouse'

    # Справочник поступил позже движений: строки пересчитываются по его merged_on.
    _nomenklatura(db, nomenklatura, 'A-1')
    _run(db, loop, "Catalog_Nomenklatura")
    assert {row[4] for row in _mart(clickhouse, "ZakazyKlientov", columns)} == {'A-1'}

    # Строку, выпавшую из набора движений, репликатор помечает: в витрине она обнуляется.
    _sql(db, 'UPDATE "{s}"."AccumulationRegister_ZakazyKlientov" '
             'SET "is_deleted_or_empty" = true, "merged_on" = clock_timestamp() '
             'WHERE "LineNumber" = 2')
    _run(db, loop, "AccumulationRegister_ZakazyKlientov")
    line_2 = [row for row in _mart(clickhouse, "ZakazyKlientov", columns) if row[:2] == (order, 2)]
    assert line_2 == [(order, 2, datetime(2026, 6, 7, 10, 15, 30), Decimal('0'), 'A-1', True)]

    # Все движения регистратора удалены: репликатор пишет запись о пустом наборе, и Period у неё пуст.
    emptied = uuid.uuid4()
    _sql(db, 'INSERT INTO "{s}"."AccumulationRegister_ZakazyKlientov" ("Recorder", "Recorder_Type", '
             '"LineNumber", "is_deleted_or_empty") VALUES (:r, :t, 1, true)', r=emptied, t=ORDER)
    _run(db, loop, "AccumulationRegister_ZakazyKlientov")
    assert [row for row in _mart(clickhouse, "ZakazyKlientov", columns) if row[0] == emptied] == [
        (emptied, 1, None, None, None, True)]

    # merged_on источника доезжает в витрину тем же моментом. Так будет, только пока пояс сессии
    # PostgreSQL совпадает с поясом ClickHouse (см. zakazy_klientov_clickhouse.py).
    with db.engine.connect() as conn:
        expected = conn.execute(text(
            f'SELECT "LineNumber", "merged_on" FROM "{db.schema}"."AccumulationRegister_ZakazyKlientov" '
            'WHERE "Recorder" = :order ORDER BY 1'), {"order": order}).all()
    got = [row[1:] for row in _mart(clickhouse, "ZakazyKlientov", ["Recorder", "LineNumber", "merged_on"])
           if row[0] == order]
    assert got == [(line, moment.astimezone(timezone.utc).replace(tzinfo=None)) for line, moment in expected]


# --- агрегат по группам ----------------------------------------------------------------------------

GROUPED_COLUMNS = ["Number", "Year", "Artikul", "Zakazano", "Period"]


def test_grouped_mart_moves_groups_with_their_document(db, clickhouse, loops):
    _source_tables(db)
    late, early = uuid.uuid4(), uuid.uuid4()
    first, second = uuid.uuid4(), uuid.uuid4()
    _nomenklatura(db, first, 'A-1')
    _nomenklatura(db, second, 'A-2')
    _movement(db, late, 1, datetime(2026, 6, 7), 5, first)
    _movement(db, late, 2, datetime(2026, 6, 7), 2, second)
    _movement(db, early, 1, datetime(2025, 12, 30), 1, first)
    _document(db, early, 'Z-2', datetime(2025, 12, 30, 9, 0))

    loop = _loop(db, ZakazyKlientovGroupedClickHouse(clickhouse.engine, clickhouse.source_database,
                                                         clickhouse.database), loops)

    # Документ late ещё не поступил: его строки в группе-заглушке ('', 0) с месяцем 1900-01.
    _run(db, loop)
    assert _mart(clickhouse, "ZakazyKlientovGrouped", GROUPED_COLUMNS) == [
        ('', 0, 'A-1', Decimal('5'), date(1900, 1, 1)),
        ('', 0, 'A-2', Decimal('2'), date(1900, 1, 1)),
        ('Z-2', 2025, 'A-1', Decimal('1'), date(2025, 12, 1)),
    ]

    # Документ поступил: его строки переходят в свою группу, а заглушка опустела и удаляется.
    # Прежнюю группу обработчик нашёл по Recorder_Keys в ClickHouse — в источнике её уже нет.
    _document(db, late, 'Z-1', datetime(2026, 6, 7, 10, 0))
    _run(db, loop, "Document_ZakazKlienta")
    assert _mart(clickhouse, "ZakazyKlientovGrouped", GROUPED_COLUMNS) == [
        ('Z-1', 2026, 'A-1', Decimal('5'), date(2026, 6, 1)),
        ('Z-1', 2026, 'A-2', Decimal('2'), date(2026, 6, 1)),
        ('Z-2', 2025, 'A-1', Decimal('1'), date(2025, 12, 1)),
    ]

    # Дата документа перешла в другой год: группа (Z-2, 2025) сменилась на (Z-2, 2026).
    _sql(db, 'UPDATE "{s}"."Document_ZakazKlienta" '
             'SET "Date" = :when, "merged_on" = clock_timestamp() WHERE "Ref_Key" = :ref',
         when=datetime(2026, 1, 10), ref=early)
    _run(db, loop, "Document_ZakazKlienta")
    assert ('Z-2', 2026, 'A-1', Decimal('1'), date(2026, 1, 1)) in \
        _mart(clickhouse, "ZakazyKlientovGrouped", GROUPED_COLUMNS)
    assert not [row for row in _mart(clickhouse, "ZakazyKlientovGrouped", GROUPED_COLUMNS)
                if row[:2] == ('Z-2', 2025)]

    # Сменился код номенклатуры: строка с прежним артикулом из группы уходит.
    _sql(db, 'UPDATE "{s}"."Catalog_Nomenklatura" '
             'SET "Code" = \'B-2\', "merged_on" = clock_timestamp() WHERE "Ref_Key" = :ref',
         ref=second)
    _run(db, loop, "Catalog_Nomenklatura")
    assert _mart(clickhouse, "ZakazyKlientovGrouped", GROUPED_COLUMNS) == [
        ('Z-1', 2026, 'A-1', Decimal('5'), date(2026, 6, 1)),
        ('Z-1', 2026, 'B-2', Decimal('2'), date(2026, 6, 1)),
        ('Z-2', 2026, 'A-1', Decimal('1'), date(2026, 1, 1)),
    ]


def test_grouped_rebuild_replaces_whole_months(db, clickhouse, loops):
    """Пересборка заменяет месяц целиком, в том числе месяц, которого в источнике уже нет."""
    _source_tables(db)
    order, nomenklatura = uuid.uuid4(), uuid.uuid4()
    _nomenklatura(db, nomenklatura, 'A-1')
    _movement(db, order, 1, datetime(2026, 6, 7), 5, nomenklatura)
    _document(db, order, 'Z-1', datetime(2026, 6, 7))

    loop = _loop(db, ZakazyKlientovGroupedClickHouse(clickhouse.engine, clickhouse.source_database,
                                                         clickhouse.database), loops)
    _run(db, loop)

    # Лишние строки в витрине: группа месяца, который в источнике есть, и месяц, которого нет.
    with clickhouse.engine.begin() as conn:
        conn.execute(text(
            f"INSERT INTO `{clickhouse.database}`.`ZakazyKlientovGrouped` "
            f"(Number, Year, Artikul, Zakazano, Period, Recorder_Keys, merged_on) VALUES "
            f"('X-9', 2026, 'Q', 1, '2026-06-01', [], now64(6)), "
            f"('X-8', 2020, 'Q', 1, '2020-01-01', [], now64(6))"))

    HandlerSignals(db.engine, db.schema).request_full_rebuild("Document_ZakazKlienta", "test")
    _run(db, loop)
    assert _mart(clickhouse, "ZakazyKlientovGrouped", GROUPED_COLUMNS) == [
        ('Z-1', 2026, 'A-1', Decimal('5'), date(2026, 6, 1))]
