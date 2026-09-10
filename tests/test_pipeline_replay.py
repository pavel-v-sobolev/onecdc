"""
Оффлайн replay-тест полного пайплайна Replicator.

Поднимает фейковый сервер 1С (tests/fake_1c.py), который проигрывает записанные ответы, и гоняет
против него реальный Replicator с локальным PostgreSQL (см. conftest.py). Живая 1С не нужна.

Параметризуется по подпапкам tests/responses/* : добавление новой конфигурации (версия/конфигурация
1С) автоматически добавляет тест-кейсы.
"""

from pathlib import Path

import pytest
from sqlalchemy import inspect, select, text

import fake_1c  # соседний модуль в tests/ (pytest добавляет каталог теста в sys.path)
from onecdc import Replicator

RESPONSES_DIR = Path(__file__).parent / "responses"
CONFIGS = sorted(p for p in RESPONSES_DIR.iterdir() if (p / "manifest.json").exists())


@pytest.fixture
def fake_server(request):
    """Поднимает фейковый сервер для конфигурации request.param, отдаёт (odata_url, fake)."""
    with fake_1c.running_server(request.param) as (odata_url, fake):
        yield odata_url, fake


def _make_replicator(odata_url, queue_guid, db, **overrides):
    kwargs = dict(
        odata_url=odata_url,
        odata_auth=None,                  # фейковый сервер не проверяет auth
        exchange_name="ДляODATA",   # сервер матчит ExchangePlan по пути, имя не важно
        queue_guid=queue_guid,
        engine=db.engine,
        db_schema=db.schema,
    )
    kwargs.update(overrides)
    return Replicator(**kwargs)


def _row_count(db) -> int:
    total = 0
    with db.engine.connect() as conn:
        for table in inspect(db.engine).get_table_names(schema=db.schema):
            total += conn.execute(text(f'SELECT COUNT(*) FROM "{db.schema}"."{table}"')).scalar()
    return total


def _full_load_required(repl) -> list[str]:
    """Объекты, помеченные в реестре как требующие полной выгрузки."""
    table = repl.metadata.objects_table
    with repl.engine.connect() as conn:
        rows = conn.execute(select(table.c.object_full_name)
                            .where(table.c.full_load_is_required.is_(True)))
        return [row.object_full_name for row in rows]


@pytest.mark.parametrize("fake_server", CONFIGS, ids=[p.name for p in CONFIGS], indirect=True)
def test_run_once_replay(fake_server, db):
    odata_url, fake = fake_server
    repl = _make_replicator(odata_url, fake.queue_guid, db)

    repl.run_once()

    # Метаданные прочитаны, первый пакет сохранён в БД.
    assert len(repl.metadata) > 0
    assert inspect(repl.engine).get_table_names(schema=db.schema)
    assert _row_count(db) >= 1
    # notify прошёл → состояние очереди продвинулось на первый пакет.
    assert fake.received_no == 1


@pytest.mark.parametrize("fake_server", CONFIGS, ids=[p.name for p in CONFIGS], indirect=True)
def test_run_forever_replay(fake_server, db):
    odata_url, fake = fake_server
    n_batches = len(fake.batches)
    repl = _make_replicator(odata_url, fake.queue_guid, db)

    # interval=0 — без пауз; max_iterations=n_batches — обработать все записанные пакеты.
    repl.run_forever(interval=0, max_iterations=n_batches)

    # Все пакеты подтверждены по очереди → счётчик дошёл до последнего MessageNo.
    assert fake.received_no == n_batches
    assert _row_count(db) >= n_batches


@pytest.mark.parametrize("fake_server", CONFIGS, ids=[p.name for p in CONFIGS], indirect=True)
def test_automatic_full_load_flags_new_objects(fake_server, db):
    """По умолчанию объект, впервые встреченный в пакете, встаёт в очередь на полную выгрузку."""
    odata_url, fake = fake_server
    repl = _make_replicator(odata_url, fake.queue_guid, db)

    repl.run_once()

    assert _full_load_required(repl)


@pytest.mark.parametrize("fake_server", CONFIGS, ids=[p.name for p in CONFIGS], indirect=True)
def test_automatic_full_load_off_orders_nothing(fake_server, db):
    """automatic_full_load=False — изменения читаются и сохраняются, но выгрузку никто не заказывает."""
    odata_url, fake = fake_server
    repl = _make_replicator(odata_url, fake.queue_guid, db, automatic_full_load=False)

    repl.run_once()

    assert _row_count(db) >= 1, 'изменения сохраняются как обычно'
    assert _full_load_required(repl) == []
