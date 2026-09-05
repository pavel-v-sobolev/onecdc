"""
Защита от дурака в параметрах: конструктор Replicator и подсказка со списком узлов обмена.

Проверяем, что неверный параметр падает СРАЗУ и с внятным текстом, а не оборачивается ошибкой 1С
где-то в середине первого цикла, и что незаданный узел обмена печатает в лог, из чего выбирать.
"""

import logging

import pytest
from pathlib import Path

import fake_1c  # соседний модуль в tests/ (pytest добавляет каталог теста в sys.path)
from onecdc import Replicator

GUID = "12345678-1234-1234-1234-123456789abc"


def _make(db, **overrides):
    kwargs = dict(odata_url="http://host/base/odata/standard.odata", odata_auth=None,
                  exchange_name="ДляODATA", queue_guid=GUID,
                  engine=db.engine, db_schema=db.schema)
    kwargs.update(overrides)
    return Replicator(**kwargs)


@pytest.mark.parametrize("overrides, expected", [
    ({"odata_url": ""}, "odata_url is required"),
    ({"odata_url": "host/base/odata/standard.odata"}, "http://"),
    ({"odata_auth": "user:password"}, "odata_auth"),
    ({"odata_auth": ("user",)}, "odata_auth"),
    ({"exchange_name": " "}, "exchange_name is required"),
    ({"queue_guid": "ДляВитрины"}, "Ref_Key"),
    ({"engine": "postgresql://localhost/db"}, "create_engine"),
    ({"db_schema": 5}, "db_schema"),
    ({"full_load_workers": 0}, "full_load_workers"),
    ({"request_timeout": 0}, "request_timeout"),
    ({"request_timeout": (60, None)}, "request_timeout"),
])
def test_bad_parameter_rejected(db, overrides, expected):
    with pytest.raises(ValueError, match=expected):
        _make(db, **overrides)


def test_parameters_normalized(db):
    repl = _make(db, odata_url="http://host/base/odata/standard.odata/",
                 exchange_name="ExchangePlan_ДляODATA", queue_guid="{" + GUID.upper() + "}",
                 db_schema="  ")
    assert repl._odata_url == "http://host/base/odata/standard.odata"
    assert repl._exchange_name == "ДляODATA"
    assert repl._queue_guid == GUID.upper()
    assert repl.db_schema is None


def test_empty_queue_guid_logs_available_nodes(db, caplog):
    """Узел не задан — в логе список узлов плана обмена, кроме ЭтотУзел."""
    config = next(p for p in (Path(__file__).parent / "responses").iterdir()
                  if (p / "manifest.json").exists())
    with fake_1c.running_server(config) as (odata_url, fake):
        repl = _make(db, odata_url=odata_url, queue_guid="")
        with caplog.at_level(logging.ERROR, logger="onecdc"):
            with pytest.raises(ValueError, match="queue_guid is not set"):
                repl.changes.read_changes()

    listing = "\n".join(record.getMessage() for record in caplog.records)
    assert fake.queue_guid in listing
    assert fake_1c.THIS_NODE_GUID not in listing


def test_unreachable_db_reports_plainly(db):
    """
    Недоступная БД должна давать одну понятную строку, а не сто с лишним строк трейса сквозь пул
    SQLAlchemy и psycopg2 (в контейнере это единственное, что видит запускающий). Проверяем и то,
    что пароль в сообщении не светится: адрес БД в лог попадает.
    """
    from sqlalchemy import create_engine

    # Порт 1 на локальном интерфейсе: отказ приходит сразу, без ожидания DNS или таймаута.
    engine = create_engine("postgresql+psycopg2://postgres:sekret@127.0.0.1:1/nowhere")

    with pytest.raises(ConnectionError) as err:
        _make(db, engine=engine)

    message = str(err.value)
    assert "cannot connect to the database" in message
    assert "127.0.0.1:1" in message
    assert "sekret" not in message
    # Причина от драйвера — ради неё всё и затевалось.
    assert "onnection refused" in message or "не удалось" in message.lower()
