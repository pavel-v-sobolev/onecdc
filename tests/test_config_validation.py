"""
Защита от дурака в параметрах: конструктор Replicator и подсказка со списком узлов обмена.

Проверяем, что неверный параметр падает СРАЗУ и с внятным текстом, а не оборачивается ошибкой 1С
где-то в середине первого цикла, и что незаданный узел обмена печатает в лог, из чего выбирать.
"""

import logging

import pytest
from pathlib import Path

from sqlalchemy import text

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
    ({"queue_guid": "ДляВитрины"}, "Ref_Key"),
    ({"engine": "postgresql://localhost/db"}, "create_engine"),
    ({"db_schema": 5}, "db_schema"),
    ({"full_load_workers": 0}, "full_load_workers"),
    ({"automatic_full_load": "false"}, "automatic_full_load"),
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
    # Канонический вид: скобки сняты И регистр приведён — с 1С сравнивается именно он.
    assert repl._queue_guid == GUID.lower()
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


# --- CDC-32: guid узла сравнивается канонически, а не найденный узел — ошибка ------------------

def _config_dir() -> Path:
    return next(p for p in (Path(__file__).parent / "responses").iterdir()
                if (p / "manifest.json").exists())


@pytest.mark.parametrize("in_config, from_1c", [
    (str.upper, str.lower),   # guid скопирован из формы 1С — канонизируем ВХОД
    (str.lower, str.upper),   # Ref_Key пришёл не в каноническом виде — канонизируем СРАВНЕНИЕ
    (str.upper, str.upper),
])
def test_the_node_is_found_whatever_case_the_guid_was_copied_in(db, in_config, from_1c):
    """
    1С отдаёт Ref_Key в нижнем регистре, а из формы 1С его копируют в верхнем. Раньше сравнение
    было строковым: узел «не находился», и цикл КАЖДЫЙ раз просил MessageNo=1 — с предупреждением,
    которое в рабочем логе никто не читает.

    Обе половины проверяются по отдельности: регистр меняем то в конфигурации, то в ответе 1С.
    """
    with fake_1c.running_server(_config_dir()) as (odata_url, fake):
        guid = fake.queue_guid
        fake.queue_guid = from_1c(guid)      # как узел выглядит в ответе сервера
        fake.notify(7)                       # очередь уже подтверждена до седьмого пакета
        repl = _make(db, odata_url=odata_url, queue_guid="{" + in_config(guid) + "}")

        assert repl.changes.get_last_received_no() == 7
        repl.close()


def test_a_guid_that_is_not_a_node_of_this_plan_stops_the_cycle(db, caplog):
    """
    Раньше здесь были WARNING и 0, то есть «продолжим с первого пакета». Молчаливым простоем это
    не кончается: SelectChanges со старым номером 1С понимает как «отдай всё, что зарегистрировано
    с тех пор», а подтверждение снимает регистрацию — обмен идёт, счётчики стоят на единице, и
    ошибки конфигурации не видно вовсе.
    """
    with fake_1c.running_server(_config_dir()) as (odata_url, fake):
        repl = _make(db, odata_url=odata_url, queue_guid="12345678-1234-1234-1234-123456789abc")

        with caplog.at_level(logging.ERROR, logger="onecdc"):
            with pytest.raises(ValueError, match="not a node of this exchange plan"):
                repl.changes.get_last_received_no()
        repl.close()

    # В логе — из чего выбирать, как и для незаданного узла.
    listing = "\n".join(record.getMessage() for record in caplog.records)
    assert fake.queue_guid in listing
    assert fake_1c.THIS_NODE_GUID not in listing


def test_this_node_is_refused_instead_of_being_read_from(db, caplog):
    """
    ЭтотУзел описывает саму базу-источник. В списке узлов он есть всегда, поэтому НАХОДИЛСЯ
    наравне с остальными: номер брался, SelectChanges уходил по нему, и что ответит 1С, зависело
    уже от платформы. Из подсказки-списка он исключён давно, из принимаемых значений — теперь.
    """
    with fake_1c.running_server(_config_dir()) as (odata_url, fake):
        repl = _make(db, odata_url=odata_url, queue_guid=fake_1c.THIS_NODE_GUID)

        with caplog.at_level(logging.ERROR, logger="onecdc"):
            with pytest.raises(ValueError, match="ThisNode"):
                repl.changes.get_last_received_no()
        repl.close()


# --- репликатор только под полные выгрузки -------------------------------------------------------

def test_a_replicator_without_an_exchange_plan_is_allowed(db):
    """
    Полная выгрузка читает объект прямо из OData и план обмена не использует вовсе. Требовать его
    в конструкторе значило бы заставлять выдумывать план ради разовой загрузки — а выдуманный
    оседал в журнале и в onecdc_exchange_nodes.
    """
    repl = _make(db, exchange_name=None, queue_guid="")

    assert repl._exchange_name == ''
    with repl.engine.connect() as conn:
        nodes = conn.execute(text(f'SELECT count(*) FROM "{db.schema}".onecdc_exchange_nodes'))
        assert nodes.scalar() == 0, 'строка узла без узла — мусор'
    repl.close()


@pytest.mark.parametrize("call", [lambda r: r.run_once(), lambda r: r.run_forever()])
def test_reading_changes_without_a_plan_refuses_to_start(db, call):
    """Отказ здесь, а не в конструкторе: молча крутить цикл, которому нечего читать, нельзя."""
    repl = _make(db, exchange_name=None, queue_guid="")

    with pytest.raises(ValueError, match="only run full loads"):
        call(repl)
    repl.close()
