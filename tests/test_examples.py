"""
Примеры из examples/ — то, что копируют первым делом, поэтому их отказы становятся отказами у всех.

Сеть здесь не нужна: сами циклы и выгрузка подменяются, проверяется сборка — что пример собирает
репликатор теми параметрами, которые обещает, и что обещания эти не разъехались с библиотекой.
Тот же приём, что и для шаблона config/runner.py (см. test_runner_template).
"""

from pathlib import Path
from unittest import mock

import pytest

from onecdc import Replicator

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def _env(monkeypatch, db, **extra):
    monkeypatch.setenv("ONECDC_DB_URL", db.engine.url.render_as_string(hide_password=False))
    monkeypatch.setenv("ONECDC_ODATA_URL", "http://server/base/odata/standard.odata")
    monkeypatch.setenv("ONECDC_DB_SCHEMA", db.schema)
    for name in ("ONECDC_ODATA_USER", "ONECDC_ODATA_PASSWORD", "ONECDC_QUEUE_GUID",
                 "ONECDC_EXCHANGE_NAME"):
        monkeypatch.delenv(name, raising=False)
    for name, value in extra.items():
        monkeypatch.setenv(name, value)


def _run(name: str, monkeypatch, argv=()):
    path = EXAMPLES / name
    monkeypatch.setattr("sys.argv", [str(path), *argv])
    namespace = {"__name__": "__main__", "__file__": str(path)}
    exec(compile(path.read_text(), str(path), "exec"), namespace)
    return namespace


def test_the_one_off_full_load_needs_no_exchange_plan(db, monkeypatch):
    """
    Полная выгрузка читает объект прямо из OData, и плана обмена у такой установки может не быть
    вовсе. Раньше его приходилось выдумывать — и выдуманный оседал в журнале и в узлах обмена.
    """
    _env(monkeypatch, db)
    loaded = []

    with mock.patch.object(Replicator, "full_load",
                           lambda self, name, **kw: loaded.append((self, name)) or 0):
        namespace = _run("full_load_once.py", monkeypatch, argv=["Catalog_Контрагенты"])

    rep = namespace["rep"]
    assert loaded == [(rep, "Catalog_Контрагенты")]
    assert rep._exchange_name == '', 'пример не должен требовать план обмена'
    with db.engine.connect() as conn:
        from sqlalchemy import text
        nodes = conn.execute(text(f'SELECT count(*) FROM "{db.schema}".onecdc_exchange_nodes'))
        assert nodes.scalar() == 0, 'узел не задан — строки быть не должно'


def test_the_forever_example_starts_the_changes_loop(db, monkeypatch):
    """Второй пример — ровно наоборот: это и есть чтение изменений, план обмена ему обязателен."""
    _env(monkeypatch, db, ONECDC_EXCHANGE_NAME="ДляODATA",
         ONECDC_QUEUE_GUID="a9bc23c5-3689-11f1-926c-0800270bc6cb")
    started = []

    with mock.patch.object(Replicator, "run_forever",
                           lambda self, **kw: started.append(kw)):
        namespace = _run("replicate_forever.py", monkeypatch)

    assert started == [{"interval": 60}]
    assert namespace["rep"]._exchange_name == "ДляODATA"


def test_anonymous_1c_works_in_the_forever_example(db, monkeypatch):
    # Пользователь не задан — публикация без авторизации, как и в образе.
    _env(monkeypatch, db, ONECDC_EXCHANGE_NAME="ДляODATA")

    with mock.patch.object(Replicator, "run_forever", lambda self, **kw: None):
        namespace = _run("replicate_forever.py", monkeypatch)

    assert namespace["rep"]._odata_auth is None


@pytest.mark.parametrize("name", ["full_load_once.py", "replicate_forever.py"])
def test_examples_do_not_mention_the_temp_schema(name):
    """
    Промежуточную таблицу merge dbmerge заводит настоящей TEMPORARY, и схема для неё в примерах
    только сбивает с толку: настраивать там нечего.
    """
    assert "temp_schema" not in (EXAMPLES / name).read_text()
