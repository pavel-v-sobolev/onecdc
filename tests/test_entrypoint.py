"""
Оффлайн-тесты entrypoint (python -m onecdc): выбор режима ONECDC_MODE и параметров из окружения.
Конструктор Replicator подменяется — сеть/БД не задействуются.
"""

import pytest

from sqlalchemy.engine import make_url

import onecdc.__main__ as entry
from conftest import TEST_DB_URL


class _FakeReplicator:
    def __init__(self):
        self.calls = []

    def run_once(self, *args, **kwargs):
        self.calls.append(("once", args, kwargs))

    def run_forever(self, *args, **kwargs):
        self.calls.append(("forever", args, kwargs))


def _set_env(monkeypatch, **extra):
    env = {"ONECDC_ODATA_URL": "http://x", "ONECDC_EXCHANGE_NAME": "E",
           "ONECDC_QUEUE_GUID": "Q", "ONECDC_DB_URL": TEST_DB_URL}
    env.update(extra)
    for k, v in env.items():
        monkeypatch.setenv(k, v)


def _patch_replicator(monkeypatch, replicator):
    """Подменяем сам класс: entrypoint собирает оркестратор явным присвоением аргументов, и
    перехватывать надо конструктор, а не фабрику. Аргументы запоминаем — разбор окружения живёт
    теперь в самом entrypoint, и проверять его больше негде."""
    captured = {}

    def fake_constructor(**kwargs):
        captured.update(kwargs)
        return replicator

    monkeypatch.setattr(entry, "Replicator", fake_constructor)
    return captured


def test_main_maps_environment_to_arguments(monkeypatch):
    _set_env(monkeypatch, ONECDC_MODE="once", ONECDC_ODATA_USER="odata",
             ONECDC_ODATA_PASSWORD="secret", ONECDC_DB_SCHEMA="onecdc",
             ONECDC_FULL_LOAD_WORKERS="4")
    captured = _patch_replicator(monkeypatch, _FakeReplicator())

    entry.main()

    assert captured["odata_url"] == "http://x"
    assert captured["odata_auth"] == ("odata", "secret")
    assert captured["exchange_name"] == "E"
    assert captured["queue_guid"] == "Q"
    assert captured["db_schema"] == "onecdc"
    assert captured["full_load_workers"] == 4
    assert captured["engine"].url.database == make_url(TEST_DB_URL).database


def test_main_without_user_means_no_auth(monkeypatch):
    # Пользователь не задан — авторизации нет; пустой кортеж ридерам не подсунуть.
    _set_env(monkeypatch, ONECDC_MODE="once")
    captured = _patch_replicator(monkeypatch, _FakeReplicator())

    entry.main()

    assert captured["odata_auth"] is None
    assert captured["db_schema"] is None
    assert captured["full_load_workers"] == 2, 'значение по умолчанию'


def test_main_loop_mode(monkeypatch):
    _set_env(monkeypatch, ONECDC_MODE="loop", ONECDC_POLL_INTERVAL="5")
    rep = _FakeReplicator()
    _patch_replicator(monkeypatch, rep)

    entry.main()
    assert rep.calls == [("forever", (), {"interval": 5.0})]


def test_main_loop_is_default(monkeypatch):
    _set_env(monkeypatch)   # без ONECDC_MODE → loop, период по умолчанию 60
    rep = _FakeReplicator()
    _patch_replicator(monkeypatch, rep)

    entry.main()
    assert rep.calls == [("forever", (), {"interval": 60.0})]


def test_main_once_mode(monkeypatch):
    _set_env(monkeypatch, ONECDC_MODE="once")
    rep = _FakeReplicator()
    _patch_replicator(monkeypatch, rep)

    entry.main()
    assert rep.calls == [("once", (), {})]


def test_main_unknown_mode(monkeypatch):
    _set_env(monkeypatch, ONECDC_MODE="bogus")
    _patch_replicator(monkeypatch, _FakeReplicator())

    with pytest.raises(SystemExit):
        entry.main()
