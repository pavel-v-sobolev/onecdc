"""
Шаблон config/runner.py — его копируют и правят, поэтому его отказы становятся отказами у всех.

Главный из них молчаливый: раскомментировать обработчики и забыть про max_workers. Единственный
поток пула навсегда занят репликатором, задания обработчиков лежат в очереди, витрины не
обновляются — и ни одной ошибки в логе. Поэтому числа здесь не пишут руками, а считают от списков,
и проверяется именно это (CDC-34).
"""

import re
from pathlib import Path
from unittest import mock

import pytest

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
RUNNER = CONFIG_DIR / "runner.py"


def _env(monkeypatch, db, **extra):
    # str(url) прячет пароль звёздочками — для подключения нужен полный вид.
    monkeypatch.setenv("ONECDC_DB_URL", db.engine.url.render_as_string(hide_password=False))
    monkeypatch.setenv("ONECDC_ODATA_URL", "http://server/base/odata/standard.odata")
    monkeypatch.setenv("ONECDC_EXCHANGE_NAME", "ДляODATA")
    monkeypatch.setenv("ONECDC_DB_SCHEMA", db.schema)
    for name in ("ONECDC_ODATA_USER", "ONECDC_ODATA_PASSWORD", "ONECDC_QUEUE_GUID",
                 "ONECDC_FULL_LOAD_WORKERS", "ONECDC_POLL_INTERVAL", "ONECDC_LOG_LEVEL"):
        monkeypatch.delenv(name, raising=False)
    for name, value in extra.items():
        monkeypatch.setenv(name, value)


def _uncomment_examples(source: str) -> str:
    """
    Делает то же, что делает пользователь: снимает комментарий с примеров в HANDLERS и CRON_JOBS
    (и с их импорта). Так проверяется сам шаблон, а не его копия в тесте.
    """
    source = source.replace("# from handlers import", "from handlers import")
    for line in ("# ZakazyKlientov(),", "# ZakazyKlientovGrouped(),",
                 '# dict(table_name="Document_ZakazKlienta", cron="0 3 * * *",',
                 '#      date_field="Date", date_from=timedelta(days=3)),',
                 '# dict(table_name="Catalog_Nomenklatura", cron="0 2 * * 0"),',
                 "# from datetime import timedelta"):
        source = source.replace(line, line.replace("# ", "", 1))
    return source


def _run(source: str, monkeypatch):
    """Исполняет шаблон, подменив пул потоков: сам run_forever нам тут не нужен."""
    monkeypatch.syspath_prepend(str(CONFIG_DIR))
    namespace = {"__name__": "__main__", "__file__": str(RUNNER)}
    with mock.patch("concurrent.futures.ThreadPoolExecutor") as pool:
        exec(compile(source, str(RUNNER), "exec"), namespace)
    return namespace, pool


def test_the_template_as_shipped_runs_one_loop(db, monkeypatch):
    _env(monkeypatch, db)

    namespace, pool = _run(RUNNER.read_text(), monkeypatch)

    assert pool.call_args.kwargs["max_workers"] == 1, 'как есть в пуле только репликатор'
    engine_pool = namespace["engine"].pool
    assert engine_pool.size() + engine_pool._max_overflow == 2 + 3, 'воркеры выгрузки + постоянные'
    namespace["replicator"].close()


def test_uncommenting_the_examples_is_enough(db, monkeypatch):
    """
    Сценарий аудита дословно: два обработчика и два расписания включены, про числа не вспомнили.
    Раньше поток оставался один, и витрины молча не считались.
    """
    _env(monkeypatch, db)

    namespace, pool = _run(_uncomment_examples(RUNNER.read_text()), monkeypatch)

    assert len(namespace["HANDLERS"]) == 2 and len(namespace["CRON_JOBS"]) == 2
    assert pool.call_args.kwargs["max_workers"] == 5, 'репликатор + два обработчика + два расписания'
    engine_pool = namespace["engine"].pool
    assert engine_pool.size() + engine_pool._max_overflow == 2 + 3 + 4, \
        'и по соединению на каждый свой поток'
    for runnable in namespace["RUNNABLES"]:
        owner = getattr(runnable, "__self__", None)
        if owner is not None and hasattr(owner, "close"):
            owner.close()
    namespace["replicator"].close()


def test_1c_without_authentication_works_here_too(db, monkeypatch):
    """
    Образ без runner.py в той же ситуации работает анонимно, а шаблон требовал обе переменные и
    падал KeyError: одна и та же 1С оказывалась доступной из одной раскладки и недоступной из
    другой.
    """
    _env(monkeypatch, db)

    namespace, _ = _run(RUNNER.read_text(), monkeypatch)

    assert namespace["replicator"]._odata_auth is None
    namespace["replicator"].close()


def test_the_environment_variables_compose_offers_are_honoured(db, monkeypatch):
    """
    docker-compose.yml предлагает их настраивать, README_ENV.md описывает — значит смонтированный
    runner обязан их читать, иначе это иллюзия настройки.
    """
    _env(monkeypatch, db, ONECDC_FULL_LOAD_WORKERS="4", ONECDC_POLL_INTERVAL="15",
         ONECDC_LOG_LEVEL="WARNING", ONECDC_ODATA_USER="odata", ONECDC_ODATA_PASSWORD="secret")

    namespace, _ = _run(RUNNER.read_text(), monkeypatch)

    import logging
    assert namespace["replicator"]._full_load_workers == 4
    engine_pool = namespace["engine"].pool
    assert engine_pool.size() + engine_pool._max_overflow == 4 + 3
    assert namespace["RUNNABLES"][0].keywords == {"interval": 15.0}
    assert logging.getLogger("onecdc").level == logging.WARNING
    assert namespace["replicator"]._odata_auth is not None
    namespace["replicator"].close()
    logging.getLogger("onecdc").setLevel(logging.INFO)


@pytest.mark.parametrize("name", ["ONECDC_DB_URL", "ONECDC_ODATA_URL", "ONECDC_EXCHANGE_NAME"])
def test_a_missing_required_variable_is_named(db, monkeypatch, name):
    # Обязательные — через os.environ[...] намеренно: KeyError называет переменную, и это
    # понятнее, чем падение где-то дальше с пустым адресом.
    _env(monkeypatch, db)
    monkeypatch.delenv(name)

    with pytest.raises(KeyError, match=name):
        _run(RUNNER.read_text(), monkeypatch)


def test_the_template_does_not_pretend_to_read_the_mode():
    # ONECDC_MODE действует только на раскладку без runner.py. Шаблон его не читает — и об этом
    # прямо сказано, иначе настройка выглядела бы действующей.
    source = RUNNER.read_text()
    assert not re.search(r'environ.*ONECDC_MODE', source)
    assert "ONECDC_MODE этот файл НЕ читает" in source


def test_the_connection_ceiling_is_set_by_us_and_not_inherited(db, monkeypatch):
    """
    Потолок — это СУММА pool_size и max_overflow, и по умолчанию он на десять больше, чем
    кажется: pool_size=5 на деле удерживает 15 (измерено). Три контейнера дают 45 соединений
    против лимита сервера, и ни одной ошибки до самого упора (CDC-40).

    Деление между частями тоже не случайно: постоянно держим только то, что работает непрерывно,
    остальное берём через overflow — такие соединения закрываются сразу, как отработали.
    """
    _env(monkeypatch, db)

    namespace, _ = _run(RUNNER.read_text(), monkeypatch)

    pool = namespace["engine"].pool
    assert pool.size() == 3, 'постоянная часть — цикл изменений и запас на его запросы'
    assert pool.size() + pool._max_overflow == 2 + 3, 'потолок: воркеры выгрузки сверх постоянных'
    assert pool._pre_ping is True, 'соединение после простоя уже закрыто сервером'
    namespace["replicator"].close()
