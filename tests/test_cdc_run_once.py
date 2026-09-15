"""
Живой smoke-тест Replicator против реального сервера 1С (тестовая база торговли) и
dev-Postgres. Параметры подключения берутся из debug_trade.py — там они и правятся, чтобы контуры не
разъезжались (раньше здесь стояла своя копия, и адрес 1С успел разойтись с отладочным).

Гоняет полный цикл Replicator.run_once(notify_changes=False): read → save БЕЗ notify, чтобы
не списывать изменения из очереди обмена 1С и оставить прогон повторяемым. Проверяет, что
метаданные читаются с живой 1С и изменения сохраняются в БД без ошибок.

Требует доступной 1С и Postgres (помечен маркером integration — отдельной обработки оффлайна нет,
без доступа тест упадёт). Запуск:
  - через pytest: `uv run pytest tests/test_cdc_run_once.py`;
  - напрямую, без pytest: `uv run python tests/test_cdc_run_once.py`.
"""

import pytest
from sqlalchemy import create_engine, inspect

from onecdc import Replicator

# Тестовый/dev-контур, не боевой (debug_trade.py лежит рядом и импортируется как обычный модуль).
from debug_trade import DB_SCHEMA, DB_URL, EXCHANGE_NAME, ODATA_AUTH, ODATA_URL, QUEUE_GUID


@pytest.mark.integration
def test_run_once_against_live_1c():

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
        # пропустил бы цикл (узел числился бы за завершившимся процессом) и прошёл бы ВХОЛОСТУЮ,
        # не проверив ничего, — а выглядел бы успешным.
        repl.close()


if __name__ == "__main__":
    # Обычный запуск без pytest: `uv run python tests/test_cdc_run_once.py`.
    import logging

    logging.basicConfig(level=logging.INFO)
    test_run_once_against_live_1c()
    print("OK: цикл read → save отработал (notify отключён, изменения остались в очереди).")
