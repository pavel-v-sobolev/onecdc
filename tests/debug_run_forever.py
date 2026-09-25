"""
Ручной прогон Replicator.run_forever против живой 1С и dev-Postgres (параметры берутся из
debug_trade.py). notify включён по умолчанию — изменения подтверждаются после успешного сохранения.

Тестовых функций здесь нет: цикл бесконечный, и pytest, собрав файл, ничего в нём не найдёт и не
запустит. Запуск только вручную:

    uv run python tests/test_run_forever_live.py

Кидай изменения из 1С — раз в POLL_INTERVAL они вычитываются, сохраняются в Postgres и
подтверждаются. Остановка — Ctrl+C (graceful: дорабатывает текущий цикл и выходит).
"""

import logging

from sqlalchemy import create_engine

from onecdc import Replicator

# Тестовый/dev-контур, не боевой (debug_trade.py лежит рядом и импортируется как обычный модуль).
from debug_trade import DB_SCHEMA, DB_URL, EXCHANGE_NAME, ODATA_AUTH, ODATA_URL, QUEUE_GUID

POLL_INTERVAL = 60.0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    repl = Replicator(
        odata_url=ODATA_URL,
        odata_auth=ODATA_AUTH,
        exchange_name=EXCHANGE_NAME,
        queue_guid=QUEUE_GUID,
        engine=create_engine(DB_URL),
        db_schema=DB_SCHEMA,
    )
    # notify включён (run_once(notify_changes=True) по умолчанию) — изменения подтверждаются.
    repl.run_forever(interval=POLL_INTERVAL)
