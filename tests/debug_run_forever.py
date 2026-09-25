"""
Ручной прогон Replicator.run_forever против живой 1С и dev-Postgres (стенд берётся из
debug_trade.py). notify включён — изменения ПОДТВЕРЖДАЮТСЯ, то есть очередь обмена в 1С
продвигается и повторить тот же пакет уже нельзя.

Имя без префикса test_ и рядом с остальными debug_* намеренно: тестовых функций здесь нет и
никогда не было (цикл бесконечный), а файл, названный тестом, читается как то, что может
запуститься само — при включённом подтверждении это опасная иллюзия.

Запуск только вручную:

    uv run python tests/debug_run_forever.py

Кидай изменения из 1С — раз в POLL_INTERVAL они вычитываются, сохраняются в Postgres и
подтверждаются. Остановка — Ctrl+C (graceful: дорабатывает текущий цикл и выходит).
"""

import logging

from sqlalchemy import create_engine

from onecdc import Replicator

# Тестовый/dev-контур, не боевой (debug_trade.py лежит рядом и импортируется как обычный модуль).
from debug_trade import (CONTOUR, DB_SCHEMA, DB_URL, EXCHANGE_NAME, ODATA_AUTH,
                         ODATA_URL, QUEUE_GUID)

POLL_INTERVAL = 60.0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    if not CONTOUR.is_configured:
        raise SystemExit(CONTOUR.why_not)

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
