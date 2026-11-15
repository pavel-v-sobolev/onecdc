"""Отдельный процесс beta-витрин; python config/runner_marts.py.

Репликатор должен писать с Replicator(tracker=tracker), используя тот же журнал.
Tracker принадлежит приложению: закрывайте его после остановки репликатора.
Служебная схема журнала и бизнес-схемы источника/витрины задаются независимо.
"""

import signal
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine

from onecdc.marts import request_stop, run_mart

from handlers import OrdersByCustomer

DB_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/onecdc"
POLL_INTERVAL = 60
DB_SCHEMA = None  # служебная схема; должна существовать, как и бизнес-схемы
SOURCE_SCHEMA = None
MART_SCHEMA = None


def main():
    engine = create_engine(DB_URL, pool_pre_ping=True, pool_size=5, max_overflow=0)
    try:
        marts = [OrdersByCustomer(engine,
                                 destination=(MART_SCHEMA, "orders_by_customer"),
                                 sources=[(SOURCE_SCHEMA, "orders")])]
        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)
        with ThreadPoolExecutor(max_workers=len(marts), thread_name_prefix='mart') as executor:
            futures = [executor.submit(run_mart, mart, poll_interval=POLL_INTERVAL,
                                       schema=DB_SCHEMA) for mart in marts]
            try:
                for future in as_completed(futures):
                    future.result()
            finally:
                # Ошибка подготовки одного worker не оставляет остальные работать вечно.
                request_stop()
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()