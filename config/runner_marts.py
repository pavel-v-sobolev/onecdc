"""Отдельный процесс beta-витрин; python config/runner_marts.py.

Репликатор должен писать с Replicator(tracker=tracker), используя тот же журнал.
Tracker принадлежит приложению: закрывайте его после остановки репликатора.
Служебная схема журнала и бизнес-схемы источника/витрины задаются независимо.
"""

import signal
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine

from dbmerge import MergeTracker, request_stop

from handlers.orders_by_customer import OrdersByCustomer
# from handlers import ZakazyKlientov, ZakazyKlientovGrouped
# from handlers import ZakazyKlientovClickHouse, ZakazyKlientovGroupedClickHouse

DB_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/onecdc"
POLL_INTERVAL = 60
DB_SCHEMA = None  # служебная схема; должна существовать, как и бизнес-схемы
SOURCE_SCHEMA = None
MART_SCHEMA = None


def main():
    engine = create_engine(DB_URL, pool_pre_ping=True, pool_size=5, max_overflow=0)
    try:
        with MergeTracker(engine, schema=DB_SCHEMA) as tracker:
            marts = [OrdersByCustomer(engine, tracker=tracker,
                                      destination=(MART_SCHEMA, "orders_by_customer"),
                                      sources=[(SOURCE_SCHEMA, "orders")])]
            # Вместо OrdersByCustomer можно запустить примеры для таблиц 1С:
            # marts = [ZakazyKlientov(engine, tracker=tracker, schema=SOURCE_SCHEMA),
            #          ZakazyKlientovGrouped(engine, tracker=tracker, schema=SOURCE_SCHEMA)]
            # ClickHouse engine создаёт приложение и dispose() после остановки workers:
            # ZakazyKlientovClickHouse(engine, clickhouse, 'onecdc_pg',
            #                          tracker=tracker, schema=SOURCE_SCHEMA)
            # ZakazyKlientovGroupedClickHouse(engine, clickhouse, 'onecdc_pg',
            #                                 tracker=tracker, schema=SOURCE_SCHEMA)
            signal.signal(signal.SIGTERM, request_stop)
            signal.signal(signal.SIGINT, request_stop)
            with ThreadPoolExecutor(max_workers=len(marts), thread_name_prefix='mart') as executor:
                try:
                    futures = [executor.submit(mart.run_forever, poll_interval=POLL_INTERVAL)
                               for mart in marts]
                    for future in as_completed(futures):
                        future.result()
                finally:
                    # Остановить workers до ожидания executor и закрытия tracker.
                    request_stop()
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()