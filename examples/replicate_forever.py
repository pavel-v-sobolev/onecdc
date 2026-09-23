"""
Непрерывная репликация изменений из 1С в целевую БД — самый простой запуск, какой бывает.

Ни докера, ни пула потоков: один процесс, один цикл. Он опрашивает план обмена, забирает пакет
изменений, пишет его в БД и подтверждает приём. Объект, впервые появившийся в пакете, встаёт на
полную выгрузку сам — так в БД попадает не только то, что менялось после подключения.

Когда понадобится больше — витрины своими циклами, полные выгрузки по расписанию, несколько планов
обмена в одном процессе, — смотрите config/runner.py: это тот же запуск, но с пулом потоков.

Останавливается по Ctrl+C или SIGTERM: текущая итерация дорабатывается, незавершённые merge не
остаются висеть.

Запуск:
    ONECDC_ODATA_URL=http://server/base/odata/standard.odata \
    ONECDC_ODATA_USER=odata ONECDC_ODATA_PASSWORD=secret \
    ONECDC_EXCHANGE_NAME=ДляODATA \
    ONECDC_QUEUE_GUID=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa \
    ONECDC_DB_URL=postgresql+psycopg2://user:pass@localhost:5432/dwh \
    python examples/replicate_forever.py

GUID узла обмена можно не знать: оставьте переменную пустой — в лог выведется список узлов плана,
и оттуда его видно.
"""

import logging
import os

from sqlalchemy import create_engine

from onecdc import Replicator

logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s: %(message)s')

# Соединения одновременно держат цикл изменений, страницы фоновых полных выгрузок (их два потока)
# и две отметки живости — отсюда пять.
engine = create_engine(os.environ['ONECDC_DB_URL'], pool_size=5)

user = os.environ.get('ONECDC_ODATA_USER')
rep = Replicator(
    odata_url=os.environ['ONECDC_ODATA_URL'],
    # Нет пользователя — 1С опубликована без авторизации.
    odata_auth=(user, os.environ.get('ONECDC_ODATA_PASSWORD', '')) if user else None,
    engine=engine,
    exchange_name=os.environ['ONECDC_EXCHANGE_NAME'],
    queue_guid=os.environ.get('ONECDC_QUEUE_GUID', ''),
    db_schema=os.environ.get('ONECDC_DB_SCHEMA'),
)

try:
    # Блокирующий цикл: опрос раз в минуту, пока процесс не попросят остановиться.
    rep.run_forever(interval=60)
finally:
    rep.close()
