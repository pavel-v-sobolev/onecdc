"""
Непрерывная репликация изменений из 1С в PostgreSQL.

Пример запускает один Replicator без Docker и собственного пула потоков. Его основной цикл
читает пакет изменений, сохраняет данные и подтверждает приём. Новые объекты автоматически
ставятся на полную выгрузку, которую выполняют внутренние фоновые потоки.

Для обработчиков витрин, расписаний или нескольких планов обмена см. config/runner.py.
По Ctrl+C или SIGTERM библиотека запрашивает остановку: цикл изменений завершает текущую
итерацию, а фоновые выгрузки останавливаются на границах страниц.

Запуск:
    ONECDC_ODATA_URL=http://server/base/odata/standard.odata \\
    ONECDC_ODATA_USER=odata ONECDC_ODATA_PASSWORD=secret \\
    ONECDC_EXCHANGE_NAME=ДляODATA \\
    ONECDC_QUEUE_GUID=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa \\
    ONECDC_DB_URL=postgresql+psycopg2://user:pass@localhost:5432/dwh \\
    python examples/replicate_forever.py

Если GUID узла неизвестен, оставьте ONECDC_QUEUE_GUID пустой. Цикл выведет список узлов
в журнал и завершится с ошибкой; остановите процесс, задайте GUID и запустите снова.
"""

import logging
import os

from sqlalchemy import create_engine

from onecdc import Replicator

logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s: %(message)s')

# Основной пул допускает до пяти соединений: три сохраняются в пуле,
# ещё два могут открываться при нагрузке. Отметки живости используют отдельный пул.
engine = create_engine(os.environ['ONECDC_DB_URL'],
                       pool_size=3, max_overflow=2, pool_pre_ping=True)

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
    # Блокирующий цикл с минутной паузой после каждой успешной итерации.
    rep.run_forever(interval=60)
finally:
    rep.close()
