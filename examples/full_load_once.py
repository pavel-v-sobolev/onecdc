"""
Разовая полная выгрузка одного объекта 1С в целевую БД.

Когда это нужно: первая загрузка справочника или документа, добор данных за прошлые периоды,
пересборка таблицы после того, как в 1С добавили реквизит. Поток изменений для этого запускать
не обязательно — полная выгрузка самостоятельна.

Запуск:
    ONECDC_ODATA_URL=... ONECDC_ODATA_USER=... ONECDC_ODATA_PASSWORD=... \
    ONECDC_DB_URL=... python examples/full_load_once.py Document_ЗаказКлиента
"""

import logging
import os
import sys

from sqlalchemy import create_engine

from onecdc import Replicator

logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s: %(message)s')

object_name = sys.argv[1] if len(sys.argv) > 1 else 'Catalog_Контрагенты'

engine = create_engine(os.environ['ONECDC_DB_URL'], pool_size=5)

rep = Replicator(
    odata_url=os.environ['ONECDC_ODATA_URL'],
    odata_auth=(os.environ['ONECDC_ODATA_USER'], os.environ['ONECDC_ODATA_PASSWORD']),
    exchange_name=os.environ['ONECDC_EXCHANGE_NAME'],
    queue_guid=os.environ.get('ONECDC_QUEUE_GUID', ''),
    engine=engine,
    db_schema=os.environ.get('ONECDC_DB_SCHEMA'),
    db_temp_schema=os.environ.get('ONECDC_DB_TEMP_SCHEMA'),
)

try:
    # Объект занимается на время выгрузки, поэтому двойной работы не будет, даже если тот же
    # объект прямо сейчас выгружает фоновый воркер или такой же скрипт в соседнем контейнере:
    # такой прогон пропустится с предупреждением в логе. Делать для этого ничего не нужно.
    #
    # Возвращается число РЕАЛЬНО изменённых строк, а не прочитанных: повторный прогон по
    # неизменившимся данным честно вернёт 0. Это не «ничего не выгрузилось», а «выгрузка сошлась
    # с тем, что уже лежит в БД». Сколько записей прочитано, видно в логе и в onecdc_replicator_log.
    changed = rep.full_load(object_name)
    print(f'{object_name}: изменено строк — {changed}')
finally:
    rep.close()
