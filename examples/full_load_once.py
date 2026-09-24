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

# Непрерывно работают две отметки живости и сама выгрузка — это pool_size. Остальное (таблица
# ключей, страницы) идёт через overflow: такие соединения закрываются сразу, как отработали.
engine = create_engine(os.environ['ONECDC_DB_URL'],
                       pool_size=3, max_overflow=2, pool_pre_ping=True)

user = os.environ.get('ONECDC_ODATA_USER')
rep = Replicator(
    odata_url=os.environ['ONECDC_ODATA_URL'],
    # Нет пользователя — 1С опубликована без авторизации.
    odata_auth=(user, os.environ.get('ONECDC_ODATA_PASSWORD', '')) if user else None,
    # Плана обмена и узла у разовой выгрузки нет: она читает объект прямо из OData. Такой
    # репликатор умеет только выгружать — читать изменения он откажется.
    engine=engine,
    db_schema=os.environ.get('ONECDC_DB_SCHEMA'),
)

try:
    # Возвращается число РЕАЛЬНО изменённых строк, а не прочитанных: повторный прогон по
    # неизменившимся данным честно вернёт 0. Это не «ничего не выгрузилось», а «выгрузка сошлась
    # с тем, что уже лежит в БД». Сколько записей прочитано, видно в логе и в onecdc_replicator_log.
    changed = rep.full_load(object_name)
    print(f'{object_name}: изменено строк — {changed}')
finally:
    rep.close()
