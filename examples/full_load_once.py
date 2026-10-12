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

# Основной пул: до пяти соединений, из них три сохраняются для повторного использования.
# Отметки живости используют отдельный пул. Расчёт соединений описан в README_DB.md.
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
    # Результат — число изменённых строк. Ноль возможен и при совпадении данных,
    # и при пропуске объекта, занятого другим прогоном. Ход чтения виден в обычном журнале;
    # onecdc_replicator_log хранит число изменений по страницам, но не число прочитанных строк.
    changed = rep.full_load(object_name)
    print(f'{object_name}: изменено строк — {changed}')
finally:
    rep.close()
