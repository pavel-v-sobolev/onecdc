"""
Ручной отладочный вход: прогон компонентов против живой 1С и dev-Postgres из-под отладчика.
Не тест — pytest его не собирает (имя не начинается с `test_`), и в пакет он не попадает.

Запускается целиком (`python tests/debug.py`) или построчно из-под отладчика: до `main()` идут
только присваивания, поэтому любой блок можно выполнить отдельно, оставив остальные закомментированными.

Параметры стенда лежат в tests/<контур>.env рядом и берутся оттуда же живыми тестами — менять их
в одном месте достаточно.
"""

import logging

from sqlalchemy import create_engine

from onecdc import ChangeReader, DBWriter, MetadataReader, NameMapper, Replicator
from debug_config import contour

# Настройки стенда — в tests/trade_demo1.env рядом (в репозитории; своё — в
# tests/trade_demo1.local.env или в переменных окружения, см. debug_config).
CONTOUR = contour('trade_demo1')
ODATA_URL = CONTOUR.odata_url
ODATA_AUTH = CONTOUR.odata_auth
EXCHANGE_NAME = CONTOUR.exchange_name
QUEUE_GUID = CONTOUR.queue_guid
DB_URL = CONTOUR.db_url
DB_SCHEMA = CONTOUR.db_schema


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    if not CONTOUR.is_configured:
        raise SystemExit(CONTOUR.why_not)
    engine = create_engine(DB_URL)

    # Компоненты по отдельности — чтобы смотреть промежуточный результат каждого.
    metadata = MetadataReader(ODATA_URL, odata_auth=ODATA_AUTH, engine=engine, schema=DB_SCHEMA)
    changes = ChangeReader(ODATA_URL, EXCHANGE_NAME, QUEUE_GUID, metadata, odata_auth=ODATA_AUTH)
    writer = DBWriter(engine=engine, name_mapper=NameMapper(), schema=DB_SCHEMA)

    changes.read_changes()
    for object_name, data_object in changes.items():
        result = writer.save(object_name, data_object)
        print(object_name, result)
    # Подтверждение приёма намеренно НЕ отправляется: без него пакет остаётся в очереди 1С и
    # отладку можно повторять сколько угодно раз. Нужно списать — раскомментируйте.
    # changes.notify_changes_received()

    # То же самое целиком, оркестратором.
    replicator = Replicator(odata_url=ODATA_URL, odata_auth=ODATA_AUTH,
                            exchange_name=EXCHANGE_NAME, queue_guid=QUEUE_GUID,
                            engine=engine, db_schema=DB_SCHEMA)
    print(replicator.list_objects())
    replicator.run_once(notify_changes=False)


if __name__ == "__main__":
    main()
