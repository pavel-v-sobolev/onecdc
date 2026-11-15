"""
Примеры пользовательских обработчиков.

Это обычный Python-пакет: классы нужно импортировать и передать в HandlerLoop.
Автоматического поиска обработчиков по каталогам или именам файлов нет.
Общий код можно вынести в соседний модуль и импортировать из него нужные функции,
например: `from handlers._common import prepare_data`.
"""

from .orders_by_customer import OrdersByCustomer
from .zakazy_klientov import ZakazyKlientov
from .zakazy_klientov_grouped import ZakazyKlientovGrouped
from .zakazy_klientov_clickhouse import ZakazyKlientovClickHouse
from .zakazy_klientov_grouped_clickhouse import ZakazyKlientovGroupedClickHouse

__all__ = ["OrdersByCustomer", "ZakazyKlientov", "ZakazyKlientovGrouped",
           "ZakazyKlientovClickHouse", "ZakazyKlientovGroupedClickHouse"]
