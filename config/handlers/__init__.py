"""
Примеры пользовательских обработчиков.

Это обычный Python-пакет: создайте MartHandler с engine и tracker,
затем вызывайте run_once() или run_forever(). Импорты onecdc не нужны.
Автоматического поиска обработчиков по каталогам или именам файлов нет.
Общий код можно вынести в соседний модуль и импортировать из него нужные функции,
например: `from handlers._common import prepare_data`.
"""

from importlib import import_module

from .orders_by_customer import OrdersByCustomer

__all__ = ["OrdersByCustomer", "ZakazyKlientov", "ZakazyKlientovGrouped",
           "ZakazyKlientovClickHouse", "ZakazyKlientovGroupedClickHouse"]


def __getattr__(name):
    # Импорт автономной dbmerge-витрины не должен загружать старые onecdc handlers.
    modules = {
        "ZakazyKlientov": "zakazy_klientov",
        "ZakazyKlientovGrouped": "zakazy_klientov_grouped",
        "ZakazyKlientovClickHouse": "zakazy_klientov_clickhouse",
        "ZakazyKlientovGroupedClickHouse": "zakazy_klientov_grouped_clickhouse",
    }
    if name not in modules:
        raise AttributeError(name)
    return getattr(import_module(f".{modules[name]}", __name__), name)
