"""
Примеры пользовательских обработчиков.

Это обычный Python-пакет: классы нужно импортировать и передать в HandlerLoop.
Автоматического поиска обработчиков по каталогам или именам файлов нет.
Общий код можно вынести в соседний модуль и импортировать из него нужные функции,
например: `from handlers._common import prepare_data`.
"""

from .zakazy_klientov import ZakazyKlientov
from .zakazy_klientov_grouped import ZakazyKlientovGrouped

__all__ = ["ZakazyKlientov", "ZakazyKlientovGrouped"]
