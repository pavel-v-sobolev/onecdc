"""
Настройки тестовых стендов для отладочных скриптов и интеграционных тестов.

Каждый стенд описан в tests/<имя>.env, например trade_demo1 или buh_demo1. Эти файлы
хранят общие настройки стендов. Личные адреса и пароли задавайте в игнорируемом Git файле
.local.env или в окружении.

Локальный файл переопределяет общий, затем применяются непустые значения из окружения:

    tests/<имя>.env          общие настройки стенда
    tests/<имя>.local.env    личные переопределения
    переменные окружения    параметры текущего запуска

Загрузчик читает строки KEY=value как текст. Пробелы внутри значения сохраняются,
а подстановки оболочки, например $(команда), не выполняются. Это не shell-скрипт:
команда source имеет другую семантику и не заменяет этот загрузчик.
"""

import os
from dataclasses import dataclass
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent


def _parse(path: Path) -> dict[str, str]:
    """Читает KEY=value; пропускает пустые строки и строки с # в начале, снимает крайние кавычки."""
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        name, _, value = line.partition('=')
        values[name.strip()] = value.strip().strip('"\'')
    return values


@dataclass(frozen=True)
class Contour:
    """Настройки стенда; отсутствующие значения представлены None, обязательные перечислены в missing."""

    name: str
    odata_url: str | None
    odata_auth: tuple[str, str] | None
    exchange_name: str | None
    queue_guid: str | None
    db_url: str | None
    db_schema: str | None
    missing: tuple[str, ...]

    @property
    def is_configured(self) -> bool:
        return not self.missing

    @property
    def why_not(self) -> str:
        return (f'контур {self.name} не настроен: нет {", ".join(self.missing)} — '
                f'задайте их в tests/{self.name}.env, tests/{self.name}.local.env '
                f'или в переменных окружения')


def contour(name: str) -> Contour:
    """
    Возвращает настройки стенда и перечень отсутствующих обязательных параметров.

    Отсутствие настроек не вызывает исключения: pytest импортирует интеграционные тесты
    до отбора по маркерам, поэтому их сбор должен работать и без настроенного стенда.
    """
    values = {**_parse(TESTS_DIR / f'{name}.env'), **_parse(TESTS_DIR / f'{name}.local.env')}

    def value(key: str) -> str | None:
        return os.environ.get(key) or values.get(key) or None

    required = ('ONECDC_ODATA_URL', 'ONECDC_EXCHANGE_NAME', 'ONECDC_QUEUE_GUID', 'ONECDC_DB_URL')
    user = value('ONECDC_ODATA_USER')
    return Contour(
        name=name,
        odata_url=value('ONECDC_ODATA_URL'),
        # Нет пользователя — публикация без авторизации, как и в самой библиотеке.
        odata_auth=(user, value('ONECDC_ODATA_PASSWORD') or '') if user else None,
        exchange_name=value('ONECDC_EXCHANGE_NAME'),
        queue_guid=value('ONECDC_QUEUE_GUID'),
        db_url=value('ONECDC_DB_URL'),
        db_schema=value('ONECDC_DB_SCHEMA'),
        missing=tuple(key for key in required if not value(key)),
    )
