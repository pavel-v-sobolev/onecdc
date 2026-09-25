"""
Параметры лабораторных контуров для отладочных скриптов и живых тестов.

Контур — это файл `tests/<имя>.env` рядом (trade_demo1, buh_demo1, …). Он лежит в репозитории
намеренно: это не секреты, а описание стенда, и теряться ему незачем. Добавить контур — положить
ещё один такой файл.

Три источника, по возрастанию старшинства:

    tests/<имя>.env          описание стенда, в репозитории
    tests/<имя>.local.env    личное переопределение, в .gitignore
    переменные окружения     всё, что задано снаружи

Порядок именно такой, чтобы СВОЙ контур не требовал правки отслеживаемого файла: правка «под себя»
рано или поздно уезжает в коммит вместе с чужими адресами и паролями.

Файл РАЗБИРАЕТСЯ, а не исполняется: `ONECDC_ODATA_PASSWORD=p@ss word` попадёт в значение как есть,
а `$(команда)` останется текстом. Это отличает загрузчик от `source`, которым тот же файл можно
применить в оболочке (`set -a; . tests/trade_demo1.env; set +a`) — там он именно исполняется.
"""

import os
from dataclasses import dataclass
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent


def _parse(path: Path) -> dict[str, str]:
    """`KEY=value` построчно; `#` — комментарий, кавычки вокруг значения снимаются."""
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
    """Настройки одного стенда. Чего нет — None: разбираться с этим вызывающему."""

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
    Настройки контура по имени файла. НЕ падает, когда ничего не задано, и это важно: живые тесты
    импортируются при обычном прогоне (маркер integration отсеивает их ПОСЛЕ сбора), и отказ на
    импорте сломал бы обычный `pytest`.
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
