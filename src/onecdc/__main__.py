"""
Entrypoint для запуска из окружения без единой строки своего кода: `python -m cdc_1c` или команда
`cdc-1c`. Все настройки — переменные окружения CDC1C_*; режим задаёт CDC1C_MODE: `loop`
(по умолчанию) запускает run_forever с периодом CDC1C_POLL_INTERVAL, `once` — один run_once.

Обязательные: CDC1C_ODATA_URL, CDC1C_EXCHANGE_NAME, CDC1C_QUEUE_GUID, CDC1C_DB_URL.
CDC1C_QUEUE_GUID не знаете — запустите без него: в лог выведется список узлов плана обмена.
Необязательные: CDC1C_ODATA_USER, CDC1C_ODATA_PASSWORD (без пользователя — без авторизации),
CDC1C_DB_SCHEMA, CDC1C_DB_TEMP_SCHEMA, CDC1C_FULL_LOAD_WORKERS, CDC1C_POLL_INTERVAL, CDC1C_LOG_LEVEL, CDC1C_MODE.

Обработчиков здесь нет: они объявляются кодом, а тут кода пользователя нет. Нужны обработчики —
берите за основу config/runner.py: там ровно та же сборка, плюс по HandlerLoop на каждого
обработчика, а значения при желании заменяются литералами.
"""
import logging
import os

import requests
from sqlalchemy import create_engine

from cdc_1c.replicator import Replicator1C


def _required(name: str) -> str:
    """Обязательная переменная окружения. KeyError в трейсбеке ничего не объясняет тому, кто
    запускает контейнер, — говорим прямо, чего не хватает."""
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"{name} is not set (required: CDC1C_ODATA_URL, CDC1C_EXCHANGE_NAME, "
                         f"CDC1C_DB_URL)")
    return value


def _number(name: str, default: str, cast=float):
    """Числовая переменная окружения с понятным сообщением вместо голого ValueError."""
    value = os.environ.get(name, default).strip() or default
    try:
        number = cast(value)
    except ValueError:
        raise SystemExit(f"{name}={value!r} is not a number")
    if number <= 0:
        raise SystemExit(f"{name}={value!r} must be positive")
    return number


def main() -> None:
    odata_user = os.environ.get("CDC1C_ODATA_USER")
    odata_url = _required("CDC1C_ODATA_URL")
    full_load_workers = _number("CDC1C_FULL_LOAD_WORKERS", "2", int)

    mode = os.environ.get("CDC1C_MODE", "loop")
    if mode not in ("loop", "once"):
        # Проверяем до соединений с БД и 1С: незачем поднимать пул ради заведомо неверного режима.
        raise SystemExit(f"Unknown CDC1C_MODE={mode!r} (expected 'loop' or 'once')")

    # Пул: одновременно соединение держат цикл изменений, ДВА потока отметки живости (реестр
    # незавершённых merge и захват полной выгрузки) и страницы полной выгрузки, отсюда
    # full_load_workers + 3 (см. README_DB.md, «Сколько нужно соединений к БД»). Отметке живости,
    # не получившей соединения дольше своего TTL, никто не верит: её захват или её границу окна
    # считают брошенными.
    # Обработчиков здесь нет — были бы, добавилось бы по соединению на каждого.
    engine = create_engine(_required("CDC1C_DB_URL"), pool_size=full_load_workers + 3)

    # Параметры присваиваются явно, по одному — как и в config/runner.py: сборка одинаково
    # читается и здесь, и в пользовательском коде, где значения будут литералами.
    # Недоступная БД — не ошибка в коде, а состояние окружения: показываем строку, ради которой
    # человек и полез бы в стотридцатистрочный трейс (см. _check_db_connection в replicator).
    try:
        replicator = Replicator1C(
            odata_url=odata_url,
            odata_auth=(odata_user, os.environ.get("CDC1C_ODATA_PASSWORD", "")) if odata_user else None,
            exchange_name=_required("CDC1C_EXCHANGE_NAME"),
            # Без узла обмена работать нельзя, но KeyError тут ничего не подскажет: пустое значение
            # дойдёт до чтения изменений, и оно выведет в лог список узлов плана обмена.
            queue_guid=os.environ.get("CDC1C_QUEUE_GUID", ""),
            engine=engine,
            db_schema=os.environ.get("CDC1C_DB_SCHEMA"),
            # Схема промежуточных таблиц dbmerge; не задана — та же, что у данных.
            db_temp_schema=os.environ.get("CDC1C_DB_TEMP_SCHEMA"),
            full_load_workers=full_load_workers,
        )
    except ConnectionError as exc:
        raise SystemExit(str(exc))

    # Уровень логирования — после конструктора: он вешает обработчик на логгер cdc_1c
    # (по умолчанию INFO), а тут переопределяем на заданный (например, DEBUG/WARNING).
    log_level = os.environ.get("CDC1C_LOG_LEVEL", "INFO").strip().upper() or "INFO"
    # getLevelName на известное имя отвечает числом, на неизвестное — строкой "Level FOO".
    # Не getLevelNamesMapping(): он появился только в 3.11, а поддерживаем с 3.10.
    level = logging.getLevelName(log_level)
    if not isinstance(level, int):
        raise SystemExit(f"Unknown CDC1C_LOG_LEVEL={log_level!r} "
                         "(expected DEBUG/INFO/WARNING/ERROR/CRITICAL)")
    logging.getLogger("cdc_1c").setLevel(level)

    # run_forever недоступную 1С переживает сам (логирует и повторяет с backoff), а run_once
    # обязан отдать ошибку наружу — здесь она и превращается в строку вместо трейса.
    try:
        if mode == "once":
            replicator.run_once()
        else:
            replicator.run_forever(interval=_number("CDC1C_POLL_INTERVAL", "60"))
    except requests.RequestException as exc:
        raise SystemExit(f"1C is not available at {odata_url}: {exc}")


if __name__ == "__main__":
    main()
