"""
Entrypoint для запуска из окружения без единой строки своего кода: `python -m onecdc` или команда
`onecdc`. Все настройки — переменные окружения ONECDC_*; режим задаёт ONECDC_MODE: `loop`
(по умолчанию) запускает run_forever с периодом ONECDC_POLL_INTERVAL, `once` — один run_once.

Обязательные: ONECDC_ODATA_URL, ONECDC_EXCHANGE_NAME, ONECDC_QUEUE_GUID, ONECDC_DB_URL.
ONECDC_QUEUE_GUID не знаете — запустите без него: в лог выведется список узлов плана обмена.
Необязательные: ONECDC_ODATA_USER, ONECDC_ODATA_PASSWORD (без пользователя — без авторизации),
ONECDC_DB_SCHEMA, ONECDC_DB_TEMP_SCHEMA, ONECDC_FULL_LOAD_WORKERS, ONECDC_AUTOMATIC_FULL_LOAD,
ONECDC_POLL_INTERVAL, ONECDC_LOG_LEVEL, ONECDC_MODE.

Обработчиков здесь нет: они объявляются кодом, а тут кода пользователя нет. Нужны обработчики —
берите за основу config/runner.py: там ровно та же сборка, плюс по HandlerLoop на каждого
обработчика, а значения при желании заменяются литералами.
"""
import logging
import math
import os

import requests
from sqlalchemy import create_engine

from onecdc.replicator import Replicator


def _required(name: str) -> str:
    """Обязательная переменная окружения. KeyError в трейсбеке ничего не объясняет тому, кто
    запускает контейнер, — говорим прямо, чего не хватает."""
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"{name} is not set (required: ONECDC_ODATA_URL, ONECDC_EXCHANGE_NAME, "
                         f"ONECDC_DB_URL)")
    return value


def _number(name: str, default: str, cast=float):
    """Числовая переменная окружения с понятным сообщением вместо голого ValueError."""
    value = os.environ.get(name, default).strip() or default
    try:
        number = cast(value)
    except ValueError:
        raise SystemExit(f"{name}={value!r} is not a number")
    if not math.isfinite(number):
        # nan и inf проходят проверку на положительность: сравнение с nan всегда ложно, а inf
        # больше нуля честно. Цена — горячий цикл (nan делает паузу пустой) либо остановка опроса
        # после первого оборота (inf).
        raise SystemExit(f"{name}={value!r} must be a finite number")
    if number <= 0:
        raise SystemExit(f"{name}={value!r} must be positive")
    return number


_TRUE = ("1", "true", "yes", "on")
_FALSE = ("0", "false", "no", "off")


def _flag(name: str, default: bool) -> bool:
    """Булева переменная окружения. Строку в bool() не отдаём: "false" в питоне истинна, и
    выключатель, набранный словом, молча остался бы включённым."""
    value = os.environ.get(name, "").strip().lower()
    if not value:
        return default
    if value in _TRUE:
        return True
    if value in _FALSE:
        return False
    raise SystemExit(f"{name}={value!r} is not a flag (expected true/false)")


def main() -> None:
    odata_user = os.environ.get("ONECDC_ODATA_USER")
    odata_url = _required("ONECDC_ODATA_URL")
    full_load_workers = _number("ONECDC_FULL_LOAD_WORKERS", "2", int)
    automatic_full_load = _flag("ONECDC_AUTOMATIC_FULL_LOAD", True)

    mode = os.environ.get("ONECDC_MODE", "loop")
    if mode not in ("loop", "once"):
        # Проверяем до соединений с БД и 1С: незачем поднимать пул ради заведомо неверного режима.
        raise SystemExit(f"Unknown ONECDC_MODE={mode!r} (expected 'loop' or 'once')")

    # Пул: одновременно соединение держат цикл изменений, ДВА потока отметки живости (реестр
    # незавершённых merge и захват полной выгрузки) и страницы полной выгрузки, отсюда
    # full_load_workers + 3 (см. README_DB.md, «Сколько нужно соединений к БД»). Отметке живости,
    # не получившей соединения дольше своего TTL, никто не верит: её захват или её границу окна
    # считают брошенными.
    # Обработчиков здесь нет — были бы, добавилось бы по соединению на каждого.
    # Потолок соединений — это СУММА pool_size и max_overflow (по умолчанию он 10, и тогда pool_size=5
    # на деле удерживает 15). В pool_size — то, что работает непрерывно; страницы выгрузки идут через
    # overflow: такие соединения закрываются сразу, как отработали, а не висят простаивая.
    # pool_pre_ping спасает от соединения, закрытого сервером за время простоя.
    engine = create_engine(_required("ONECDC_DB_URL"), pool_size=3,
                           max_overflow=full_load_workers, pool_pre_ping=True)

    # Параметры присваиваются явно, по одному — как и в config/runner.py: сборка одинаково
    # читается и здесь, и в пользовательском коде, где значения будут литералами.
    # Недоступная БД — не ошибка в коде, а состояние окружения: показываем строку, ради которой
    # человек и полез бы в стотридцатистрочный трейс (см. _check_db_connection в replicator).
    try:
        replicator = Replicator(
            odata_url=odata_url,
            odata_auth=(odata_user, os.environ.get("ONECDC_ODATA_PASSWORD", "")) if odata_user else None,
            exchange_name=_required("ONECDC_EXCHANGE_NAME"),
            # Без узла обмена работать нельзя, но KeyError тут ничего не подскажет: пустое значение
            # дойдёт до чтения изменений, и оно выведет в лог список узлов плана обмена.
            queue_guid=os.environ.get("ONECDC_QUEUE_GUID", ""),
            engine=engine,
            db_schema=os.environ.get("ONECDC_DB_SCHEMA"),
            # Схема промежуточных таблиц dbmerge; не задана — та же, что у данных.
            db_temp_schema=os.environ.get("ONECDC_DB_TEMP_SCHEMA"),
            full_load_workers=full_load_workers,
            # Новый объект в пакете сам встаёт на полную выгрузку. Выключают тем, кто инициирует
            # первую загрузку на стороне 1С или назначает её расписанием.
            automatic_full_load=automatic_full_load,
        )
    except ConnectionError as exc:
        raise SystemExit(str(exc))

    # Уровень логирования — после конструктора: он вешает обработчик на логгер onecdc
    # (по умолчанию INFO), а тут переопределяем на заданный (например, DEBUG/WARNING).
    log_level = os.environ.get("ONECDC_LOG_LEVEL", "INFO").strip().upper() or "INFO"
    # getLevelName на известное имя отвечает числом, на неизвестное — строкой "Level FOO".
    # Не getLevelNamesMapping(): он появился только в 3.11, а поддерживаем с 3.10.
    level = logging.getLevelName(log_level)
    if not isinstance(level, int):
        raise SystemExit(f"Unknown ONECDC_LOG_LEVEL={log_level!r} "
                         "(expected DEBUG/INFO/WARNING/ERROR/CRITICAL)")
    logging.getLogger("onecdc").setLevel(level)

    # run_forever недоступную 1С переживает сам (логирует и повторяет с backoff), а run_once
    # обязан отдать ошибку наружу — здесь она и превращается в строку вместо трейса.
    try:
        if mode == "once":
            replicator.run_once()
        else:
            replicator.run_forever(interval=_number("ONECDC_POLL_INTERVAL", "60"))
    except requests.RequestException as exc:
        raise SystemExit(f"1C is not available at {odata_url}: {exc}")


if __name__ == "__main__":
    main()
