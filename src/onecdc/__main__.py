"""
Запуск OneCDC из переменных окружения: команда onecdc или python -m onecdc.

ONECDC_MODE=loop запускает непрерывный цикл с фоновыми полными выгрузками;
once выполняет один цикл изменений. Обязательны ONECDC_ODATA_URL,
ONECDC_EXCHANGE_NAME и ONECDC_DB_URL. Для чтения изменений нужен ONECDC_QUEUE_GUID;
без него выводится список узлов, а цикл завершается ошибкой.

Все параметры перечислены в README_ENV.md. Обработчики, расписания и несколько
репликаторов настраиваются Python-кодом; пример — config/runner.py.
"""
import logging
import math
import os

import requests
from sqlalchemy import create_engine

from onecdc.replicator import Replicator
from onecdc.db_logs import DEFAULT_LOG_RETENTION_DAYS


def _required(name: str) -> str:
    """Прочитать непустую обязательную переменную или завершить команду с её именем в ошибке."""
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"{name} is not set (required: ONECDC_ODATA_URL, ONECDC_EXCHANGE_NAME, "
                         f"ONECDC_DB_URL)")
    return value


def _number(name: str, default: str, cast=float, *, allow_zero: bool = False):
    """
    Прочитать конечное положительное число, преобразовав его функцией cast.

    allow_zero=True дополнительно разрешает ноль; используется для бессрочного
    хранения журнала. Ошибки значения завершают команду сообщением с именем переменной.
    """
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
    if number < 0 or (number == 0 and not allow_zero):
        requirement = "non-negative" if allow_zero else "positive"
        raise SystemExit(f"{name}={value!r} must be {requirement}")
    return number


_TRUE = ("1", "true", "yes", "on")
_FALSE = ("0", "false", "no", "off")


def _flag(name: str, default: bool) -> bool:
    """Прочитать булев флаг из окружения; неизвестное написание завершает команду с ошибкой."""
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
    # Сколько суток держать журнал загрузок; 0 — хранить всё.
    log_retention_days = _number("ONECDC_LOG_RETENTION_DAYS", str(DEFAULT_LOG_RETENTION_DAYS),
                                int, allow_zero=True)
    automatic_full_load = _flag("ONECDC_AUTOMATIC_FULL_LOAD", True)

    mode = os.environ.get("ONECDC_MODE", "loop")
    if mode not in ("loop", "once"):
        # Проверяем до соединений с БД и 1С: незачем поднимать пул ради заведомо неверного режима.
        raise SystemExit(f"Unknown ONECDC_MODE={mode!r} (expected 'loop' or 'once')")

    # Предел основного пула — pool_size + max_overflow: три соединения для цикла изменений
    # и служебных запросов плюс запас на параллельные полные выгрузки. Соединения не закреплены
    # за конкретными потоками. Отметки живости используют отдельный пул; расчёт — в README_DB.md.
    # pool_pre_ping проверяет соединение перед выдачей, в том числе после длительного простоя.
    engine = create_engine(_required("ONECDC_DB_URL"), pool_size=3,
                           max_overflow=full_load_workers, pool_pre_ping=True)

    # Параметры присваиваются явно, по одному — как и в config/runner.py: сборка одинаково
    # читается и здесь, и в пользовательском коде, где значения будут литералами.
    # Ошибку подключения выводим кратко; подробности формирует _check_db_connection в replicator.
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
            # Схема таблиц ключей полной выгрузки; по умолчанию — схема данных.
            db_temp_schema=os.environ.get("ONECDC_DB_TEMP_SCHEMA"),
            full_load_workers=full_load_workers,
            log_retention_days=log_retention_days,
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
