import base64
import json
import os
import re
import socket
import uuid

from datetime import date, datetime

import requests
from sqlalchemy import DateTime, func

from onecdc.logging_config import get_logger

ODATA_PREFIX = 'StandardODATA.'


def truncate_to_bytes(name: str, max_bytes: int) -> str:
    """
    Усечение по БАЙТАМ с отбрасыванием оборванной многобайтовой последовательности.

    Именно по байтам, а не по символам: Postgres считает длину идентификатора в байтах и лишнее
    обрезает МОЛЧА, без ошибки. Кириллица (и всё, что не попало в таблицу транслита — украинские
    і ї є, белорусская ў) занимает по два байта, поэтому 63 символа легко оказываются 113 байтами,
    и два разных имени схлопываются в одну таблицу незаметно.
    """
    encoded = name.encode('utf-8')
    if len(encoded) <= max_bytes:
        return name
    return encoded[:max_bytes].decode('utf-8', errors='ignore')


def instance_owner(name: str) -> str:
    """
    Имя владельца, уникальное на ЭКЗЕМПЛЯР процесса: <name>:<хост>:<pid>:<случайный суффикс>.

    Владельцем помечаются межпроцессные отметки в БД — захват объекта под полную выгрузку
    (full_load_claim) и реестр незавершённых merge (write_tracker). Имя плана обмена или
    обработчика для этого НЕ годится: репликатор, расписание и обработчики штатно поднимаются
    в разных процессах и контейнерах одного обмена (так описано в README), и тогда они считались
    бы одним владельцем — снимали бы захваты и чистили строки друг друга.

    Случайный суффикс нужен и при совпадении хоста с pid: в контейнерах pid=1 у всех, а хост —
    это идентификатор пода, который после пересоздания повторяется.

    Хост и pid оставлены, чтобы по строке в БД было видно, кто именно держит объект.
    """
    return f'{name}:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}'

# Часы БД без часового пояса. Просто now() не годится: PostgreSQL отдаёт timestamptz, драйвер —
# offset-aware datetime, а merged_on, started_at и onecdc_handlers.last_run_at лежат в колонках без
# пояса и читаются offset-naive. Сравнить такие значения в Python нельзя — «can't compare
# offset-naive and offset-aware datetimes», — а сравниваются они постоянно (граница окна против
# last_run_at, guard'ы полной выгрузки против merged_on).
#
# Приведение делает сама БД, а не Python: там же, где живут эти часы, и ровно так же, как при
# записи timestamptz в колонку timestamp — перевод в часовой пояс сессии, затем отбрасывание
# смещения. Значение при этом не меняется, меняется только его тип.
DB_NOW_WITHOUT_TIMEZONE = func.now().cast(DateTime)


logger = get_logger(__name__)

# Предел длины описания ошибки в логе — на случай, если распознать формат не удалось и в лог идёт
# сырое тело. У распознанных ответов описание короткое, до предела не доходит.
MAX_ERROR_BODY_CHARS = 500

BYTE_UNITS = ('B', 'KB', 'MB', 'GB', 'TB')


def format_bytes(size: float) -> str:
    """
    Размер ответа для лога в удобной единице: байты для мелочи, дальше КБ/МБ/ГБ. Ответы 1С
    различаются на порядки (страница справочника — килобайты, набор движений — мегабайты),
    и в сырых байтах разницу глазом не поймать.
    """
    for unit in BYTE_UNITS:
        if size < 1024 or unit == BYTE_UNITS[-1]:
            return f'{size:.0f} {unit}' if unit == BYTE_UNITS[0] else f'{size:.1f} {unit}'
        size /= 1024


def format_duration(seconds: float) -> str:
    """
    Длительность для лога: секунды с десятой долей, от минуты — «1m 04s», от часа — «1h 05m 03s».
    Обработка пакета занимает от долей секунды до десятков минут, и в сырых секундах такой разброс
    читается плохо.
    """
    if seconds < 60:
        return f'{seconds:.1f}s'
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f'{hours}h {minutes:02d}m {sec:02d}s'
    return f'{minutes}m {sec:02d}s'


def _one_line(text: str) -> str:
    """Схлопывает переносы и лишние пробелы: описание ошибки должно занимать одну строку лога."""
    text = re.sub(r'\s+', ' ', text).strip()
    if len(text) > MAX_ERROR_BODY_CHARS:
        return f'{text[:MAX_ERROR_BODY_CHARS]}... [+{len(text) - MAX_ERROR_BODY_CHARS} chars]'
    return text


def _exception_descriptions(payload: dict) -> list[str]:
    """
    Описания из цепочки exception -> inner в JSON-исключении сервера приложений 1С. Берём только
    descr: рядом лежат creationStack (адреса в DLL) и base64-дамп на сотни строк, которые в логе
    бесполезны. Вложенные описания часто повторяют друг друга — оставляем только те, что не
    являются куском уже отобранного.

    Внешнюю обёртку вида «HTTP: Forbidden. Ошибка при выполнении запроса GET к ресурсу …»
    отбрасываем: код, метод и ресурс уже есть в нашем же сообщении. Но если она единственная —
    оставляем, лучше так, чем пустая ошибка.
    """
    kept: list[str] = []
    node = payload.get('exception') or payload
    while isinstance(node, dict):
        descr = _one_line(node.get('descr') or '')
        if descr and not any(descr in text for text in kept):
            kept = [text for text in kept if text not in descr]
            kept.append(descr)
        node = node.get('inner')

    meaningful = [text for text in kept if not text.startswith('HTTP: ')]
    return meaningful or kept


# Текст ошибки 1С кодирует в base64, когда сам не может положить его в XML, — а не может он
# ровно тогда, когда в тексте есть недопустимый символ. То есть base64 здесь надёжная примета
# самого неприятного случая, и разворачивать его надо обязательно: иначе в лог попадает
# нечитаемая простыня, а причина остаётся неизвестной.
_BASE64_ONLY = re.compile(r'^[A-Za-z0-9+/\s]{40,}={0,2}$')
# Управляющие символы: в логе они невидимы, а искать надо именно их.
_CONTROL_CHARS = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]')


def _decode_base64(text: str) -> str:
    """base64 -> текст, если это действительно base64 с валидным UTF-8 внутри. Иначе как было."""
    if not _BASE64_ONLY.match(text):
        return text
    try:
        decoded = base64.b64decode(text, validate=False).decode('utf-8')
    except (ValueError, UnicodeDecodeError):
        return text
    # Пустая или почти пустая расшифровка — скорее случайное совпадение с шаблоном, чем base64.
    return decoded if decoded.strip() else text


def _show_control_chars(text: str) -> str:
    """Управляющие символы -> <XX>. Без этого сообщение «недопустимый символ в позиции 24» ведёт
    к строке, в которой глазами ничего не видно: символ не печатается."""
    return _CONTROL_CHARS.sub(lambda m: f'<{ord(m.group()):02X}>', text)


def _readable(text: str) -> str:
    """Отделка описания перед выводом: развернуть base64, показать управляющие символы, сжать
    в одну строку. Порядок важен — base64 разворачиваем ДО подсветки, иначе подсвечивать нечего."""
    return _one_line(_show_control_chars(_decode_base64(text.strip())))


def extract_error_text(body: str) -> str:
    """
    Человекочитаемое описание ошибки из ответа 1С. Отвечает она тремя разными способами:

    - ошибка OData: XML `<m:error><m:message>…</m:message></m:error>`;
    - исключение сервера приложений: JSON с цепочкой exception/inner, где полезен только descr;
    - ошибка платформы/веб-сервера: HTML «1C:Enterprise 8 application error … by reason: …».

    Если формат не распознан, отдаём тело как есть (обрезанное). В любом случае результат —
    одна строка: полный дамп тела в лог не нужен, там мегабайты служебного мусора.
    """
    text = (body or '').lstrip('﻿').strip()
    if not text:
        return '<empty body>'

    match = re.search(r'<m:message[^>]*>(.*?)</m:message>', text, re.S)
    if match:
        return _readable(match.group(1))

    if text.startswith('{'):
        try:
            payload = json.loads(text)
        except ValueError:
            payload = None
        if isinstance(payload, dict):
            descriptions = _exception_descriptions(payload)
            if descriptions:
                return ' | '.join(_readable(d) for d in descriptions)

    match = re.search(r'by reason:\s*</b>\s*<br>(.*?)</body>', text, re.S | re.I)
    if match:
        return _readable(re.sub(r'<[^>]+>', ' ', match.group(1)))

    return _readable(text)


def odata_datetime_value(value: "date | datetime") -> str:
    """
    Дата-время в виде YYYY-MM-DDTHH:MM:SS для OData-литерала datetime'…'.

    Собирается вручную, а не strftime: у %Y год НЕ дополняется нулями до четырёх знаков (на этой
    платформе datetime(1,1,1).strftime('%Y-…') даёт '1-01-01'), а 1С такой литерал отвергает —
    400 «Ошибка при разборе опции запроса $filter». Проверено на живой 1С: datetime'1-01-01T00:00:00'
    → 400, datetime'0001-01-01T00:00:00' → 200.

    Это не экзотика: `0001-01-01` — ПУСТАЯ ДАТА 1С, она приходит в обычных данных (например, в
    измерении-дате независимого регистра сведений), и литералы строятся в том числе из значений,
    прочитанных из самой 1С (курсор по периоду, перепроверка кандидатов на пометку). Годы меньше 1000
    встречаются и как опечатка оператора.

    1С хранит дату-время с точностью до секунды, поэтому доли секунды усекаются — для 1С безопасно.
    """
    return (f'{value.year:04d}-{value.month:02d}-{value.day:02d}'
            f'T{getattr(value, "hour", 0):02d}:{getattr(value, "minute", 0):02d}'
            f':{getattr(value, "second", 0):02d}')


def raise_for_status(response: requests.Response, context: str = '') -> None:
    """
    Замена response.raise_for_status(): всё содержательное в ответе 1С лежит в теле, а штатный
    raise_for_status отдаёт наружу только «HTTPError: 500» и причину из логов не видно.
    В лог и в текст HTTPError идёт разобранное описание (см. extract_error_text), а не сырое тело.
    """
    if response.ok:
        return

    message = (f'1C request failed: {response.status_code} {response.reason} '
               f'for {context or response.url}: {extract_error_text(response.text)}')
    logger.error(message)
    raise requests.HTTPError(message, response=response)

def parse_object_full_name(object_full_name):
    """
    Очищаем имя объекта от разных префиксов, постфиксов и скобок.
    Возвращает очищенное имя и тип объекта
    """
    if object_full_name is None:
        logger.error('Object full name is None')
        return None, None

    object_name = object_full_name

    if object_name.startswith('Collection'):
        object_name = object_name.removeprefix('Collection(')
        object_name = object_name.removesuffix(')')

    object_name = object_name.removeprefix(ODATA_PREFIX)
    object_name = object_name.removesuffix('_RowType')

    if '_' in object_name:
        object_type = object_name.split('_')[0]
    else:
        logger.error(f'Object type not found in object full name {object_full_name}')
        return None, None
    return object_name, object_type
