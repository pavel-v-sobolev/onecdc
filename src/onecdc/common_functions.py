import json
import re

from datetime import date, datetime

import requests
from sqlalchemy import DateTime, func

from cdc_1c.logging_config import get_logger

ODATA_PREFIX = 'StandardODATA.'

# Часы БД без часового пояса. Просто now() не годится: PostgreSQL отдаёт timestamptz, драйвер —
# offset-aware datetime, а merged_on, started_at и handlers_1c.last_run_at лежат в колонках без
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
        return _one_line(match.group(1))

    if text.startswith('{'):
        try:
            payload = json.loads(text)
        except ValueError:
            payload = None
        if isinstance(payload, dict):
            descriptions = _exception_descriptions(payload)
            if descriptions:
                return ' | '.join(descriptions)

    match = re.search(r'by reason:\s*</b>\s*<br>(.*?)</body>', text, re.S | re.I)
    if match:
        return _one_line(re.sub(r'<[^>]+>', ' ', match.group(1)))

    return _one_line(text)


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
