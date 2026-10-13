"""
Общие преобразования и небольшие помощники, используемые несколькими модулями.

Здесь собраны нормализация GUID и имён OData, подготовка Basic-авторизации, форматирование дат и
времени, разбор ответов OData и ошибок 1С, ограниченное чтение HTTP-ответа, форматирование
значений для логов, а также общие отметки времени и владельцы экземпляров процесса. Модуль не
содержит циклов загрузки: их поведение находится в `change_reader`, `data_reader` и `replicator`.
"""

import base64
import json
import os
import re
import socket
import uuid

from datetime import date, datetime
from typing import Any
from xml.parsers.expat import ExpatError

import requests
import xmltodict
from sqlalchemy import func

from onecdc.logging_config import get_logger

ODATA_PREFIX = 'StandardODATA.'


# Размер одного фрагмента при чтении HTTP-ответа (1 МиБ); это не ограничение общего размера
# ответа — его задаёт MAX_RESPONSE_BYTES. Заодно это точность, с которой срабатывает потолок:
# счётчик принятых байт проверяется после каждого фрагмента, поэтому лимит может быть перебран
# на фрагмент прежде, чем соединение оборвётся (см. read_within_limit).
CHUNK_SIZE = 1024 * 1024

# PostgreSQL хранит не больше 63 байт имени идентификатора (не символов). Символы, оставшиеся
# многобайтовыми после транслитерации, занимают несколько байт; см. truncate_to_bytes. Константа
# общая для имён таблиц и схем, поэтому она находится здесь, а не в name_mapper.
POSTGRES_MAX_IDENTIFIER = 63


def canonical_guid(value) -> str | None:
    """
    Вернуть GUID в нижнем регистре с дефисами или None для некорректного значения.

    Пробелы по краям удаляются. Принимаются строковые формы, поддерживаемые uuid.UUID,
    включая запись без дефисов, в фигурных скобках и с префиксом urn:uuid:.
    Единая форма нужна для сравнения настроенного queue_guid с Ref_Key из ответа 1С.
    """
    if not isinstance(value, str):
        return None
    try:
        return str(uuid.UUID(value.strip()))
    except ValueError:
        return None


def check_queue_guid(queue_guid) -> str:
    """
    Проверить Ref_Key узла обмена и вернуть GUID в канонической форме.

    None и пустая строка превращаются в ''. При чтении изменений такое значение
    приведёт к выводу списка доступных узлов и ValueError. Непустое значение должно
    быть строкой GUID; код и наименование узла не принимаются.

    Проверка общая для Replicator и для прямого создания ChangeReader.
    """
    if queue_guid is None:
        return ''
    if not isinstance(queue_guid, str):
        raise ValueError(f"queue_guid must be a string Ref_Key of the exchange node "
                         f"(got {queue_guid!r})")
    if not queue_guid.strip():
        return ''
    guid = canonical_guid(queue_guid)
    if guid is None:
        raise ValueError(f"queue_guid must be the Ref_Key (guid) of the exchange node, not its "
                         f"code or name (got {queue_guid!r}). Leave it empty to log the list of "
                         f"available nodes")
    return guid


class Utf8BasicAuth(requests.auth.AuthBase):
    """
    Basic-авторизация с кодированием имени пользователя и пароля в UTF-8.

    Стандартный обработчик requests использует latin-1, который не поддерживает
    кириллицу. UTF-8 проверен на демо-базе УТ, опубликованной через IIS.
    """

    def __init__(self, user: str, password: str):
        """Собрать заголовок Basic из пары строк, закодированной в UTF-8."""
        token = base64.b64encode(f'{user}:{password}'.encode('utf-8')).decode('ascii')
        self._header = 'Basic ' + token

    def __call__(self, request):
        """Установить `Authorization` в подготовленном запросе и вернуть его."""
        request.headers['Authorization'] = self._header
        return request


def odata_auth_header(odata_auth):
    """
    Преобразовать пару строк (пользователь, пароль) в Utf8BasicAuth.

    None и готовые объекты авторизации возвращаются без изменений.
    """
    if (isinstance(odata_auth, (tuple, list)) and len(odata_auth) == 2
            and all(isinstance(part, str) for part in odata_auth)):
        return Utf8BasicAuth(*odata_auth)
    return odata_auth


def truncate_to_bytes(name: str, max_bytes: int) -> str:
    """
    Усечь строку до max_bytes байт UTF-8, сохранив только целые символы.

    Используется для идентификаторов PostgreSQL, длина которых ограничена байтами,
    а не числом символов.
    """
    encoded = name.encode('utf-8')
    if len(encoded) <= max_bytes:
        return name
    return encoded[:max_bytes].decode('utf-8', errors='ignore')


# Максимальное время ожидания потока отметки живости при close() его владельца. Владельцев три, и
# механика у них одна, поэтому константа общая: Lease (аренда роли — узел обмена, имя
# обработчика), FullLoadClaim (захват объекта под полную выгрузку) и WriteTracker (реестр
# незавершённых merge).
#
# Обычно поток просыпается сразу после установки флага остановки; если он уже выполняет запрос,
# close() ждёт не дольше этого срока. Ограничение не отменяет сам запрос, а не даёт его зависанию
# задержать остановку процесса.
HEARTBEAT_JOIN_TIMEOUT = 30.0


def instance_owner(name: str) -> str:
    """
    Сформировать идентификатор владельца: <name>:<хост>:<pid>:<случайный суффикс>.

    Идентификатор различает экземпляры аренды и реестров между процессами и после
    перезапуска. Одного имени плана обмена недостаточно: несколько процессов могут
    работать с тем же планом. Хост и PID позволяют определить владельца по записи в БД.
    """
    return f'{name}:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}'

# Текущее время БД как момент времени. Служебные отметки хранятся в timestamptz; без приведения
# SQLAlchemy получает от драйвера datetime с часовым поясом, и сравнения остаются timezone-aware.
#
# Раньше `now()` приводился к `timestamp without time zone`. Такое значение — местное настенное
# время: при осеннем переводе часов один час повторяется, поэтому временные метки из этих двух
# часов нельзя надёжно упорядочить. Из-за этого некоторые строки с merged_on оказывались раньше
# last_run_at и выпадали из окна обработчика (CDC-25). Тип timestamptz сохраняет однозначный
# момент времени и устраняет эту неоднозначность.
#
# Даты из 1С (например, Период и Дата) остаются без часового пояса: это бизнес-значения, а не
# моменты времени. Проверка границ периода учитывает это различие в replicator._check_period_bound.
DB_NOW_WITH_TIMEZONE = func.now()


logger = get_logger(__name__)

# Сколько символов исходного текста ошибки оставлять перед пометкой о сокращении. Ограничение
# применяется и к нераспознанному ответу, когда вместо описания используется его тело.
MAX_ERROR_BODY_CHARS = 500

BYTE_UNITS = ('B', 'KB', 'MB', 'GB', 'TB')


def format_bytes(size: float) -> str:
    """
    Форматировать размер для журнала, последовательно деля его на 1024.

    Байты выводятся целым числом, более крупные единицы — с одним десятичным знаком.
    Суффиксы B/KB/MB/GB/TB сохранены для совместимости формата журнала.
    """
    for unit in BYTE_UNITS:
        if size < 1024 or unit == BYTE_UNITS[-1]:
            return f'{size:.0f} {unit}' if unit == BYTE_UNITS[0] else f'{size:.1f} {unit}'
        size /= 1024


def format_duration(seconds: float) -> str:
    """Форматировать длительность: 0.1s, 1m 04s или 1h 05m 03s."""
    if seconds < 60:
        return f'{seconds:.1f}s'
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f'{hours}h {minutes:02d}m {sec:02d}s'
    return f'{minutes}m {sec:02d}s'


def _error_excerpt(text: str) -> str:
    """
    Подготовить краткий текст ошибки для журнала и onecdc_handlers.last_error.

    Переносы строк и повторяющиеся пробелы заменяются одиночными пробелами. Текст ограничивается
    MAX_ERROR_BODY_CHARS с указанием числа пропущенных символов. Это нужно для ответов 1С,
    которые могут содержать многострочный traceback или большой дамп в base64.

    Функция предназначена только для диагностических сообщений: сокращать ею данные нельзя.
    """
    text = re.sub(r'\s+', ' ', text).strip()
    if len(text) > MAX_ERROR_BODY_CHARS:
        return f'{text[:MAX_ERROR_BODY_CHARS]}... [+{len(text) - MAX_ERROR_BODY_CHARS} chars]'
    return text


def _exception_descriptions(payload: dict) -> list[str]:
    """
    Извлечь описания из цепочки exception → inner в JSON-ошибке сервера 1С.

    Используется поле descr; стеки creationStack и дампы base64 пропускаются. Повторяющиеся
    описания и описания, целиком входящие в уже выбранное, исключаются.

    Внешнее описание вида «HTTP: Forbidden. Ошибка при выполнении запроса GET к ресурсу …»
    дублирует код, метод и ресурс из сообщения библиотеки. Оно сохраняется только тогда,
    когда других описаний нет.
    """
    kept: list[str] = []
    node = payload.get('exception') or payload
    while isinstance(node, dict):
        descr = _error_excerpt(node.get('descr') or '')
        if descr and not any(descr in text for text in kept):
            kept = [text for text in kept if text not in descr]
            kept.append(descr)
        node = node.get('inner')

    meaningful = [text for text in kept if not text.startswith('HTTP: ')]
    return meaningful or kept


# В ответах об ошибках 1С может передавать описание в base64, например если в нём есть символы,
# которые нельзя записать в XML. Распознаём такие строки по алфавиту base64 и минимальной длине,
# а затем проверяем, что результат декодируется как UTF-8. Если проверка не проходит, сохраняем
# исходный текст.
_BASE64_ONLY = re.compile(r'^[A-Za-z0-9+/\s]{40,}={0,2}$')
# Управляющие символы могут быть невидимы в логе; заменяем их на явную запись вида <0A>.
_CONTROL_CHARS = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]')


def _decode_base64(text: str) -> str:
    """Декодировать похожую на base64 строку с текстом UTF-8; иначе вернуть исходную."""
    if not _BASE64_ONLY.match(text):
        return text
    try:
        decoded = base64.b64decode(text, validate=False).decode('utf-8')
    except (ValueError, UnicodeDecodeError):
        return text
    # Пустая после удаления пробелов строка чаще означает случайное совпадение с шаблоном,
    # поэтому в таком случае оставляем исходное значение.
    return decoded if decoded.strip() else text


def _show_control_chars(text: str) -> str:
    """Заменить непечатаемые управляющие символы записью <XX> с их кодом."""
    return _CONTROL_CHARS.sub(lambda m: f'<{ord(m.group()):02X}>', text)


def _readable(text: str) -> str:
    """Декодировать base64, показать управляющие символы и свернуть текст в одну строку."""
    return _error_excerpt(_show_control_chars(_decode_base64(text.strip())))


def extract_error_text(body: str) -> str:
    """
    Извлечь краткое описание ошибки из тела ответа 1С.

    Поддерживаются XML с m:message, JSON с цепочкой exception/inner/descr и HTML
    с описанием после «by reason:». Нераспознанное тело также приводится к одной строке
    и сокращается. Пустое тело возвращается как <empty body>.
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
    400 «Ошибка при разборе опции запроса $filter». Проверено на реальной 1С: datetime'1-01-01T00:00:00'
    → 400, datetime'0001-01-01T00:00:00' → 200.

    Это не экзотика: `0001-01-01` — пустая дата 1С, она приходит в обычных данных (например, в
    измерении-дате независимого регистра сведений), и литералы строятся в том числе из значений,
    прочитанных из самой 1С (курсор по периоду, границы окна). Годы меньше 1000 встречаются и как
    опечатка оператора.

    1С хранит дату-время с точностью до секунды, поэтому доли секунды усекаются — для 1С безопасно.
    """
    return (f'{value.year:04d}-{value.month:02d}-{value.day:02d}'
            f'T{getattr(value, "hour", 0):02d}:{getattr(value, "minute", 0):02d}'
            f':{getattr(value, "second", 0):02d}')


class ResponseTooLargeError(Exception):
    """
    Размер ответа 1С превысил установленный предел.

    Повторный запрос обычно вернёт тот же большой ответ и снова потребует работы сервера.
    Поэтому цикл изменений сразу устанавливает максимальную паузу перед повтором.

    Пакет остаётся в очереди 1С. Чтение прекращается до разбора ответа, но это не ограничивает
    общую память процесса. Сообщение содержит установленный предел и размер из заголовка
    либо число уже принятых байт.
    """


def read_within_limit(response, limit: int | None, context: str) -> bytes:
    """
    Вернуть тело ответа или поднять ResponseTooLargeError при превышении limit.

    Если Content-Length уже превышает предел, ответ закрывается без чтения тела.
    Независимо от заголовка проверяется суммарный размер принятых фрагментов. Проверка
    выполняется после чтения фрагмента, поэтому до отказа можно принять до CHUNK_SIZE
    лишних байт. При превышении ответ закрывается; limit=None отключает обе проверки.

    Ограничение относится к телу ответа. Его последующий разбор требует дополнительной
    памяти: в одном из замеров 28 МБ ответа дали 102 МБ пиковых выделений.
    """
    declared = response.headers.get('Content-Length')
    if limit is not None and declared and declared.isdigit() and int(declared) > limit:
        response.close()
        raise ResponseTooLargeError(
            f'{context}: 1C declared {int(declared)} bytes, over the {limit}-byte limit — '
            f'the body was not downloaded at all')

    chunks: list[bytes] = []
    received = 0
    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
        received += len(chunk)
        if limit is not None and received > limit:
            response.close()
            raise ResponseTooLargeError(
                f'{context}: the response exceeded the {limit}-byte limit (got at least '
                f'{received} bytes) and the connection was dropped')
        chunks.append(chunk)
    return b''.join(chunks)


class ODataFormatError(ValueError):
    """
    Успешный HTTP-ответ не содержит ожидаемых данных OData.

    Полная выгрузка должна завершиться с ошибкой до пометки пропавших строк. Иначе, например,
    HTML-страница шлюза могла бы быть принята за пустой объект, а все его строки — за удалённые.
    """


def parse_odata(body: str, root: str, context: str, force_list: tuple = ()) -> Any:
    """
    Разобрать XML и вернуть содержимое ожидаемого корня feed, entry или d:Result.

    Невалидный XML и отсутствие корня вызывают ODataFormatError. Пустой корень допустим:
    <feed/> означает отсутствие записей, а страница шлюза без feed — ошибку формата.
    Это различие не позволяет полной выгрузке принять ошибочный ответ за пустой объект
    и пометить все его строки отсутствующими. force_list передаётся в xmltodict.parse.
    """
    try:
        parsed = xmltodict.parse(body, force_list=force_list)
    except ExpatError as exc:
        raise ODataFormatError(
            f'{context}: response is not XML at all ({exc}); '
            f'body starts with {body[:120]!r}') from exc
    if not isinstance(parsed, dict) or root not in parsed:
        raise ODataFormatError(
            f'{context}: response has no <{root}> — this is not an OData answer, and it must not '
            f'be read as "no data"; body starts with {body[:120]!r}')
    return parsed[root]


def raise_for_status(response: requests.Response, context: str = '') -> None:
    """
    Для ошибочного HTTP-ответа записать причину в журнал и вызвать HTTPError.

    Описание извлекается из тела ответа 1С через extract_error_text. context уточняет,
    какой запрос завершился ошибкой; если он пуст, используется response.url.
    """
    if response.ok:
        return

    message = (f'1C request failed: {response.status_code} {response.reason} '
               f'for {context or response.url}: {extract_error_text(response.text)}')
    logger.error(message)
    raise requests.HTTPError(message, response=response)

def parse_object_full_name(object_full_name):
    """
    Нормализовать имя типа объекта из OData и вернуть `(имя, класс)`.

    Убирает обёртку `Collection(...)`, префикс `StandardODATA.` и суффикс `_RowType`.
    Например, `Collection(StandardODATA.Document_Заказ_RowType)` превращается в
    `Document_Заказ`; классом будет `Document` — часть до первого подчёркивания.

    Для `None` или имени без подчёркивания возвращает `(None, None)` и пишет ошибку в лог.
    Предполагается, что остальные входные значения — строки в формате имён OData.
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
