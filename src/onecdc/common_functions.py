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
from sqlalchemy import DateTime, func

from onecdc.logging_config import get_logger

ODATA_PREFIX = 'StandardODATA.'


# Куском какого размера принимаем тело ответа (см. read_within_limit).
CHUNK_SIZE = 1 << 20

# Лимит длины идентификатора в PostgreSQL — 63 БАЙТА (не символа: буквы вне таблицы транслита
# остаются многобайтовыми, см. truncate_to_bytes). Лежит здесь, а не в name_mapper: длину меряют
# по нему и имена таблиц, и имя схемы (см. db_logs._check_db_schema).
POSTGRES_MAX_IDENTIFIER = 63


def canonical_guid(value) -> str | None:
    """
    GUID в каноническом виде (нижний регистр, с дефисами) либо None, если это не GUID.

    Канонический вид нужен ДЛЯ СРАВНЕНИЯ: 1С отдаёт Ref_Key в нижнем регистре, а в конфигурацию
    его копируют как придётся — из формы 1С он приходит в верхнем, из реестра в фигурных скобках.
    Строковое сравнение такой guid не узнавало, и узел «не находился» при верной настройке.

    Разбирает всё, что понимает uuid.UUID: с дефисами и без, в скобках, с префиксом urn:uuid.
    """
    if not isinstance(value, str):
        return None
    try:
        return str(uuid.UUID(value.strip()))
    except ValueError:
        return None


def check_queue_guid(queue_guid) -> str:
    """
    Ref_Key узла обмена в каноническом виде. Пустой — допустим: чтение изменений тогда выведет в
    лог список узлов (см. ChangeReader._raise_no_queue_guid). Непустой обязан быть guid: имя или
    код узла в URL даст ответ 1С, по которому это не угадать.

    Живёт здесь, а не в параметрах репликатора: ChangeReader создают и напрямую (см.
    tests/debug_trade.py), и тогда значение попало бы в сравнение и в URL как написано.
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
    Basic-авторизация с учётными данными в UTF-8.

    requests кодирует их в latin-1 (историческое поведение, до RFC 7617), поэтому кириллический
    логин или пароль роняет ЛЮБОЙ запрос ещё до отправки — `UnicodeEncodeError` из глубины
    requests, по которому не догадаться, что дело в пароле. Для 1С это не экзотика: пользователь
    «админ» с русским паролем заводится повсеместно.

    Проверено на живой 1С (демо УТ, публикация на IIS): заголовок в UTF-8 принимается — 200,
    в cp1251 — 401. То есть UTF-8 это не компромисс, а единственное, что работает.
    """

    def __init__(self, user: str, password: str):
        token = base64.b64encode(f'{user}:{password}'.encode('utf-8')).decode('ascii')
        self._header = 'Basic ' + token

    def __call__(self, request):
        request.headers['Authorization'] = self._header
        return request


def odata_auth_header(odata_auth):
    """
    Пару (пользователь, пароль) превращает в авторизацию для requests; всё остальное отдаёт как
    есть — None, уже готовый объект авторизации или что угодно, что requests понимает сам.
    """
    if (isinstance(odata_auth, (tuple, list)) and len(odata_auth) == 2
            and all(isinstance(part, str) for part in odata_auth)):
        return Utf8BasicAuth(*odata_auth)
    return odata_auth


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


# Сколько ждать поток отметки живости при close(). Ограничение, а не ожидание: поток стоит на
# ожидании флага, который close() будит сразу, поэтому реальная задержка — максимум один уже
# начатый запрос. Предел нужен только чтобы зависший запрос не задержал остановку процесса.
HEARTBEAT_JOIN_TIMEOUT = 30.0


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


class ResponseTooLargeError(Exception):
    """
    Ответ 1С крупнее потолка, и мы отказались его принимать.

    Отдельный тип, потому что обращаться с ним надо не как с обычным сбоем: повтор через минуту
    получит ровно тот же ответ, а его формирование стоит серверу 1С минут работы. Цикл поэтому
    уводит паузу на потолок сразу, как для неустранимой ошибки.

    Отказ ничего не чинит — пакет остаётся в очереди 1С. Он меняет ТИХУЮ смерть по OOM на внятный
    отказ: процесс жив, полные выгрузки и обработчики работают, а в логе стоит, сколько байт
    пришло, каков потолок и что делать (см. README, «Требования к ресурсам»).
    """


def read_within_limit(response, limit: int | None, context: str) -> bytes:
    """
    Читает тело ответа, не принимая больше limit байт. limit=None — без ограничения.

    Две проверки, и обе нужны. `Content-Length` 1С отдаёт (проверено на живой), и по нему отказ
    бесплатный — тело не скачивается вовсе. Но заголовка может и не быть: перед 1С бывает прокси
    со сжатием или chunked-кодированием. Тогда границу держит счётчик принятых байт, а соединение
    обрывается на превышении.

    Зачем вообще потолок. Тело загружается целиком, и дальше от него живут ещё несколько
    представлений: текст, дерево разбора, колоночные списки. Измерено на пакете в 28 МБ — пик
    выделений 102 МБ, то есть ×3.6. Без потолка достаточно крупный пакет убивает процесс по OOM
    молча, а следующий цикл запрашивает его снова.
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
    Ответ пришёл с кодом 2xx, но это не ответ OData.

    Отдельный тип, потому что обращаться с ним надо не как с «данных нет»: прогон полной выгрузки
    обязан прерваться ДО пометки пропавших строк, иначе страница шлюза объявит удалённым весь
    объект.
    """


# Корень ошибки OData: <error> или <m:error>, с любым префиксом пространства имён.
_ODATA_ERROR_ROOT = re.compile(r'<\s*(?:[\w.-]+:)?error[\s/>]', re.I)
# Приметы ответа «такого объекта нет» в теле. Нужны потому, что тело 1С отдаёт не всегда одинаково,
# а по одному коду 404 её ответ от ответа веб-сервера не отличить. Тот же приём уже применён
# к 404.15 от IIS (см. replicator._is_query_too_long).
ENTITY_ABSENT_MARKERS = ('экземпляр сущности не найден', 'entity instance not found')


def is_entity_absent(response: requests.Response) -> bool:
    """
    404 пришёл ОТ 1С и означает «экземпляра сущности нет», а не отказ инфраструктуры.

    Проверять обязательно. Такой же 404 отдаёт IIS со снятой публикацией, ingress без нужного
    правила во время обновления, чужой vhost. А вызывают эту проверку там, где «нет» трактуется
    как ответ: в перепроверке кандидатов на пометку удалёнными. Принять отказ веб-сервера за
    ответ 1С — значит объявить живые строки удалёнными и погасить их ресурсы; полминуты такого
    404 дают полминуты ложных удалений подряд.

    Признаётся ответ 1С двумя приметами: корень тела — ошибка OData (<error>/<m:error>, либо
    JSON-исключение сервера приложений с odata.error), либо в теле есть сама формулировка
    «экземпляр сущности не найден». Страница веб-сервера не содержит ни того, ни другого.
    """
    body = (getattr(response, 'text', '') or '').lstrip('\ufeff').strip()
    if not body:
        return False
    lowered = body.lower()
    if any(marker in lowered for marker in ENTITY_ABSENT_MARKERS):
        return True
    if body.startswith('{'):
        return 'odata.error' in body
    return bool(_ODATA_ERROR_ROOT.search(body[:512]))


def parse_odata(body: str, root: str, context: str, force_list: tuple = ()) -> Any:
    """
    Разбирает ответ 1С и отдаёт содержимое ожидаемого корня (feed, entry, d:Result).

    Отсутствие корня — ОШИБКА ФОРМАТА, а не «данных нет». Разница существенная: честное «нет
    данных» от 1С выглядит как feed с нулём entry, а не как отсутствие feed. Раньше эти два случая
    были неразличимы, и 200 со страницей шлюза («Service temporarily unavailable», корректный
    XHTML — такой разбирается без ошибки) читался как пустой объект. Для полной выгрузки это
    означало пометку всех строк удалёнными: прогон считал объект дочитанным и вычищал всё, чего
    не увидел.

    Корень проверяется на ПРИСУТСТВИЕ ключа, а не на непустоту: <feed/> — это пустой, но
    совершенно законный ответ.
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
