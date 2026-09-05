import functools
import os
import re
import socket
import uuid
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from datetime import date, datetime, timedelta
from urllib.parse import quote

import requests
from dbmerge import mergeResult
from sqlalchemy import Engine, Integer, Numeric, and_
from sqlalchemy.exc import NoSuchTableError, OperationalError

from cdc_1c.metadata_reader import ACCOUNTING_REGISTER_TYPE, MetadataReader1C, type_mapping
from cdc_1c.common_functions import format_duration, odata_datetime_value
from cdc_1c.data_reader import (DataReader1C, IS_DELETED_OR_EMPTY_FIELD, ODATA_PREFIX,
                                RECORDER_FIELDS, _odata_literal)
from cdc_1c.change_reader import ChangeReader1C
from cdc_1c.full_load_claim import FullLoadClaim
from cdc_1c.name_mapper import NameMapper1C
from cdc_1c.db_writer import DBWriter1C, save_order_key
from cdc_1c.db_logs import Replicator1CLog, LOAD_TYPE_CHANGES, LOAD_TYPE_FULL
from cdc_1c.full_load_keys import FullLoadKeys
from cdc_1c.handlers import (HandlerSignals, SOURCE_CHANGES, SOURCE_FULL_LOAD)
from cdc_1c.stop_signal import StopSignal, install_signal_handlers
from cdc_1c.write_tracker import WriteTracker
from cdc_1c.logging_config import _ensure_handler, get_logger, load_mode, LOAD_MODE_CHANGES, LOAD_MODE_FULL

logger = get_logger(__name__)

# Ретрай упавшего цикла (run_forever): экспоненциальная пауза вместо слепого повтора каждые
# interval секунд. Неудачный SelectChanges — это не бесплатная попытка: 1С успевает отработать
# минуты на таблице регистрации изменений, и повторы начинают накладываться друг на друга,
# порождая уже конфликты блокировок. Пауза удваивается до потолка и сбрасывается после успеха.
BACKOFF_FACTOR = 2.0
DEFAULT_MAX_BACKOFF = 1800.0

# HTTP-коды, при которых повтор того же запроса бессмысленен: права, адрес, состав запроса.
# Такие ошибки сразу уводят паузу на потолок — процесс живёт (перезапуск ничего не чинит),
# но 1С не долбим. Всё остальное (таймаут, обрыв, 5xx, конфликт блокировок) считаем временным.
PERMANENT_HTTP_CODES = frozenset((400, 401, 403, 404, 405, 501))

# Полная выгрузка: во сколько раз уменьшать страницу, если 1С не осилила запрос, и нижний предел.
# Страницу объекта 1С собирает целиком во временных файлах на сервере приложений, а её объём
# зависит не от batch_size, а от того, сколько строк тянется вместе с одной записью: у документа
# с табличными частями entry — это сотни строк, у регистраторного регистра — весь набор движений
# регистратора, а он бывает и в мегабайт. Универсального batch_size поэтому нет: при отказе
# уменьшаем страницу и повторяем, вплоть до одной записи за запрос — меньше уже некуда, entry
# неделима. Найденный размер запоминается на объект (_full_load_page_size), чтобы повторный
# прогон не начинал снова с batch_size и не жёг сервер заведомо провальными попытками.
FULL_LOAD_BATCH_DIVISOR = 4
FULL_LOAD_MIN_BATCH = 1

# Целевой вес страницы полной выгрузки и размер первой («пробной») страницы, пока вес entry
# неизвестен. Размер страницы подбирается по факту: после каждой страницы известен её вес и
# число entry, отсюда — сколько entry укладывается в бюджет. batch_size остаётся верхней
# границей. Просить у 1С сразу batch_size нельзя: у толстого объекта это гигабайты временных
# файлов на сервере приложений, и запрос падает ещё до того, как мы узнаем вес entry.
FULL_LOAD_TARGET_BYTES = 32 * 1024 * 1024
FULL_LOAD_PROBE_BATCH = 20

# Полная выгрузка режется на окна по периоду, а внутри окна страницы берутся через $skip. Смысл
# в том, что $skip дорог: 1С на каждый запрос строит выборку заново, сортирует и отбрасывает
# первые N строк, поэтому цена растёт квадратично по числу страниц. Фильтр по дате переводит
# запрос на индекс (Дата у документа, Период у регистра входят в него), и сортируется уже
# маленький кусок.
#
# Окно глубже этого числа страниц сужается: значит и в нём $skip уходит слишком далеко. Порог
# небольшой, потому что перечитывание уже прочитанных страниц — цена сужения, и платить за неё
# много раз не хочется.
FULL_LOAD_PARTITION_MAX_PAGES = 10

# Окна отмеряются В ДНЯХ, а не календарными месяцами: месяц — единица неравномерная (28–31 день),
# и от неё нет никакой пользы, потому что окно всё равно подбирается по глубине, а не по календарю.
# Идём от свежих к старым, начальный размер — FULL_LOAD_WINDOW_DAYS; окно, упёршееся в лимит
# страниц, делится на FULL_LOAD_WINDOW_DIVISOR и перечитывается, вплоть до FULL_LOAD_WINDOW_MIN_DAYS
# (день — минимум, дальше дробить бессмысленно: внутри дня фильтр по дате уже ничего не отсекает).
#
# Только сужение, обратно окно не растёт. Причина та же, что у потолка размера страницы
# (_full_load_page_limit): вернувшись к прежнему размеру, мы снова упрёмся в лимит и заплатим за
# перечитывание ещё раз.
FULL_LOAD_WINDOW_DAYS = 30
FULL_LOAD_WINDOW_MIN_DAYS = 1
FULL_LOAD_WINDOW_DIVISOR = 3

# Сколько пустых окон подряд считать «дальше шагать не по чему»: обход прекращается, а остаток
# истории добирается одним сплошным чтением без нижней границы.
#
# Считаем их ВСЕГДА, а не только когда границы периода у 1С не спросить (регистр, подчинённый
# регистратору: дата лежит внутри набора записей, и $orderby по ней платформа молча игнорирует —
# см. _supports_date_bounds). Полученной от 1С границе тоже нельзя верить как мере работы: в
# периоде встречается мусор — пустая дата 1С (0001-01-01) или промах пальцем (в демо-базе
# бухгалтерии есть запись за 0209 год). Одна такая запись растягивала обход на 22 тысячи окон.
# Граница остаётся полезной как признак ПОСЛЕДНЕГО окна, когда история кончается честно.
FULL_LOAD_EMPTY_WINDOWS_TO_STOP = 3

# Поля, по которым выгрузка режется на периоды, если пользователь не задал своё: у документа это
# Date, у регистра — Period. Порядок важен: у регистра сведений бывают оба.
PARTITION_DATE_FIELDS = ('Date', 'Period')
# Поле периода записи регистра. У регистра бухгалтерии оно же — единственная ось, по которой
# режется полная выгрузка (см. full_load).
PERIOD_FIELD = 'Period'

# Имя переменной в лямбде OData-фильтра по вложенной коллекции записей регистра:
# RecordSet/any(r: r/Period ge ...). См. _Window.filter.
RECORD_SET_FIELD = 'RecordSet'
RECORD_SET_LAMBDA = 'r'

# Перепроверка кандидатов на пометку (mark_missing при выгрузке за период): ключи объединяются в
# один $filter через OR. Ограничивает эту строку не 1С, а веб-сервер перед ней, и меряет он БАЙТЫ,
# а не количество ключей — поэтому и бюджет здесь в байтах. Раньше тут стояло фиксированное «20
# ключей на запрос», и на составном ключе регистра (Recorder + LineNumber + Recorder_Type, где
# тип регистратора — длинное кириллическое имя в процентном кодировании) двадцать ключей давали
# далеко за 2048 байт.
#
# Значения по умолчанию у распространённых веб-серверов:
#
#   IIS      maxQueryString 2048 байт, maxUrl 4096 (requestFiltering/requestLimits)
#   Apache   LimitRequestLine 8190 байт (вся строка запроса целиком)
#   nginx    large_client_header_buffers 8k (строка запроса)
#
# Берём самый жёсткий — IIS: под Windows 1С обычно публикуется именно там. Пользователю это
# задавать не нужно: лимит либо совпадает с умолчанием, либо больше него, а меньше не бывает.
RECHECK_MAX_QUERY_BYTES = 2048
# Запас в той же строке на всё, что не $filter: $orderby по полям ключа, само «$filter=» и «?».
RECHECK_QUERY_RESERVE_BYTES = 512

# Страховка на случай, если лимит всё-таки урезали ниже умолчания: получив от сервера «строка
# запроса слишком длинная», бюджет делим и повторяем ту же пачку. Только вниз и до конца процесса —
# как потолок размера страницы. Ниже RECHECK_MIN_QUERY_BYTES не опускаемся: там уже и один ключ не
# всегда влезает, и дальнейшее деление только маскировало бы настоящую причину отказа.
RECHECK_BUDGET_DIVISOR = 2
RECHECK_MIN_QUERY_BYTES = 256

# Коды, которыми веб-серверы отвечают на слишком длинную строку запроса. 414 — стандартный (nginx,
# Apache), а IIS отдаёт 404.15 «Query String Too Long», то есть обычный 404: по одному коду его не
# отличить от опечатки в имени объекта, поэтому 404 засчитываем только вместе с приметой в теле
# (см. _is_query_too_long).
URI_TOO_LONG_CODES = frozenset((414, 404))
URI_TOO_LONG_MARKERS = ('404.15', 'query string', 'строка запроса', 'uri too long')


def _is_permanent_error(exc: BaseException) -> bool:
    """Ошибка, которую ретрай не исправит (см. PERMANENT_HTTP_CODES)."""
    response = getattr(exc, 'response', None)
    status = getattr(response, 'status_code', None)
    return status in PERMANENT_HTTP_CODES


def _is_query_too_long(exc: BaseException) -> bool:
    """
    Отказ «строка запроса слишком длинная». 414 — стандартный код, но IIS отдаёт 404.15 обычным
    404, поэтому 404 засчитываем, только если в теле есть примета: иначе мы приняли бы за него
    опечатку в имени объекта и молча резали бы запросы вдвое до самого дна.
    """
    response = getattr(exc, 'response', None)
    status = getattr(response, 'status_code', None)
    if status not in URI_TOO_LONG_CODES:
        return False
    if status != 404:
        return True
    body = (getattr(response, 'text', '') or '').lower()
    return any(marker in body for marker in URI_TOO_LONG_MARKERS)


def _recorder_type_for_url(field: str, value) -> str:
    """
    Значение поля ключа для прямого адреса (см. DataReader1C.read_by_key). Всё, кроме `<Имя>_Type`,
    идёт как есть; типу возвращается пространство имён, которое разбор снял.

    Снимаем мы только `StandardODATA.` (см. _get_record_fields), и в адресе его действительно надо
    вернуть — без него 1С отвечает 400 «Недопустимое значение … для свойства составного типа». Но
    так называются не все типы: регистратором может быть документ, НЕ опубликованный в этом
    интерфейсе OData, и такой тип приходит уже со своим пространством имён —
    `UnavailableEntities.UnavailableEntity_<guid>`. Ему `StandardODATA.` не нужен, и с ним 1С
    отвечает тем же 400.

    Отличаем по точке: имя объекта 1С — идентификатор, точек в нём нет, поэтому точка в значении
    означает, что пространство имён при нём уже есть. Проверено на живой базе, где в одном регистре
    встретились оба вида.
    """
    if not field.endswith('_Type'):
        return value
    value = str(value)
    return value if '.' in value else f'{ODATA_PREFIX}{value}'


# Проверка параметров конструктора: ошибка в них иначе всплывает далеко от места, где её
# допустили — 404 от 1С посреди цикла, KeyError в чужом коде, а то и молча неверная работа.
# Проверяем на месте вызова и сообщением говорим, что именно передать.
_UUID_RE = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')


def _check_odata_url(odata_url) -> str:
    """URL корня OData: http(s)://host/base/odata/standard.odata (без слеша на конце — к нему
    везде дописывается /<ресурс>)."""
    if not isinstance(odata_url, str) or not odata_url.strip():
        raise ValueError("odata_url is required: URL of the 1C OData root, e.g. "
                         "http://host/base/odata/standard.odata")
    url = odata_url.strip().rstrip('/')
    if not url.startswith(('http://', 'https://')):
        raise ValueError(f"odata_url must start with http:// or https:// (got {odata_url!r})")
    if not url.endswith('/odata/standard.odata'):
        # Не ошибка: адрес публикации бывает нестандартным. Но чаще это забытый хвост пути.
        logger.warning("odata_url %s does not end with /odata/standard.odata — is this the OData "
                       "root and not the base URL?", url)
    return url


def _check_odata_auth(odata_auth):
    """(user, password) либо None. Часто передают одну строку или один элемент — с таким requests
    уходит в 1С без авторизации или падает не по делу."""
    if odata_auth is None:
        return None
    if (isinstance(odata_auth, (tuple, list)) and len(odata_auth) == 2
            and all(isinstance(part, str) for part in odata_auth)):
        return tuple(odata_auth)
    raise ValueError("odata_auth must be a (user, password) tuple of strings, or None for "
                     f"anonymous access (got {odata_auth!r})")


def _check_exchange_name(exchange_name) -> str:
    """Имя плана обмена так, как оно уходит в URL: ExchangePlan_<имя>. Префикс/точечное имя
    (ПланОбмена.Х, ExchangePlan_Х) снимаем: в URL их дописывает сам ChangeReader1C."""
    if not isinstance(exchange_name, str) or not exchange_name.strip():
        raise ValueError("exchange_name is required: name of the 1C exchange plan "
                         "(as in the configuration, e.g. ДляВитрины)")
    name = exchange_name.strip()
    for prefix in ('ExchangePlan_', 'ПланОбмена.', 'ExchangePlan.'):
        if name.startswith(prefix):
            logger.warning("exchange_name %r: dropping the %r prefix, the plain plan name is "
                           "expected", exchange_name, prefix)
            name = name[len(prefix):]
    if '/' in name or ' ' in name:
        raise ValueError(f"exchange_name must be a bare exchange plan name (got {exchange_name!r})")
    return name


def _check_queue_guid(queue_guid) -> str:
    """Ref_Key узла обмена. Пустой — допустим: чтение изменений тогда выведет в лог список узлов
    (см. ChangeReader1C._raise_no_queue_guid). Непустой обязан быть guid: имя или код узла в URL
    даст ответ 1С, по которому это не угадать."""
    if queue_guid is None:
        return ''
    if not isinstance(queue_guid, str):
        raise ValueError(f"queue_guid must be a string Ref_Key of the exchange node (got {queue_guid!r})")
    guid = queue_guid.strip().strip('{}')
    if guid and not _UUID_RE.match(guid):
        raise ValueError(f"queue_guid must be the Ref_Key (guid) of the exchange node, not its "
                         f"code or name (got {queue_guid!r}). Leave it empty to log the list of "
                         f"available nodes")
    return guid


def _check_engine(engine) -> Engine:
    """Готовый Engine, а не строка подключения: пул и опции задаёт вызывающий (см. docstring)."""
    if isinstance(engine, Engine):
        return engine
    if isinstance(engine, str):
        raise ValueError("engine must be a SQLAlchemy Engine, not a connection string: "
                         f"pass create_engine({engine!r})")
    raise ValueError(f"engine must be a SQLAlchemy Engine (got {type(engine).__name__})")


def _check_db_connection(engine: Engine) -> Engine:
    """
    Проверяет, что БД отвечает, — до того, как её тронет первый же компонент (журнал загрузок
    создаёт свою таблицу прямо в конструкторе).

    Смысл в сообщении, а не в проверке: недоступная база даёт полтораста строк трейса сквозь пул
    SQLAlchemy и psycopg2, где полезна ровно одна строка — «хост не резолвится» или «отказано в
    соединении». Для того, кто запускает контейнер, это нечитаемо. Поднимаем ту же ошибку, но с
    коротким текстом и без чужого трейса (`from None`), добавив к ней адрес БД без пароля.

    Проверка одноразовая, на старте: разрыв связи в работающем цикле — дело обычное, его ловит
    run_forever и повторяет с backoff.
    """
    try:
        with engine.connect():
            pass
    except OperationalError as exc:
        # orig — исключение драйвера: у psycopg2 в нём та самая единственная полезная строка.
        reason = str(exc.orig or exc).strip().splitlines()[0] if exc.orig else str(exc)
        url = engine.url.render_as_string(hide_password=True)
        # ConnectionError, а не OperationalError: та печатает себя вместе с SQL-контекстом,
        # которого здесь нет, — соединение не открылось вовсе.
        raise ConnectionError(f"cannot connect to the database {url}: {reason}") from None
    return engine


def _check_db_schema(db_schema):
    """Имя схемы БД либо None (схема по умолчанию). Пустая строка — почти наверняка незаполненная
    переменная окружения, а не осознанный выбор."""
    if db_schema is None:
        return None
    if not isinstance(db_schema, str):
        raise ValueError(f"db_schema must be a schema name string or None (got {db_schema!r})")
    return db_schema.strip() or None


def _check_full_load_workers(full_load_workers) -> int:
    """Число потоков полной выгрузки: >= 1 (0 остановил бы выгрузку молча)."""
    if isinstance(full_load_workers, bool) or not isinstance(full_load_workers, int):
        raise ValueError(f"full_load_workers must be an int >= 1 (got {full_load_workers!r})")
    if full_load_workers < 1:
        raise ValueError(f"full_load_workers must be >= 1 (got {full_load_workers})")
    return full_load_workers


def _check_request_timeout(request_timeout):
    """Таймаут requests: число секунд либо (connect, read). None — значение по умолчанию
    (DEFAULT_REQUEST_TIMEOUT). Явный 0/None внутри кортежа — вечное ожидание, это не таймаут."""
    if request_timeout is None:
        return None
    values = request_timeout if isinstance(request_timeout, (tuple, list)) else (request_timeout,)
    if (len(values) not in (1, 2)
            or not all(isinstance(v, (int, float)) and not isinstance(v, bool) and v > 0
                       for v in values)):
        raise ValueError("request_timeout must be a positive number of seconds or a "
                         f"(connect, read) pair of them (got {request_timeout!r})")
    return tuple(values) if isinstance(request_timeout, (tuple, list)) else request_timeout


def _rows_modified(result) -> int:
    """Сколько строк merge реально изменил. None — save вышел рано (пустой набор, нет метаданных)."""
    if result is None:
        return 0
    return result.inserted_row_count + result.updated_row_count + result.deleted_row_count


def _marked_result(marked: int) -> mergeResult:
    """Результат шага пометки пропавших строк в терминах mergeResult: журнал и сигнал обработчикам
    принимают именно его, а пометка — это ровно то, что dbmerge считает deleted_row_count."""
    return mergeResult(total_row_count=marked, inserted_row_count=0, updated_row_count=0,
                       deleted_row_count=marked, total_time=0.0, temp_insert_time=0.0,
                       insert_time=0.0, update_time=0.0, delete_time=0.0)


def _log_failure(exc: BaseException, message: str, *args) -> None:
    """
    Пишет в лог падение цикла. Для ошибок обмена traceback не нужен: он целиком состоит из
    внутренностей requests и ничего не добавляет к описанию, которое пришло от 1С.

    - HTTPError: описание уже выведено raise_for_status строкой выше, не дублируем;
    - прочие ошибки requests (таймаут, обрыв): traceback не нужен, но текст выводим — его нигде нет;
    - OperationalError от БД (упала, перезапустилась, кончились соединения): то же самое, полезна
      строка драйвера, а не сто строк внутренностей SQLAlchemy;
    - остальное: это уже похоже на ошибку в коде, traceback оставляем.
    """
    if isinstance(exc, requests.HTTPError):
        logger.error(message, *args)
    elif isinstance(exc, requests.RequestException):
        logger.error(f'{message}: %s', *args, exc)
    elif isinstance(exc, OperationalError):
        logger.error(f'{message}: %s', *args, str(exc.orig or exc).strip().splitlines()[0])
    else:
        logger.exception(message, *args)


def _load_mode_tag(mode: str):
    """
    Помечает режимом загрузки все сообщения лога cdc_1c, выданные внутри метода: полная выгрузка
    идёт фоновыми потоками параллельно с чтением изменений, и в общем логе иначе не разобрать, к
    чему относится строка. Декоратором, а не блоком with — чтобы не заворачивать тело целиком.
    """
    def decorator(method):
        @functools.wraps(method)
        def wrapper(self, *args, **kwargs):
            with load_mode(mode):
                return method(self, *args, **kwargs)
        return wrapper
    return decorator


def _and_filters(*parts: str | None) -> str | None:
    """Склейка OData-фрагментов $filter по AND; None и пустые пропускаются."""
    clauses = [part for part in parts if part]
    if not clauses:
        return None
    return " and ".join(clauses)


class _Window:
    """
    Окно периода [start, end) полной выгрузки: за него отвечает один $filter, внутри окна страницы
    берутся обычным способом. Границы — полные datetime, без округления до суток: и `Дата`
    документа, и `Период` записи регистра хранят время, и обрезав его до полуночи мы получили бы
    либо дыру, либо перечитывание.

    Любая граница может быть None — это «до бесконечности» в соответствующую сторону:

    - end=None у самого свежего окна: документ, созданный уже во время прогона, попадает в него, а
      не остаётся за краем. Туда же попадает и дата в будущем — редкость, но своё окно у неё есть;
    - start=None у хвостового окна: им добирается вся оставшаяся история одним сплошным $skip,
      когда границу снизу спросить не у кого (см. FULL_LOAD_EMPTY_WINDOWS_TO_STOP).

    Окна строятся строго встык (end одного = start следующего), поэтому пропустить между ними
    ничего нельзя, чем бы ни было время в самих датах.

    record_set=True — регистр, подчинённый регистратору. У него entry — это НАБОР записей
    регистратора, и поля `Period` на верхнем уровне нет вовсе: она лежит внутри вложенной
    коллекции RecordSet. Поэтому фильтр строится через лямбду `RecordSet/any(r: r/Period ...)`, а
    не плоским сравнением — плоское 1С отвергает с 400 «Сегмент пути Period не найден!».
    Набор при этом отбирается ЦЕЛИКОМ, если в окно попала хоть одна его запись, — что как раз и
    нужно: страница обязана содержать набор целиком, иначе scoped-удаление снесёт его остаток.
    """

    def __init__(self, date_field: str, start: datetime | None, end: datetime | None, *,
                 record_set: bool = False):
        self.date_field = date_field
        self.start = start
        self.end = end
        self.record_set = record_set

    @property
    def filter(self) -> str | None:
        """OData $filter окна; None у окна без обеих границ (весь объект — фильтровать нечего)."""
        field = (f'{RECORD_SET_LAMBDA}/{self.date_field}' if self.record_set else self.date_field)
        clauses = []
        if self.start is not None:
            clauses.append(f"{field} ge {Replicator1C._odata_datetime(self.start)}")
        if self.end is not None:
            clauses.append(f"{field} lt {Replicator1C._odata_datetime(self.end)}")
        if not clauses:
            return None
        expression = " and ".join(clauses)
        if not self.record_set:
            return expression
        return f"{RECORD_SET_FIELD}/any({RECORD_SET_LAMBDA}: {expression})"

    @property
    def title(self) -> str:
        """Окно для лога. Время показываем всегда: границы редко приходятся на полночь, и без него
        два соседних окна выглядели бы одинаково."""
        fmt = '%Y-%m-%d %H:%M:%S'
        left = f"{self.start:{fmt}}" if self.start is not None else '-inf'
        right = f"{self.end:{fmt}}" if self.end is not None else '+inf'
        return f"[{left} .. {right})"


class Replicator1C:
    """
    Оркестратор CDC: читает изменения из 1С (OData) и сохраняет их в БД, подтверждая получение
    только после успешного сохранения.

    Компоненты (MetadataReader1C / ChangeReader1C / NameMapper1C / DBWriter1C) строятся в
    конструкторе, но без обращения к сети: MetadataReader1C создаётся пустым, а фактическая
    загрузка метаданных (сетевой запрос) откладывается до первого run_once (по флагу
    metadata.is_loaded). Поэтому недоступность 1С на старте не роняет конструктор — ошибка загрузки
    всплывает уже в run_once: в run_forever она попадает в его try/except и повторяется, а в
    одиночном run_once пробрасывается (это нормально).

    Принимает отдельные аргументы, а не объект настроек: параметры присваиваются явно и на месте
    вызова видно, что именно передано — одинаково и в python-приложении с литералами, и в
    контейнере, где значения берутся из окружения. БД передаётся готовым engine: пользователь сам
    управляет пулом и опциями, а тот же engine прокидывается в DBWriter1C.
    """

    def __init__(self, odata_url: str, odata_auth: tuple[str, str] | None,
                 exchange_name: str, queue_guid: str,
                 engine: Engine, db_schema: str | None = None,
                 db_temp_schema: str | None = None,
                 request_timeout: float | None = None,
                 full_load_workers: int = 2):
        # Включаем вывод логов, если приложение не настроило логирование само.
        _ensure_handler()
        # Перехват SIGTERM/SIGINT ставим здесь, а не при запуске цикла: run_forever типовая точка
        # входа отправляет в пул потоков, а из рабочего потока перехват поставить нельзя (см.
        # stop_signal). Конструктор же вызывается из главного.
        install_signal_handlers(quiet=False)

        # Параметры проверяем здесь, а не по месту использования: неверный адрес или не тот guid
        # иначе оборачиваются ошибкой 1С посреди первого цикла, где уже не видно, что не так.
        self.engine = _check_db_connection(_check_engine(engine))
        self.db_schema = _check_db_schema(db_schema)
        # Схема промежуточных таблиц dbmerge. Не задана — та же, что у данных. Отдельная схема
        # (например cdc_1c_tmp) держит их в стороне от таблиц с данными: в ней по определению нет
        # ничего ценного, поэтому таблицу, оставшуюся после падения процесса, там видно и не жалко.
        self.db_temp_schema = _check_db_schema(db_temp_schema)
        self._odata_url = _check_odata_url(odata_url)
        self._exchange_name = _check_exchange_name(exchange_name)
        self._queue_guid = _check_queue_guid(queue_guid)
        # odata_auth — кортеж (user, password) либо None, как в ридерах (передаётся им как есть).
        self._odata_auth = _check_odata_auth(odata_auth)
        # None → таймаут не задан явно: ридеры подставят DEFAULT_REQUEST_TIMEOUT (metadata_reader).
        self._request_timeout = _check_request_timeout(request_timeout)

        # Компоненты строятся сразу, но в сеть не ходят: MetadataReader1C создаётся пустым,
        # метаданные подгрузятся лениво при первом run_once. MetadataReader1C получает engine —
        # он же ведёт реестр объектов metadata_objects_1c (состояние полной выгрузки).
        self.metadata = MetadataReader1C(self._odata_url, odata_auth=self._odata_auth,
                                         request_timeout=self._request_timeout,
                                         engine=self.engine, schema=self.db_schema,
                                         temp_schema=self.db_temp_schema)
        self.name_mapper = NameMapper1C()

        # Фоновая полная выгрузка: пул потоков.
        self._full_load_workers = _check_full_load_workers(full_load_workers)
        # Размер страницы, который 1С реально осилила по этому объекту (см. FULL_LOAD_MIN_BATCH).
        # Пишет только поток самой выгрузки, а он на объект один (захват не пускает второго).
        self._full_load_page_size: dict[str, int] = {}
        # Потолок размера страницы по объекту: ставится отказом 1С и только опускается. Нужен
        # потому, что подбор по весу ответа (_next_page_size) причину отказа не видит и вернул бы
        # размер обратно — см. _load_pages.
        self._full_load_page_limit: dict[str, int] = {}
        # Бюджет длины $filter перепроверки, в байтах. Считается от умолчания самого жёсткого
        # веб-сервера и только опускается — если тот всё-таки ответил «слишком длинно»
        # (см. RECHECK_MAX_QUERY_BYTES и _still_in_1c). Общий на все объекты: ограничение стоит
        # перед 1С, а не внутри неё, и от объекта не зависит.
        self._recheck_query_budget = RECHECK_MAX_QUERY_BYTES - RECHECK_QUERY_RESERVE_BYTES
        # Захват объекта под выгрузку (см. full_load_claim): не пускает второго ни в этом процессе,
        # ни в чужом. Репликатор и расписание могут быть подняты в разных контейнерах, поэтому
        # заслон только один и только в БД — множество в памяти их бы не развело.
        self._full_load_claim = FullLoadClaim(
            engine, lambda: self.metadata.objects_table,
            owner=f'{exchange_name}:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}')
        self.changes = ChangeReader1C(self._odata_url, self._exchange_name, self._queue_guid,
                                      self.metadata, odata_auth=self._odata_auth,
                                      request_timeout=self._request_timeout)
        self.writer = DBWriter1C(engine=self.engine, name_mapper=self.name_mapper,
                                 schema=self.db_schema, temp_schema=self.db_temp_schema)
        # Лог загрузки (строка на объект) пишет оркестратор: только здесь есть контекст обмена
        # (exchange_name/message_no), а writer универсален и может делать и полную перевыгрузку.
        self.replicator_log = Replicator1CLog(self.engine, self.db_schema)

        # Реестр идущих merge — в БД, а не в памяти: обработчик может считать витрину в другом
        # процессе, и границу своего окна он обязан прижимать к НАШИМ незавершённым merge.
        self.writes = WriteTracker(self.engine, self.db_schema, self._exchange_name)
        # Сигналы обработчикам идут через handlers_1c, а не через объекты: репликатору не нужны
        # ни их код, ни общий с ними процесс (см. HandlerSignals).
        self.handler_signals = HandlerSignals(self.engine, self.db_schema)
        # Действующий StopSignal текущего run_forever — через него цикл останавливают снаружи,
        # когда он крутится не в главном потоке и своего перехвата сигналов не имеет.
        self._stop_signal: "StopSignal | None" = None


    @_load_mode_tag(LOAD_MODE_CHANGES)
    def run_once(self, notify_changes: bool = True) -> None:
        """
        Один цикл: (load metadata при первом вызове) → read → save → notify. Подтверждение
        получения отправляется только после успешного сохранения — если save упадёт, изменения
        не подтверждаются и придут снова.

        Метаданные грузятся при первом вызове (первый сетевой запрос). В run_forever его падение
        ловится и повторяется; в одиночном run_once — пробрасывается.

        Если изменений не было, notify не шлём: незачем подтверждать и двигать счётчик пакета
        обмена на пустом пакете.

        notify_changes=False отключает подтверждение совсем: изменения остаются в очереди обмена
        1С (полезно для отладки/тестов — цикл становится повторяемым).

        """
        # Первый вызов: грузим метаданные. Дальше не перечитываем — это делает сам data_reader
        # при появлении нового объекта/поля (get_metadata держит is_loaded=True).
        if not self.metadata.is_loaded:
            self.metadata.get_metadata()
        # Время пакета считаем от чтения из 1С и до конца всех merge — это то, что реально
        # занимает цикл. Загрузка метаданных сюда не входит: она разовая и к пакету не относится.
        started = time.monotonic()
        self.changes.read_changes()
        self._save_changes()
        logger.info("Changes package %s processed in %s: %s rows",
                    self.changes.message_no, format_duration(time.monotonic() - started),
                    self.changes.rows_read())
        # Объект пришёл в пакете → он в плане обмена. Если ни разу не выгружался целиком,
        # помечаем на полную выгрузку (выполнит фоновый воркер в run_forever). 
        # Табличные части пропускаем: они догружаются вместе с владельцем при его full_load,
        # приезжая вложенными в его entry. Отдельная сущность в OData у них есть, но грузить их
        # ею незачем и дороже — страница владельца приносит его табличные части целиком.
        for object_full_name in self.changes:
            metadata_obj = self.metadata.get(object_full_name)
            if metadata_obj is None:
                self.metadata.get_metadata()
                metadata_obj = self.metadata.get(object_full_name)
                if metadata_obj is None:
                    raise RuntimeError(f"No metadata object for {object_full_name}")

            if metadata_obj.is_table_part:
                continue

            self.metadata.require_full_load_if_new(object_full_name)
        
        if notify_changes and len(self.changes) > 0:
            self.changes.notify_changes_received()
        else:
            logger.debug("No changes — skipping confirmation")


    def _save_changes(self) -> None:
        """
        Сохраняет объекты пакета по одному, записывая лог загрузки на каждый объект
        (replicator_1c_log). finish() — только после успешного save: упавший объект остаётся с
        finished_at=NULL и не двигает границу окна обработчика. Лог здесь, а не в DBWriter1C,
        потому что только тут есть контекст обмена (exchange_name/message_no).

        Порядок сохранения — справочники → документы → регистры (save_order_key): документы ссылаются
        на справочники, регистры — на документы, поэтому родителей пишем раньше. Внутри группы
        исходный порядок пакета (сортировка стабильна).
        """
        for object_name, data_object in sorted(self.changes.items(),
                                               key=lambda kv: save_order_key(kv[0])):
            log_id = self.replicator_log.start(
                self.changes.exchange_name, object_name, self.changes.message_no, LOAD_TYPE_CHANGES)
            table_name = self._handler_key(object_name)
            with self.writes.track(table_name):
                result = self.writer.save(object_name, data_object)
            # Одно сохранение на строку лога: счётчики и завершение — одним запросом.
            self.replicator_log.write_result(log_id, result, finish=True)
            self._signal_handlers(table_name, result, SOURCE_CHANGES)

    def _handler_key(self, object_name: str) -> str:
        """
        Имя таблицы объекта в БД — под ним объект и известен обработчикам (см. Handler1C.ON).
        Подписка идёт по имени таблицы, а не по имени объекта 1С, потому что обработчик пишет SQL
        по таблицам: имя 1С он в глаза не видит, а транслит стоит у него в запросе.
        """
        return self.name_mapper.map_object_name(object_name)

    def _signal_handlers(self, table_name: str, result, source: str) -> None:
        """
        Сообщает обработчикам, что таблица изменилась — но только если merge реально что-то сделал.
        1С регистрирует изменение объекта на любую перезапись, и в пакет приезжает масса записей,
        идентичных тому, что уже лежит в БД (шумные поля при сравнении не учитываются, см.
        DBWriter1C._noisy_fields). Обработчик всё равно выбирает данные сам, и на пустом прогоне
        его SELECT вернул бы пусто — вызывать незачем.

        Сообщаем через БД: ставим метку update_requested_at в handlers_1c тем, у кого эта таблица есть
        в update_on. Ни объектов обработчиков, ни их кода репликатору для этого не нужно, поэтому
        они могут работать в другом процессе или контейнере.

        Появление новой колонки — отдельный случай: значения строк оно не меняет, merged_on не
        двигает, и окно обработчика её не увидит никогда. Поэтому на added_fields подписчики
        таблицы отправляются пересобирать витрину целиком.
        """
        if result is None:
            return
        if result.added_fields:
            self.handler_signals.request_full_rebuild(
                table_name, f"{table_name} gained columns {sorted(result.added_fields)}", source)
        if _rows_modified(result) > 0:
            self.handler_signals.signal(table_name, source)

    def list_objects(self) -> list[str]:
        """
        Список имён объектов 1С, доступных для выгрузки (документы/справочники и регистры).
        Табличные части исключаются: они приходят вложенно с владельцем и грузятся вместе с ним.
        Отдельная сущность в OData у них есть, и full_load её примет, но выгружать табличную часть
        отдельно не нужно — а страницей меньше её группы ещё и вредно (см. DataReader1C.read_object).
        Метаданные при необходимости подгружаются (первый сетевой запрос). Удобно, чтобы узнать,
        что передавать в full_load.

        Имена — как в 1С (кириллица). В full_load годится и такое имя, и имя таблицы в БД: он
        принимает обе формы (см. MetadataReader1C.resolve_object_name). Список имён таблиц лежит в
        реестре metadata_objects_1c, колонка object_full_name_en.
        """
        if not self.metadata.is_loaded:
            self.metadata.get_metadata()
        return [name for name, obj in self.metadata.items() if not obj.is_table_part]

    @_load_mode_tag(LOAD_MODE_FULL)
    def full_load(self, object_name: str, batch_size: int = 1000,
                  date_field: str | None = None,
                  date_from: date | datetime | str | None = None,
                  date_to: date | datetime | str | None = None,
                  mark_missing: bool = True) -> int:
        """
        Полная постраничная выгрузка объекта 1С в целевую таблицу: страницами, размер которых
        подбирается по их весу (batch_size — лишь верхняя граница, см. ниже), и каждая страница
        сразу сохраняется через writer.save(full_load_started_at=...). Идемпотентно — повторный
        прогон обновляет строки по ключу. Документ/справочник — чистый upsert; регистр/табличная часть —
        own-or-skip группы целиком (группа умещается на одной странице), см. DBWriter1C.save.

        Документ/справочник выгружается вместе с табличными частями — они приходят вложенно в той же
        странице и сохраняются как отдельные объекты. Сортировка страниц — по первичному ключу
        (см. _full_load_key), переход к следующей странице — смещением $skip: курсора по ключу 1С
        не даёт, потому что в ключе стоит ссылка (см. DESIGN.md, «Почему страницы берутся только
        через $skip»). Глубину лечит не курсор, а нарезка окнами по периоду (см. ниже).
        Один прогон = одна строка в replicator_1c_log (message_no=NULL — это не пакет обмена);
        finished_at проставляется после успеха всех страниц.

        batch_size — верхняя граница, а не жёсткий размер. Реальный размер страницы подбирается по
        её весу (см. _next_page_size): первая страница пробная, дальше столько записей, сколько
        укладывается в FULL_LOAD_TARGET_BYTES. Если 1С всё же не осилила страницу (500), размер
        уменьшается и запрос повторяется с того же места (см. FULL_LOAD_BATCH_DIVISOR).

        Глубокий объект (не дочитался за FULL_LOAD_PARTITION_MAX_PAGES страниц) перечитывается
        ОКНАМИ ПО ПЕРИОДУ — от свежих к старым, окнами в днях, см. _load_by_windows. У документа
        границы периода точные, у регистра, подчинённого регистратору, их не спросить, и конец
        истории нащупывается пустыми окнами.

        Гонка с изменениями: в save уходит full_load_started_at — отметка, взятая на КАЖДУЮ
        страницу по часам БД, и не «сейчас», а граница по реестру незавершённых merge
        (WriteTracker.boundary, см. _load_pages). Снимок не трогает строки, переписанные уже после
        этой отметки, и не воскрешает удалённые за это время строки групп (регистр/ТЧ). Всё, что
        старше, снимок перезаписывает: полная выгрузка остаётся способом выровнять данные.
        См. DBWriter1C.save. Отдельно от этого берётся started_at прогона (writer.db_now()) — он
        нужен только пометке пропавших строк (mark_missing).

        Необязательный фильтр по периоду: date_field — имя поля даты/времени объекта (Date у
        документов, Period у регистров), date_from/date_to — границы (datetime/date/ISO-строка,
        включительно). Транслируется в OData $filter `date_field ge …` + `le`/`lt` для верхней
        границы (чистая дата включает весь день целиком; у регистра, подчинённого регистратору,
        всё это оборачивается в лямбду по вложенной коллекции — см. _build_date_filter).
        Полезно для ручной догрузки за нужный период.

        mark_missing — пометить строки, которых в 1С не оказалось (см. full_load_keys). Нужно
        затем, что физическое удаление объекта в обмен не приходит вовсе, и такая строка иначе
        остаётся в таблице навсегда. Ключи прогона копятся в отдельной таблице, и после успешного
        завершения строки, которых там нет, помечаются (is_deleted_or_empty), а не удаляются:
        обработчик замечает изменение только по merged_on.

        ПО УМОЛЧАНИЮ ВКЛЮЧЕНО. Выключать стоит осознанно: без пометки удалённые строки остаются в
        таблице навсегда, а витрина, построенная поверх, не увидит удаления вообще — и разойдётся
        с источником молча. Плата за пометку — таблица ключей на прогон и один UPDATE в конце.

        Область пометки — то, что прогон читал: выгрузка за период помечает только строки этого
        периода (см. _marking_scope). Внутри неё каждый кандидат перед пометкой ещё и
        перепроверяется запросом в 1С: из окна строка могла не исчезнуть, а уехать (у документа
        изменилась дата, у независимого регистра — поле ключа).

        Возвращает число РЕАЛЬНО изменённых строк (вставлено + обновлено + удалено, по всем
        страницам и вложенным объектам). Это проверка самого CDC: если изменения доезжают исправно,
        выгрузка находит ровно то, что уже лежит в БД, и ответ должен быть 0.
        """
        if not self.metadata.is_loaded:
            self.metadata.get_metadata()

        # Имя объекта и имя поля даты принимаются в ОБЕИХ формах — как в 1С и как в БД
        # (`Document_ЗаказКлиента` / `Document_ZakazKlienta`, `Дата` / `Data`), см.
        # MetadataReader1C.resolve_object_name. Ровно то же делают FullLoadCron и Handler1C.ON:
        # настраивая выгрузку, смотрят в базу, а не в конфигуратор.
        object_name = self.metadata.resolve_object_name(object_name)
        if date_field:
            date_field = self.metadata.resolve_field_name(object_name, date_field)

        # Ключ сортировки: справочник/документ → [Ref_Key], регистраторный → [Recorder]/
        # [Recorder_Key], независимый регистр → весь первичный ключ (составной ключ).
        key_fields = self._full_load_key(object_name)
        # Регистр бухгалтерии читается из виртуальной таблицы (только там есть субконто), а у неё
        # ни $skip, ни сортировки: страницы идут курсором по периоду, см. _load_pages.
        by_period = self._is_accounting_register(object_name)
        date_filter = self._build_date_filter(object_name, date_field, date_from, date_to)

        reader = DataReader1C(self._odata_url, self.metadata, odata_auth=self._odata_auth,
                              request_timeout=self._request_timeout)
        # Полная выгрузка = базовая версия: emn=0 (ниже любого номера пакета изменений >=1).
        reader.exchange_message_no = 0

        # Момент старта прогона по часам БД: по нему помечаются пропавшие строки в конце прогона
        # (guard'ы save берут свою отметку на каждую страницу, см. ниже).
        started_at = self.writer.db_now()

        # Поле, по которому объект можно порезать на периоды, если он окажется глубоким. Нужно
        # везде, где страницы берутся через $skip, то есть всюду, кроме регистра бухгалтерии:
        # его курсор по периоду в глубину не уходит (смещения нет вовсе), и окна ему не нужны —
        # точнее, он и так читается только окнами.
        partition_field = (None if by_period
                           else self._partition_date_field(object_name, date_field))

        log_id = self.replicator_log.start(self._exchange_name, object_name, None, LOAD_TYPE_FULL)
        logger.info("Full load of %s started (batch_size=%s, key=%s, paging=%s, date_filter=%s, "
                    "partition_field=%s)", object_name, batch_size, key_fields,
                    'period' if by_period else 'skip',
                    date_filter, partition_field)
        total = 0
        rows_modified = 0
        # Ключи прогона: собираются постранично, в конце по ним помечаются пропавшие строки.
        # ExitStack — чтобы одноразовая таблица гарантированно удалилась и при ошибке прогона.
        keys = self._full_load_keys(object_name) if mark_missing else None
        stack = ExitStack()
        with stack:
            if keys is not None:
                stack.enter_context(keys)
            page_args = dict(reader=reader, key_fields=key_fields, by_period=by_period,
                             batch_size=batch_size, keys=keys, log_id=log_id)
            # Читался ли объект окнами по дате. Важно для пометки пропавших строк: окно
            # порождает «уехавшие» строки (см. _mark_missing_rows), и неважно, задал его
            # пользователь или обход окнами выбрал сам.
            windowed = False

            # Сначала читаем объект как есть — без окон и без лишних запросов. Мелкому объекту
            # (а таких большинство) окна только вредят: он укладывается в пару страниц, а за
            # обход пришлось бы заплатить запросом на каждое окно истории, даже пустое.
            # Лимит страниц ставим, только если резать вообще есть по чему.
            if by_period:
                # Регистр бухгалтерии читается ТОЛЬКО окнами: у виртуальной таблицы нет ни
                # смещения, ни сортировки, а Top отдаёт произвольное подмножество выборки, а не
                # её начало (проверено: Top=20 вернул двадцать движений вразброс по всей истории).
                # Значит, единственный способ прочитать объект целиком и ничего не потерять —
                # разбить время на отрезки и взять каждый отрезок ЦЕЛИКОМ.
                windowed = True
                records, modified = self._load_by_windows(
                    object_name, reader=reader, date_field=PERIOD_FIELD,
                    date_filter=date_filter, page_args=page_args)
                total += records
                rows_modified += modified
                exhausted = True
            else:
                records, modified, exhausted = self._load_pages(
                    object_name, extra_filter=date_filter, **page_args,
                    max_pages=FULL_LOAD_PARTITION_MAX_PAGES if partition_field else None)
                total += records
                rows_modified += modified

            if not exhausted:
                # Объект глубокий: $skip уже уходит далеко, и дальше цена растёт квадратично —
                # 1С на каждый запрос строит выборку заново, сортирует и отбрасывает первые N
                # строк. Перечитываем его окнами по периоду: фильтр по дате переводит запрос на
                # индекс (Дата у документа, Период у регистра входят в него), и сортируется
                # маленький кусок. Прочитанные страницы перезапишутся теми же значениями —
                # выгрузка идемпотентна, и это дешевле, чем гадать о размере объекта заранее
                # ($count 1С не отдаёт).
                logger.info("Full load of %s: deep object (> %s pages), re-reading it by %s "
                            "windows of %s days", object_name, FULL_LOAD_PARTITION_MAX_PAGES,
                            partition_field, FULL_LOAD_WINDOW_DAYS)
                windowed = True
                records, modified = self._load_by_windows(
                    object_name, reader=reader, date_field=partition_field,
                    date_filter=date_filter, page_args=page_args)
                total += records
                rows_modified += modified

            if keys is not None:
                # Пометка — только здесь, после последней страницы: прогон, упавший на середине,
                # объявил бы «пропавшим» весь непрочитанный хвост объекта.
                rows_modified += self._mark_missing_rows(
                    object_name, keys, started_at, reader,
                    recheck=date_filter is not None or windowed, log_id=log_id,
                    date_column=(self.name_mapper.map_field_name(date_field)
                                 if date_field else None),
                    date_from=date_from, date_to=date_to)
        self.replicator_log.write_result(log_id, finish=True)
        logger.info("Full load of %s finished (%s records, %s rows modified)",
                    object_name, total, rows_modified)
        return rows_modified


    def _load_pages(self, object_name: str, *, reader: DataReader1C, key_fields: list[str],
                    extra_filter: str | None, batch_size: int, keys, log_id,
                    max_pages: int | None, by_period: bool = False) -> tuple[int, int, bool]:
        """
        Постраничное чтение одной выборки (объект целиком либо его окно по периоду) с записью
        каждой страницы. Возвращает (сколько записей прочитано, сколько строк изменено, дочитано ли
        до конца).

        max_pages ограничивает число страниц: превышение означает «выборка слишком глубокая», и
        вызывающий режет её на меньшие периоды (см. full_load). None — читать до конца.

        by_period — регистр бухгалтерии: читается из виртуальной таблицы, где нет ни $skip, ни
        сортировки, и курсором служит сам ПЕРИОД (см. DataReader1C.read_accounting_register).
        """
        skip = 0
        total = 0
        rows_modified = 0
        pages = 0
        # Начинаем с размера, подобранного по этому объекту раньше, иначе — с пробной страницы.
        page_size = min(batch_size,
                        self._full_load_page_size.get(object_name, FULL_LOAD_PROBE_BATCH),
                        self._full_load_page_limit.get(object_name, batch_size))
        while True:
            # Отметка берётся на КАЖДУЮ страницу, а не одна на прогон. Guard'ы снимка проверяют
            # «строку не переписали после того, как мы прочитали эти данные», и точка отсчёта у
            # них — момент чтения страницы. С одной отметкой на прогон группа, разрезанная
            # границей страниц (табличная часть одного владельца), блокировала сама себя:
            # строки, записанные предыдущей страницей, выглядели как чужое свежее изменение,
            # и остаток группы молча не вставлялся (87 строк в 1С → 84 в БД).
            #
            # Не «сейчас», а граница по реестру незавершённых merge — та же, к которой
            # прижимается окно обработчика (WriteTracker.boundary). merged_on штампуется ВНУТРИ
            # merge-транзакции, а коммитится позже: чужой merge, начавшийся до нашего чтения и
            # закоммиченный после, оставил бы merged_on левее «сейчас», guard счёл бы строку
            # старой, и снимок затёр бы свежее изменение. Брошенные строки реестра границу не
            # держат — их отсекает отметка живости (MERGE_HEARTBEAT_TTL), поэтому умерший процесс
            # тормозит выгрузку максимум на TTL, а не навсегда. Свои же записи помехой не
            # становятся: строка реестра живёт до коммита, а страницы пишутся последовательно —
            # к этому моменту нашей строки в реестре уже нет.
            # Таблицы объекта считаются на страницу, а не один раз на прогон: табличная часть
            # может появиться в метаданных уже по ходу выгрузки (их перечитывает data_reader), и
            # тогда её merge не держал бы границу. Обращение локальное, в сеть не ходит.
            page_started_at = self.writes.boundary(self._full_load_tables(object_name))
            try:
                if by_period:
                    # Окно берётся ЦЕЛИКОМ, одним запросом: страницу внутри него не отрезать
                    # ничем — Top отдаёт произвольное подмножество, а не начало выборки
                    # (см. read_accounting_register).
                    page = reader.read_accounting_register(object_name, condition=extra_filter)
                else:
                    page = reader.read_object(object_name, top=page_size, key_fields=key_fields,
                                              extra_filter=extra_filter, skip=skip)
            except requests.HTTPError as exc:
                if by_period:
                    # Окно не по зубам серверу 1С. Уменьшать тут нечего — размер выборки задаёт
                    # только ширина окна, поэтому отдаём «не дочитано» и вызывающий сузит окно.
                    if _is_permanent_error(exc):
                        raise
                    logger.warning("Full load of %s: window failed, narrowing it (%s)",
                                   object_name, exc)
                    return total, rows_modified, False
                # Страница не по зубам серверу 1С (упирается в память/временные файлы) —
                # уменьшаем её и повторяем с того же места. Смещение не сдвигалось.
                if _is_permanent_error(exc) or page_size <= FULL_LOAD_MIN_BATCH:
                    raise
                page_size = max(FULL_LOAD_MIN_BATCH, page_size // FULL_LOAD_BATCH_DIVISOR)
                # Потолок, а не просто новый размер. Подбор по весу (_next_page_size) считает
                # страницу из БАЙТОВ ответа, а 1С падает не только от них: толстый документ валит
                # сборку во временных файлах, отдав перед этим лёгкий ответ. Без потолка первая же
                # удавшаяся страница вернула бы размер к batch_size — и следующий запрос снова лёг
                # бы: 500 → уменьшили → успех → вернулись → 500. Потолок только опускается и живёт
                # столько же, сколько подобранный размер, — до конца процесса.
                self._full_load_page_limit[object_name] = page_size
                self._full_load_page_size[object_name] = page_size
                logger.warning("Full load of %s: page failed, retrying with batch_size=%s "
                               "(and not going above it again)", object_name, page_size)
                continue
            if keys is not None:
                # Ключи страницы — до сохранения: если save упадёт, прогон не закончится и
                # пометки не будет вовсе, а лишние ключи в одноразовой таблице никому не мешают.
                keys.add(self._page_keys(object_name, reader))
            for obj_name, data_object in reader.items():
                # Много страниц/объектов пишутся в одну строку лога — счётчики суммируются в БД.
                table_name = self._handler_key(obj_name)
                with self.writes.track(table_name):
                    result = self.writer.save(obj_name, data_object,
                                              full_load_started_at=page_started_at)
                self.replicator_log.write_result(log_id, result)
                rows_modified += _rows_modified(result)
                # Сигнал на каждую страницу, а не один в конце прогона: это метка времени в
                # одной колонке (handlers_1c.update_requested_at), а не очередь событий, — тысяча
                # страниц тысячу раз перепишет ту же метку, а не выстроит тысячу вызовов. Зато
                # витрина начинает наполняться после первой же страницы, а не через часы, когда
                # выгрузка закончится.
                self._signal_handlers(table_name, result, SOURCE_FULL_LOAD)
            total += page
            pages += 1
            if by_period:
                # Окно прочитано целиком — «следующей страницы» у него не бывает.
                break
            if page < page_size:
                break
            if max_pages is not None and pages >= max_pages:
                # Дочитать можно и так, но дальше $skip уходит в глубину — пусть вызывающий
                # порежет выборку на меньшие периоды.
                return total, rows_modified, False
            skip += page
            page_size = self._next_page_size(object_name, page_size, page,
                                             reader.last_response_bytes, batch_size)
        return total, rows_modified, True

    def _partition_date_field(self, object_name: str, date_field: str | None) -> str | None:
        """
        Поле, по которому режем выгрузку на периоды: заданное пользователем либо угаданное по
        метаданным — Date у документа, Period у регистра. У справочника даты нет вовсе, и это
        нормально: он и не растёт так, чтобы $skip стал проблемой.

        Имя проверяем по метаданным, а не по классу объекта: набор полей 1С отдаёт в $metadata,
        и опираться на него надёжнее, чем на разбор имени.
        """
        properties = self.metadata.get(object_name) or {}
        if date_field:
            return date_field
        for candidate in PARTITION_DATE_FIELDS:
            if properties.get(candidate) == 'DateTime':
                return candidate
        return None

    def _load_by_windows(self, object_name: str, *, reader: DataReader1C, date_field: str,
                         date_filter: str | None, page_args: dict) -> tuple[int, int]:
        """
        Перечитывает объект ОКНАМИ ПО ПЕРИОДУ, от свежих к старым. Возвращает (прочитано записей,
        изменено строк).

        Окна отмеряются в днях (FULL_LOAD_WINDOW_DAYS), а не календарными месяцами: календарь тут
        ни при чём, размер окна выбирается по глубине $skip, а не по названию месяца. Границы —
        полные datetime: и `Дата` документа, и `Период` записи регистра хранят время, и окно,
        обрезанное до полуночи, либо оставило бы дыру, либо заставило перечитывать сутки.

        Порядок обхода:

        1. Открытое окно вверх `[anchor, +inf)`. Им забираются даты в будущем (редкость, но своя
           у них быть должна) и всё, что создаётся уже во время прогона.
        2. Вниз окнами `[cursor - window, cursor)`, встык: end одного окна = start следующего,
           поэтому пропустить между ними нельзя ничего.
        3. Хвост `(-inf, cursor)` одним сплошным $skip — как только подряд попалось
           FULL_LOAD_EMPTY_WINDOWS_TO_STOP пустых окон (см. ниже).

        Окно, упёршееся в лимит страниц, СУЖАЕТСЯ (делится на FULL_LOAD_WINDOW_DIVISOR) и
        перечитывается с того же места — заново, а не с середины: страницы упорядочены по ключу, а
        не по дате, и какие строки уже прочитаны, в терминах периода неизвестно. Дойдя до
        FULL_LOAD_WINDOW_MIN_DAYS, окно читается без лимита страниц: дробить дальше бессмысленно.
        Обратно окно не растёт (см. FULL_LOAD_WINDOW_DAYS).

        Где остановиться — зависит от того, отдаёт ли 1С границы периода (_supports_date_bounds):

        - документ: границы точные. `anchor` берётся у самой поздней даты, поэтому пустой промежуток
          между ней и «сейчас» не перебирается окнами впустую, а обход заканчивается ровно на окне,
          накрывшем самую раннюю дату, — хвост не нужен;
        - регистр в режиме набора записей: границ нет, `$orderby` по дате платформа молча
          игнорирует. `anchor` — «сейчас», а конец истории нащупывается пустыми окнами:
          FULL_LOAD_EMPTY_WINDOWS_TO_STOP подряд означают, что данных ниже, скорее всего, не
          осталось. «Скорее всего» — поэтому остаток и добирается хвостовым окном, а не
          отбрасывается: дыра в истории длиннее трёх окон иначе стоила бы потерянных строк.
        """
        total = 0
        rows_modified = 0

        def read(start, end, *, max_pages):
            nonlocal total, rows_modified
            window = self._window(object_name, date_field, start, end)
            records, modified, exhausted = self._load_pages(
                object_name, extra_filter=_and_filters(window.filter, date_filter),
                max_pages=max_pages, **page_args)
            total += records
            rows_modified += modified
            logger.debug("Full load of %s: window %s — %s records%s", object_name, window.title,
                         records, '' if exhausted else ' (hit the page limit)')
            return records, exhausted

        # Сюда попадают только объекты, которые НЕ дочитались за лимит страниц, — то есть заведомо
        # непустые. Поэтому отсутствие границы здесь значит не «данных нет», а «границу взять не
        # удалось» (1С не отдала дату в первой строке, см. read_date_bound): переходим на тот же
        # путь, что у регистра, — нащупываем конец пустыми окнами и добираем остаток хвостом.
        # Раньше на этом месте прогон просто заканчивался, отчитавшись успехом на половине объекта.
        oldest = newest = None
        if self._supports_date_bounds(object_name):
            oldest = reader.read_date_bound(object_name, date_field, newest=False,
                                            extra_filter=date_filter)
            if oldest is None:
                logger.warning("Full load of %s: its %s boundaries are unknown — falling back to "
                               "probing the history with empty windows", object_name, date_field)
            else:
                newest = reader.read_date_bound(object_name, date_field, newest=True,
                                                extra_filter=date_filter)

        # «Сейчас» по часам ЭТОЙ машины, и годится любое приближение: окно вверх открыто, а вниз
        # мы идём встык, поэтому промах часов в любую сторону не создаёт дыры — только лишнее
        # пустое окно. Часы 1С ради этого спрашивать незачем.
        anchor = newest or datetime.now().replace(microsecond=0)

        read(anchor, None, max_pages=None)

        cursor = anchor
        window_days = FULL_LOAD_WINDOW_DAYS
        empty_in_a_row = 0
        # Обход прекращён по пустым окнам, а не потому, что дошёл до самой ранней даты. Значит,
        # ниже cursor данные ещё могут быть, и их надо добрать (см. ниже).
        history_probed = False
        while oldest is None or cursor > oldest:
            start = cursor - timedelta(days=window_days)
            # Окно, накрывшее самую раннюю дату, — последнее: ниже ничего нет, и лимит страниц ему
            # уже не нужен, дробить всё равно нечего.
            last = oldest is not None and start <= oldest
            no_limit = last or window_days <= FULL_LOAD_WINDOW_MIN_DAYS
            records, exhausted = read(start, cursor,
                                      max_pages=None if no_limit else FULL_LOAD_PARTITION_MAX_PAGES)
            if not exhausted:
                # Глубоко даже в этом окне — сужаем и перечитываем ТОТ ЖЕ отрезок.
                window_days = max(FULL_LOAD_WINDOW_MIN_DAYS,
                                  window_days // FULL_LOAD_WINDOW_DIVISOR)
                logger.info("Full load of %s: window %s is deep (> %s pages), narrowing to %s days",
                            object_name, self._window(object_name, date_field, start, cursor).title,
                            FULL_LOAD_PARTITION_MAX_PAGES, window_days)
                continue
            cursor = start
            if last:
                return total, rows_modified
            # Пустые окна считаем ВСЕГДА, а не только когда границу снизу спросить не у кого.
            # Известная граница доверия не заслуживает: 1С отдаёт её как есть, а в периоде
            # регистра сведений встречается мусор — пустая дата 1С (0001-01-01) или просто
            # промах пальцем (в демо-базе бухгалтерии лежит запись за 0209 год). Одна такая
            # запись заставляла шагать окнами от сегодняшнего дня до неё: 22 тысячи запросов
            # на регистр, внешне неотличимые от зависшего прогона. Теперь обход ограничен
            # ПЛОТНОСТЬЮ ДАННЫХ, а не календарём, и древний хвост стоит трёх пустых окон плюс
            # одно сплошное чтение.
            empty_in_a_row = empty_in_a_row + 1 if records == 0 else 0
            if empty_in_a_row >= FULL_LOAD_EMPTY_WINDOWS_TO_STOP:
                history_probed = True
                break

        if history_probed:
            # Дальше шагать окнами не по чему: остаток истории добираем одним окном без нижней
            # границы. Дороже одного окна, но дешевле сотен пустых — и ничего не теряем.
            logger.info("Full load of %s: %s empty windows in a row, reading everything below "
                        "%s in one go", object_name, empty_in_a_row, cursor)
            read(None, cursor, max_pages=None)
        return total, rows_modified

    def _is_record_set_object(self, object_name: str) -> bool:
        """
        Отдаётся ли объект РЕЖИМОМ НАБОРА ЗАПИСЕЙ: одна entry = набор движений регистратора, а не
        строка. Так 1С отдаёт регистры, подчинённые регистратору; у них на верхнем уровне entry
        лежат только Recorder, Recorder_Type и вложенная коллекция RecordSet с самими записями.

        Признак — заполненный object_key у объекта, который не является табличной частью. У
        табличной части object_key тоже заполнен (Ref_Key, по нему идёт scoped-удаление), но
        читается она плоскими строками, поэтому её сюда пускать нельзя.

        От этого зависит, как строится фильтр по дате (см. _Window.filter) и можно ли вообще
        спросить у 1С границы периода (см. _supports_date_bounds).
        """
        metadata_obj = self.metadata.get(object_name)
        if metadata_obj is None:
            return False
        return bool(metadata_obj.object_key) and not metadata_obj.is_table_part

    def _supports_date_bounds(self, object_name: str) -> bool:
        """
        Можно ли узнать у 1С самую раннюю и самую позднюю дату объекта одним запросом
        (`$top=1&$orderby=<дата>`, см. DataReader1C.read_date_bound).

        У документа — можно: `Date` лежит на верхнем уровне entry и входит в индекс.

        У регистра в режиме набора записей — НЕЛЬЗЯ, и это не «не поддерживается», а хуже:
        `$orderby=Period` платформа принимает с кодом 200 и МОЛЧА ИГНОРИРУЕТ. Проверено на живой
        1С: `$orderby=Period`, `$orderby=Period desc` и запрос вовсе без сортировки отдают одну и
        ту же первую entry. Поэтому «первая строка упорядоченной выборки» у такого объекта не
        значит ничего, и границы приходится нащупывать пустыми окнами
        (FULL_LOAD_EMPTY_WINDOWS_TO_STOP).
        """
        return not self._is_record_set_object(object_name)

    def _is_accounting_register(self, object_name: str) -> bool:
        """Регистр бухгалтерии: читается из виртуальной таблицы RecordsWithExtDimensions, потому
        что субконто есть только там (см. DataReader1C.read_accounting_register)."""
        return object_name.startswith(ACCOUNTING_REGISTER_TYPE)

    def _window(self, object_name: str, date_field: str,
                start: datetime | None, end: datetime | None) -> _Window:
        """Окно [start, end) с фильтром, подходящим этому объекту (см. _Window)."""
        return _Window(date_field, start, end,
                       record_set=(self._is_record_set_object(object_name)
                                   and not self._is_accounting_register(object_name)))

    def _primary_key_columns(self, object_name: str) -> dict:
        """
        Первичный ключ объекта в терминах целевой таблицы: {колонка: тип SQLAlchemy}. По нему
        собираются ключи прогона и по нему же идёт анти-join пометки.

        Типы берём из самого primary_key, а не из полного набора полей: там лежат те же имена типов
        1С (см. MetadataReader1C._read_metadata_item_key), но набор гарантированно полный. У полей
        ключа, собранных не из $metadata напрямую, соответствия в списке полей может не быть —
        и обращение по ключу роняло бы прогон.
        """
        metadata_obj = self.metadata.get(object_name)
        return {self.name_mapper.map_field_name(field): type_mapping[type_name]
                for field, type_name in metadata_obj.primary_key.items()}

    def _full_load_keys(self, object_name: str) -> FullLoadKeys:
        """Одноразовая таблица ключей прогона (см. full_load_keys)."""
        return FullLoadKeys(self.engine, target_table_name=self._handler_key(object_name),
                            key_columns=self._primary_key_columns(object_name),
                            schema=self.db_temp_schema or self.db_schema)

    def _page_keys(self, object_name: str, reader: DataReader1C) -> list[dict]:
        """Ключи строк одной страницы — только самого объекта: табличные части приезжают вложенно и
        помечаются вместе с владельцем (own-or-skip группы), отдельного снимка по ним нет."""
        data_object = reader.get(object_name)
        if data_object is None or data_object.data_length == 0:
            return []
        data = data_object.data
        fields = self.metadata.get(object_name).primary_key
        columns = [(field, self.name_mapper.map_field_name(field)) for field in fields]
        return [{column: data[field][i] for field, column in columns}
                for i in range(data_object.data_length)]

    def _mark_missing_rows(self, object_name: str, keys: FullLoadKeys, started_at,
                           reader: DataReader1C, recheck: bool, log_id: int | None = None,
                           date_column: str | None = None,
                           date_from: date | datetime | str | None = None,
                           date_to: date | datetime | str | None = None) -> int:
        """
        Помечает строки, которых прогон в 1С не увидел, и сообщает об этом обработчикам.

        recheck=True (выгрузка шла за период) — кандидаты сперва перепроверяются в 1С: из окна
        строка могла не исчезнуть, а уехать (у документа изменилась дата, у независимого регистра —
        поле, входящее в ключ). Ответ перепроверки авторитетнее снимка: он свежее.
        """
        table_name = self._handler_key(object_name)
        try:
            target = self.writer.target_table(table_name)
        except NoSuchTableError:
            # Таблицы нет: объект пуст и в 1С, и в БД (её создаёт первая же сохранённая страница).
            # Помечать нечего.
            logger.info("Full load of %s: nothing to mark, table %s does not exist yet",
                        object_name, table_name)
            return 0
        mark_field = self.name_mapper.map_field_name(IS_DELETED_OR_EMPTY_FIELD)
        scope = self._marking_scope(target, date_column, date_from, date_to)
        if recheck:
            candidates = keys.missing_rows(target, started_at, mark_field, scope=scope)
            if candidates:
                alive = self._still_in_1c(object_name, candidates, reader)
                logger.info("Full load of %s: %s of %s candidates are still in 1C (moved out of "
                            "the period, not deleted)", object_name, len(alive), len(candidates))
                keys.add(alive)
        with self.writes.track(table_name):
            reset_values = self._resource_reset_values(object_name, target)
            marked = keys.mark_missing(target, started_at, mark_field,
                                       reset_values=reset_values, scope=scope)
        if marked:
            logger.info("Full load of %s: %s rows are gone from 1C and were marked deleted",
                        object_name, marked)
            # В журнал пометка идёт как deleted_row_count — тем же счётчиком, которым dbmerge
            # считает строки, помеченные удалёнными.
            if log_id is not None:
                self.replicator_log.write_result(log_id, _marked_result(marked))
            self.handler_signals.signal(table_name, SOURCE_FULL_LOAD)
        return marked

    def _resource_reset_values(self, object_name: str, target) -> dict:
        """Числовые ресурсы регистра гасим в NULL вместе с пометкой — ровно как при выпадении
        строки из набора (см. DBWriter1C._resource_reset_values): SUM игнорирует NULL, и итог
        остаётся верным даже в запросе, забывшем фильтр по is_deleted_or_empty."""
        metadata_obj = self.metadata.get(object_name)
        column_types = metadata_obj.get_column_types()
        values = {}
        for resource in metadata_obj.resources:
            column = self.name_mapper.map_field_name(resource)
            if column in target.c and isinstance(column_types.get(resource), (Integer, Numeric)):
                values[column] = None
        return values

    def _recheck_batch(self, terms: list[str], start: int) -> list[str]:
        """
        Сколько условий влезает в один $filter начиная с terms[start], по бюджету длины
        (см. RECHECK_MAX_QUERY_BYTES). Меряем ЗАКОДИРОВАННУЮ длину — считает байты веб-сервер, а
        до него строка доезжает уже процентно-закодированной, и кириллический тип регистратора в
        ней раздувается втрое.

        Одно условие возвращается всегда, даже если оно само больше бюджета: разбить его нельзя,
        и уж лучше попробовать и получить внятный отказ, чем зациклиться.
        """
        batch = [terms[start]]
        size = len(quote(terms[start], safe="':"))
        separator = len(quote(' or ', safe="':"))
        for term in terms[start + 1:]:
            size += separator + len(quote(term, safe="':"))
            if size > self._recheck_query_budget:
                break
            batch.append(term)
        return batch

    def _still_in_1c_by_recorder(self, object_name: str, candidates: list[dict],
                                 reader: DataReader1C, metadata_obj) -> list[dict]:
        """
        Перепроверка кандидатов у регистра, подчинённого регистратору: по одному запросу НА НАБОР,
        прямым адресом (DataReader1C.read_by_key).

        Пачками через `$filter` тут нельзя вообще ничем: `Recorder` — поле неограниченной длины, и
        `eq guid'…'` 1С отвергает с 500, а `eq '…'` строкой отвечает 200 и НОЛЬ строк, то есть
        молча врёт (подробности и таблица — в read_by_key). Спрашивать по строке тоже нельзя:
        `LineNumber` лежит внутри RecordSet, и фильтр по нему — 400 «Сегмент пути LineNumber не
        найден!».

        Спрашиваем поэтому про НАБОР: он и есть та единица, которая либо существует в 1С, либо нет.
        Набор приходит целиком, _page_keys достаёт из него ключи строк — и строка, выпавшая из
        набора, в «живые» не попадёт, то есть будет помечена, как и задумано.

        Цена — запрос на каждый набор-кандидат. Это терпимо потому, что кандидаты здесь не весь
        объект, а только строки, которых прогон не увидел; но на выгрузке, где «пропало» многое,
        шаг заметен, и заплатить за него приходится: дешёвого способа спросить 1С о наборе по
        ключу у платформы нет.
        """
        recorder_fields = [(field, self.name_mapper.map_field_name(field))
                           for field in metadata_obj.object_key]
        alive = []
        seen = set()
        for row in candidates:
            key = {field: _recorder_type_for_url(field, row[column])
                   for field, column in recorder_fields}
            values = tuple(key.values())
            if values in seen:
                continue     # один регистратор приходит на каждую свою строку — спрашиваем раз
            seen.add(values)
            if reader.read_by_key(object_name, key):
                alive.extend(self._page_keys(object_name, reader))
        return alive

    def _still_in_1c(self, object_name: str, candidates: list[dict],
                     reader: DataReader1C) -> list[dict]:
        """
        Кандидаты, которые в 1С всё-таки есть: запрашиваем их по ключу пачками и возвращаем те,
        что пришли в ответе.

        Пачка набирается по длине строки запроса, а не по числу ключей: ограничивает её веб-сервер
        перед 1С, и меряет он байты. У документа ключ — один guid, и в пачку их влезают десятки; у
        регистра ключ составной, с длинным именем типа регистратора, и влезает несколько.

        Запрос идёт БЕЗ фильтра по периоду — в том и смысл: проверяем существование объекта, а не
        попадание в окно.
        """
        metadata_obj = self.metadata.get(object_name)
        if self._is_record_set_object(object_name):
            return self._still_in_1c_by_recorder(object_name, candidates, reader, metadata_obj)
        fields = [(field, self.name_mapper.map_field_name(field), type_name)
                  for field, type_name in metadata_obj.primary_key.items()]
        terms = []
        for row in candidates:
            conj = ' and '.join(f"{field} eq {_odata_literal(row[column], type_name)}"
                                for field, column, type_name in fields)
            terms.append(f"({conj})" if ' and ' in conj else conj)

        alive = []
        index = 0
        while index < len(terms):
            batch = self._recheck_batch(terms, index)
            try:
                reader.read_object(object_name, extra_filter=' or '.join(batch),
                                   key_fields=[fields[0][0]])
            except requests.HTTPError as exc:
                # Страховка: лимит веб-сервера оказался ниже умолчания, от которого мы считали.
                # Бюджет опускаем и повторяем ТУ ЖЕ пачку — она пересоберётся короче.
                if (not _is_query_too_long(exc) or len(batch) == 1
                        or self._recheck_query_budget <= RECHECK_MIN_QUERY_BYTES):
                    raise
                self._recheck_query_budget = max(RECHECK_MIN_QUERY_BYTES,
                                                 self._recheck_query_budget
                                                 // RECHECK_BUDGET_DIVISOR)
                logger.warning("Recheck of %s: the web server refused the query string as too "
                               "long, lowering the budget to %s bytes and retrying",
                               object_name, self._recheck_query_budget)
                continue
            alive.extend(self._page_keys(object_name, reader))
            index += len(batch)
        return alive

    @staticmethod
    def _odata_datetime(value: date | datetime | str) -> str:
        """OData-литерал datetime'YYYY-MM-DDTHH:MM:SS' из datetime/date (date → полночь) или строки.
        Форматирование — odata_datetime_value: год обязан быть четырёхзначным, иначе 1С отвечает
        400 (важно для пустой даты 1С, 0001-01-01)."""
        if isinstance(value, (datetime, date)):
            value = odata_datetime_value(value)
        return f"datetime'{value}'"

    @staticmethod
    def _marking_scope(target, date_column: str | None,
                       date_from: date | datetime | str | None,
                       date_to: date | datetime | str | None):
        """
        Условие «строка лежит в том же периоде, что читал прогон» — область пометки пропавших строк.

        Без него выгрузка ЗА ПЕРИОД помечала бы всю остальную таблицу: кандидат — это строка, не
        встреченная в прогоне, а прогон видел только своё окно. Спасала бы их одна перепроверка в
        1С, а она идёт пачками ключей по длине строки запроса — на регистре в 20 тысяч строк с
        недельным окном это тысяча лишних запросов КАЖДУЮ ночь.

        Границы повторяют _build_date_filter буква в букву, иначе пометка и загрузка разойдутся на
        краю окна: чистая дата сверху включает весь день целиком, дата-время сравнивается как есть.

        None — ограничивать нечем: период не задан (прогон прочитал объект целиком, и кандидатом
        законно становится вся таблица) либо поля даты в целевой таблице нет.
        """
        if not date_column or (date_from is None and date_to is None):
            return None
        if date_column not in target.c:
            logger.warning("Marking scope skipped: %s has no column %s",
                           target.name, date_column)
            return None
        column = target.c[date_column]
        conditions = []
        if date_from is not None:
            conditions.append(column >= date_from)
        if date_to is not None:
            if isinstance(date_to, date) and not isinstance(date_to, datetime):
                conditions.append(column < date_to + timedelta(days=1))
            else:
                conditions.append(column <= date_to)
        return and_(*conditions)

    def _build_date_filter(self, object_name: str, date_field: str | None,
                           date_from: date | datetime | str | None,
                           date_to: date | datetime | str | None) -> str | None:
        """
        OData $filter по периоду (границы включительно). Возвращает None, если границы не заданы;
        требует date_field, если задана хотя бы одна граница.

        У регистра в режиме набора записей фильтр оборачивается в лямбду по вложенной коллекции —
        ровно как у окон обхода (см. _Window): плоское `Period ge ...` такой объект отвергает.

        Верхняя граница date_to:
        - чистая дата (date без времени) → включаем весь день целиком, даже если поле хранит
          дату-время: `date_field lt <дата+1 день, полночь>` (иначе `le 2026-06-30T00:00:00`
          отсекло бы все записи этого дня, кроме полуночи);
        - дата-время или строка → используем как есть: `date_field le <to>`.
        Нижняя граница date_from всегда включительна (`ge`); чистая дата = с начала дня (полночь).
        """
        if date_from is None and date_to is None:
            return None
        if not date_field:
            raise ValueError("full_load: date_from/date_to require date_field")
        # Регистр бухгалтерии читается не набором записей, а виртуальной таблицей
        # RecordsWithExtDimensions (там движения плоские и Period лежит на верхнем уровне),
        # поэтому лямбда по вложенной коллекции ему не нужна и не подходит.
        record_set = (self._is_record_set_object(object_name)
                      and not self._is_accounting_register(object_name))
        field = f'{RECORD_SET_LAMBDA}/{date_field}' if record_set else date_field
        clauses = []
        if date_from is not None:
            clauses.append(f"{field} ge {Replicator1C._odata_datetime(date_from)}")
        if date_to is not None:
            if isinstance(date_to, date) and not isinstance(date_to, datetime):
                next_day = date_to + timedelta(days=1)
                clauses.append(f"{field} lt {Replicator1C._odata_datetime(next_day)}")
            else:
                clauses.append(f"{field} le {Replicator1C._odata_datetime(date_to)}")
        expression = " and ".join(clauses)
        if not record_set:
            return expression
        return f"{RECORD_SET_FIELD}/any({RECORD_SET_LAMBDA}: {expression})"

    def _next_page_size(self, object_name: str, page_size: int, entries: int,
                        response_bytes: int, batch_size: int) -> int:
        """
        Размер следующей страницы по фактическому весу выданной: сколько entry укладывается в
        FULL_LOAD_TARGET_BYTES. Вес entry у разных объектов различается на порядки (строка
        справочника — килобайты, документ с табличными частями или набор движений регистратора —
        мегабайты), поэтому единый batch_size либо гоняет лишние запросы, либо просит у 1С
        страницу в гигабайты. Сверху ограничивают batch_size и потолок, оставленный отказом 1С
        (_full_load_page_limit), снизу — FULL_LOAD_MIN_BATCH.
        """
        if not entries or not response_bytes:
            return page_size
        per_entry = response_bytes / entries
        fits = max(FULL_LOAD_MIN_BATCH, int(FULL_LOAD_TARGET_BYTES / per_entry))
        # Потолок отказа — сверху вместе с batch_size: вес ответа причину отказа не объясняет,
        # и без потолка подбор вернул бы размер к странице, которую 1С уже не осилила.
        page_size = min(batch_size, fits, self._full_load_page_limit.get(object_name, batch_size))
        self._full_load_page_size[object_name] = page_size
        return page_size

    def _full_load_tables(self, object_name: str) -> list[str]:
        """
        Имена таблиц (в терминах реестра незавершённых merge), в которые пишет полная выгрузка
        объекта: сам объект и его табличные части — 1С отдаёт их вместе с владельцем, и страница
        сохраняет их той же записью.

        Табличные части в метаданных лежат отдельными объектами, названными «владелец_ЧастьИмени»,
        поэтому и ищутся по префиксу.
        """
        prefix = object_name + '_'
        names = [name for name in self.metadata.keys()
                 if name == object_name or name.startswith(prefix)]
        return [self._handler_key(name) for name in names]

    def _full_load_key(self, object_name: str) -> list[str]:
        """
        Поля сортировки страниц full_load по метаданным объекта (список — потому что у
        независимого регистра ключ составной, см. read_object):
        - Ref_Key (справочник/документ) → ['Ref_Key'];
        - Recorder (регистраторный регистр) → ['Recorder']; одна entry = набор регистратора,
          поэтому страница не рвёт набор;
        - Recorder_Key (тот же регистраторный регистр, но с единственным типом регистратора — 1С
          отдаёт поле как Guid и без Recorder_Type) → ['Recorder_Key'];
        - иначе (независимый регистр сведений) → весь первичный ключ.
        Пустой первичный ключ → ValueError.
        """
        metadata_obj = self.metadata.get(object_name)
        primary_key = metadata_obj.primary_key if metadata_obj else {}
        for field in ('Ref_Key', 'Recorder', 'Recorder_Key'):
            if field in primary_key:
                return [field]
        if primary_key:
            return list(primary_key.keys())
        raise ValueError(f"full_load: no primary key for {object_name}")

    def run_forever(self, interval: float = 60.0, max_iterations: int = 0,
                    max_backoff: float = DEFAULT_MAX_BACKOFF) -> None:
        """
        Цикл run_once с паузой interval секунд. Упавший цикл логируется и не подтверждается —
        повтор на следующей итерации. Корректно завершается по SIGTERM/SIGINT.

        Повтор после падения — с экспоненциальной паузой (BACKOFF_FACTOR, потолок max_backoff),
        которая сбрасывается до interval после успешного цикла. Ошибки прав/адреса
        (PERMANENT_HTTP_CODES) уводят паузу на потолок сразу: повтор их не исправит.
        При interval=0 (тесты, прогон без пауз) backoff не применяется.

        max_iterations ограничивает число итераций (0 — бесконечно). Итерацией считается каждый
        вызов run_once, включая упавший на коннекте: ретрай подключения к недоступной 1С — это
        и есть итерация (внутри run_once своих ретраев нет), поэтому max_iterations ограничивает
        и число попыток подключения. Полезно для отладки/тестов.

        Таймаут запросов с interval не связан: пакет изменений обрабатывается сколько нужно,
        а не «не дольше периода опроса». Если он не задан в конструкторе, ридеры подставляют
        DEFAULT_REQUEST_TIMEOUT (см. metadata_reader).

        После каждого цикла фоном (пул потоков) запускаются полные выгрузки помеченных объектов —
        диспетчеризация только здесь (одиночный run_once лишь взводит флаги).

        Потоков получается пять сортов, и пулы у них раздельные: этот цикл, full_load_workers
        потоков полной выгрузки, два потока отметки живости — незавершённых merge (его ведёт
        реестр, а не этот цикл) и захвата полной выгрузки (его ведёт FullLoadClaim) — и по потоку
        на каждого обработчика. Обработчики в пул выгрузки не сабмитятся и занять его не могут.
        Общий у них только engine, поэтому дефицит возникает не в потоках, а в соединениях:
        одновременно их держат пакет изменений, страницы выгрузки, ОБЕ отметки живости и каждый
        обработчик, отсюда
        pool_size >= full_load_workers + 3 + число обработчиков + число расписаний FullLoadCron
        (см. README_DB.md, «Сколько нужно соединений к БД»). Отметка живости, не получившая
        соединения дольше своего TTL, неотличима от мёртвого процесса: её захват достанется
        чужому расписанию, а границу окна обработчика перестанет держать реестр merge.
        """
        stop = StopSignal()
        self._stop_signal = stop
        logger.info("Starting replication loop (interval=%ss, max_iterations=%s, timeout=%ss)",
                    interval, max_iterations, self._request_timeout)
        # Отметку живости незавершённых merge ведёт сам реестр (WriteTracker), а не этот
        # цикл: строки появляются и в одиночном run_once, и в вызванном руками full_load, где
        # никакого цикла нет.
        self._replication_loop(stop, interval, max_iterations, max_backoff)
        logger.info("Replication loop stopped")

    def request_stop(self) -> None:
        """
        Просит идущий run_forever завершиться после текущей итерации — то же, что SIGTERM, но
        программно. Нужна репликатору, крутящемуся не в главном потоке: свой перехват сигналов ему
        поставить нельзя (см. stop_signal), поэтому останавливает его тот, кто поток завёл.
        """
        if self._stop_signal is not None:
            self._stop_signal.requested = True

    def _replication_loop(self, stop: StopSignal, interval: float, max_iterations: int,
                          max_backoff: float) -> None:
        """Тело run_forever: цикл run_once с backoff и фоновыми полными выгрузками."""
        iterations = 0
        delay = interval
        with ThreadPoolExecutor(max_workers=self._full_load_workers,
                                thread_name_prefix='full_load') as executor:
            while not stop.requested:
                try:
                    self.run_once()
                    self._dispatch_full_loads(executor)
                    delay = interval
                except Exception as exc:
                    if interval <= 0:
                        _log_failure(exc, "Replication cycle failed, will retry")
                    elif _is_permanent_error(exc):
                        delay = max_backoff
                        _log_failure(
                            exc, "Replication cycle failed with a permanent error (check "
                            "credentials, rights and exchange plan settings), retry in %ss", delay)
                    else:
                        delay = min(max(delay, interval) * BACKOFF_FACTOR, max_backoff)
                        _log_failure(exc, "Replication cycle failed, retry in %ss", delay)

                iterations += 1
                if max_iterations > 0 and iterations >= max_iterations:
                    logger.info("Reached max_iterations (%s), stopping", max_iterations)
                    break

                stop.wait(delay)
            logger.info("Replication loop stopping, waiting for full loads to finish")

    @contextmanager
    def claim_full_load(self, object_full_name: str):
        """
        Занимает объект под полную выгрузку на время блока: отдаёт True, если объект свободен, и
        False, если его уже выгружает кто-то другой (фоновая выгрузка репликатора или другое
        расписание). Множество занятых — общее с _dispatch_full_loads, поэтому claim видят обе
        стороны.

        Нужен вызывающим извне цикла — прежде всего FullLoadCron: две одновременные выгрузки одного
        объекта данные не портят (у каждого снимка свой full_load_started_at, см. DBWriter1C.save),
        но дают 1С двойную работу и две параллельные строки в replicator_1c_log.

        Заслон один и живёт в БД — отметкой в metadata_objects_1c (см. full_load_claim). Множества
        в памяти процесса тут мало: репликатор и расписание могут работать в разных контейнерах, и
        памятью их не развести. Занять объект удаётся тому, чей UPDATE изменил строку.

        Сам full_load намеренно не охраняется: прямой вызов «выгрузи вот это прямо сейчас» должен
        отрабатывать всегда.

        Имя принимается в обеих формах, как и у full_load, — иначе один и тот же объект, названный
        по-разному, занял бы две разные позиции в множестве занятых, и claim не сработал бы.
        """
        if self.metadata.is_loaded:
            object_full_name = self.metadata.resolve_object_name(object_full_name)
        claimed = self._full_load_claim.claim(object_full_name)
        try:
            yield claimed
        finally:
            if claimed:
                self._full_load_claim.release(object_full_name)

    def _dispatch_full_loads(self, executor: ThreadPoolExecutor) -> None:
        """
        Ставит в пул полные выгрузки объектов с full_load_is_required, кроме уже выполняющихся.

        Захват берётся ЗДЕСЬ, а не внутри задания: иначе объект сабмитился бы повторно, пока ждёт
        своего воркера. Снимается он в _run_full_load (finally).
        """
        for object_full_name in self.metadata.list_full_load_required():
            if not self._full_load_claim.claim(object_full_name):
                logger.debug("Full load of %s is already claimed, skipping", object_full_name)
                continue
            executor.submit(self._run_full_load, object_full_name)

    @_load_mode_tag(LOAD_MODE_FULL)
    def _run_full_load(self, object_full_name: str) -> None:
        """Фоновая полная выгрузка одного объекта; на успехе фиксирует mark_full_loaded вместе с
        метриками прогона. При ошибке флаг full_load_is_required остаётся → ретрай на следующем
        цикле, а метрики не пишутся: они описывают завершённую выгрузку."""
        started = time.monotonic()
        try:
            rows_modified = self.full_load(object_full_name)
            self.metadata.mark_full_loaded(object_full_name, rows_modified=rows_modified,
                                           minutes=round((time.monotonic() - started) / 60, 3))
        except Exception as exc:
            _log_failure(exc, "Background full_load of %s failed, will retry", object_full_name)
        finally:
            self._full_load_claim.release(object_full_name)
