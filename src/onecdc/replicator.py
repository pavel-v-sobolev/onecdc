import functools
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from datetime import date, datetime, timedelta
from urllib.parse import quote

import requests
from dbmerge import mergeResult
from sqlalchemy import Engine, Integer, MetaData, Numeric, Table, and_, exists
from sqlalchemy.exc import NoSuchTableError, OperationalError

from onecdc.metadata_reader import (ACCOUNTING_REGISTER_TYPE, METADATA_ONLY_TYPES,
                                    SUPPORTED_TYPES, MetadataReader, type_mapping)
from onecdc.common_functions import (DB_NOW_WITHOUT_TIMEZONE, format_duration,
                                     instance_owner, odata_datetime_value)
from onecdc.data_reader import (DataReader, FULL_LOAD_MESSAGE_NO, IS_DELETED_OR_EMPTY_FIELD,
                                ODATA_PREFIX, RECORDER_FIELDS, _odata_literal)
from onecdc.change_reader import ChangeReader
from onecdc.full_load_claim import (CLAIM_HEARTBEAT_TTL, HEARTBEAT_FIELD,
                                    OWNER_FIELD, FullLoadClaim)
from onecdc.name_mapper import NameMapper
from onecdc.db_writer import DBWriter, save_order_key
from onecdc.db_logs import (LOAD_TYPE_CHANGES, LOAD_TYPE_FULL, NODE_HEARTBEAT_FIELD,
                            NODE_KEY_FIELD, NODE_OWNER_FIELD, ReplicatorLog,
                            create_table_if_absent,
                            exchange_nodes_table)
from onecdc.full_load_keys import FullLoadKeys, mark_orphaned_table_part
from onecdc.lease import Lease, lease_engine
from onecdc.handlers import (HandlerSignals, SOURCE_CHANGES, SOURCE_FULL_LOAD)
from onecdc.stop_signal import StopSignal, install_signal_handlers
from onecdc.write_tracker import WriteTracker
from onecdc.logging_config import _ensure_handler, get_logger, load_mode, LOAD_MODE_CHANGES, LOAD_MODE_FULL

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

# Сколько времени по МОНОТОННЫМ часам может пройти между продлением аренды узла и отправкой
# подтверждения. С запасом меньше LEASE_ROLE_TTL: между ними всего несколько операторов, и любое
# заметное время здесь означает, что процесс замирал (см. Replicator._confirm_package).
CONFIRM_LEASE_BUDGET = 30.0



def _is_permanent_error(exc: BaseException) -> bool:
    """Ошибка, которую ретрай не исправит (см. PERMANENT_HTTP_CODES)."""
    response = getattr(exc, 'response', None)
    status = getattr(response, 'status_code', None)
    return status in PERMANENT_HTTP_CODES


def _recorder_type_for_url(field: str, value) -> str:
    """
    Значение поля ключа для прямого адреса (см. DataReader.read_by_key). Всё, кроме `<Имя>_Type`,
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
    (ПланОбмена.Х, ExchangePlan_Х) снимаем: в URL их дописывает сам ChangeReader."""
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
    (см. ChangeReader._raise_no_queue_guid). Непустой обязан быть guid: имя или код узла в URL
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


def _check_automatic_full_load(automatic_full_load) -> bool:
    """Автоматическая постановка на полную выгрузку: только True/False. Строку не принимаем
    намеренно — "False" из окружения истинна, и такая опечатка молча включила бы то, что просили
    выключить."""
    if not isinstance(automatic_full_load, bool):
        raise ValueError("automatic_full_load must be True or False "
                         f"(got {automatic_full_load!r})")
    return automatic_full_load


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
    Помечает режимом загрузки все сообщения лога onecdc, выданные внутри метода: полная выгрузка
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
            clauses.append(f"{field} ge {Replicator._odata_datetime(self.start)}")
        if self.end is not None:
            clauses.append(f"{field} lt {Replicator._odata_datetime(self.end)}")
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


class Replicator:
    """
    Оркестратор CDC: читает изменения из 1С (OData) и сохраняет их в БД, подтверждая получение
    только после успешного сохранения.

    Компоненты (MetadataReader / ChangeReader / NameMapper / DBWriter) строятся в
    конструкторе, но без обращения к сети: MetadataReader создаётся пустым, а фактическая
    загрузка метаданных (сетевой запрос) откладывается до первого run_once (по флагу
    metadata.is_loaded). Поэтому недоступность 1С на старте не роняет конструктор — ошибка загрузки
    всплывает уже в run_once: в run_forever она попадает в его try/except и повторяется, а в
    одиночном run_once пробрасывается (это нормально).

    Принимает отдельные аргументы, а не объект настроек: параметры присваиваются явно и на месте
    вызова видно, что именно передано — одинаково и в python-приложении с литералами, и в
    контейнере, где значения берутся из окружения. БД передаётся готовым engine: пользователь сам
    управляет пулом и опциями, а тот же engine прокидывается в DBWriter.
    """

    def __init__(self, odata_url: str, odata_auth: tuple[str, str] | None,
                 exchange_name: str, queue_guid: str,
                 engine: Engine, db_schema: str | None = None,
                 db_temp_schema: str | None = None,
                 request_timeout: float | None = None,
                 full_load_workers: int = 2,
                 automatic_full_load: bool = True,
                 read_subconto: bool = False):
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
        # (например onecdc_tmp) держит их в стороне от таблиц с данными: в ней по определению нет
        # ничего ценного, поэтому таблицу, оставшуюся после падения процесса, там видно и не жалко.
        self.db_temp_schema = _check_db_schema(db_temp_schema)
        self._odata_url = _check_odata_url(odata_url)
        self._exchange_name = _check_exchange_name(exchange_name)
        self._queue_guid = _check_queue_guid(queue_guid)
        # odata_auth — кортеж (user, password) либо None, как в ридерах (передаётся им как есть).
        self._odata_auth = _check_odata_auth(odata_auth)
        # None → таймаут не задан явно: ридеры подставят DEFAULT_REQUEST_TIMEOUT (metadata_reader).
        self._request_timeout = _check_request_timeout(request_timeout)

        # Компоненты строятся сразу, но в сеть не ходят: MetadataReader создаётся пустым,
        # метаданные подгрузятся лениво при первом run_once. MetadataReader получает engine —
        # он же ведёт реестр объектов onecdc_metadata_objects (состояние полной выгрузки).
        # Маппер имён создаётся ПЕРВЫМ и дальше передаётся как есть: он ведёт реестр заявок
        # onecdc_name_claims, и второй экземпляр рядом только зря читал бы ту же таблицу
        # (см. name_mapper — почему имя закрепляется в БД, а не вычисляется).
        self.name_mapper = NameMapper(self.engine, self.db_schema)
        self.metadata = MetadataReader(self._odata_url, odata_auth=self._odata_auth,
                                       request_timeout=self._request_timeout,
                                       engine=self.engine, schema=self.db_schema,
                                       temp_schema=self.db_temp_schema,
                                       name_mapper=self.name_mapper)

        # Фоновая полная выгрузка: пул потоков.
        self._full_load_workers = _check_full_load_workers(full_load_workers)
        # Ставить ли новые объекты на полную выгрузку самому (см. _require_full_load_for_new_objects).
        # False — репликатор только читает изменения: выгрузку тогда назначают руками (флаг
        # full_load_is_required в onecdc_metadata_objects), расписанием FullLoadCron или регистрацией
        # на стороне 1С. Исполнителем выгрузки репликатор остаётся в любом случае — помеченный
        # объект фоновые воркеры возьмут и с False.
        self._automatic_full_load = _check_automatic_full_load(automatic_full_load)
        # Размер страницы, который 1С реально осилила по этому объекту (см. FULL_LOAD_MIN_BATCH).
        # Пишет только поток самой выгрузки, а он на объект один (захват не пускает второго).
        self._full_load_page_size: dict[str, int] = {}
        # Потолок размера страницы по объекту: ставится отказом 1С и только опускается. Нужен
        # потому, что подбор по весу ответа (_next_page_size) причину отказа не видит и вернул бы
        # размер обратно — см. _load_pages.
        self._full_load_page_limit: dict[str, int] = {}
        # Бюджет длины $filter перепроверки, в байтах. Считается от умолчания самого жёсткого
        # Захват объекта под выгрузку (см. full_load_claim): не пускает второго ни в этом процессе,
        # ни в чужом. Репликатор и расписание могут быть подняты в разных контейнерах, поэтому
        # заслон только один и только в БД — множество в памяти их бы не развело.
        self._full_load_claim = FullLoadClaim(
            engine, lambda: self.metadata.objects_table,
            owner=instance_owner(exchange_name))
        # Субконто регистра бухгалтерии: по умолчанию НЕ читаем (см. DataReader._fill_subconto).
        self._read_subconto = read_subconto
        if read_subconto:
            logger.warning(
                "read_subconto=True: ext dimensions are read from the RecordsWithExtDimensions "
                "virtual table, which supports neither $skip nor a usable Top, so they can only "
                "be addressed by listing periods. On a large accounting register this means many "
                "requests and it scales poorly. Analytics of the same posting is usually easier to "
                "take from its recorder document than from the subconto JSON column")

        self.changes = ChangeReader(self._odata_url, self._exchange_name, self._queue_guid,
                                    self.metadata, odata_auth=self._odata_auth,
                                    request_timeout=self._request_timeout,
                                    read_subconto=self._read_subconto)
        # lease_guard вшивает «объект всё ещё наш» прямо в условие записи снимка. Проверять до
        # записи здесь мало: страница пишется минутами, и проверка успевает устареть — а условие
        # внутри оператора проверяет СУБД в момент записи (см. DBWriter._still_ours).
        self.writer = DBWriter(engine=self.engine, name_mapper=self.name_mapper,
                               schema=self.db_schema, temp_schema=self.db_temp_schema,
                               lease_guard=self._full_load_lease_guard)
        # Лог загрузки (строка на объект) пишет оркестратор: только здесь есть контекст обмена
        # (exchange_name/message_no), а writer универсален и может делать и полную перевыгрузку.
        self.onecdc_replicator_log = ReplicatorLog(self.engine, self.db_schema)

        # Сигналы обработчикам идут через handlers, а не через объекты: репликатору не нужны
        # ни их код, ни общий с ними процесс (см. HandlerSignals).
        self.handler_signals = HandlerSignals(self.engine, self.db_schema)
        # Реестр идущих merge — в БД, а не в памяти: обработчик может считать витрину в другом
        # процессе, и границу своего окна он обязан прижимать к НАШИМ незавершённым merge.
        # Он же отвечает за доставку сигнала: строка снимается одной транзакцией с сигналом,
        # а брошенная строка — единственный след того, что сигнал не дошёл (см. _deliver_signal).
        self.writes = WriteTracker(self.engine, self.db_schema, self._exchange_name,
                                   deliver_signal=self._deliver_signal)
        # Аренда узла обмена: читать изменения одного узла вправе ровно один процесс. Без неё
        # два репликатора читают и подтверждают параллельно, а подтверждение удаляет регистрации
        # изменений в самой 1С — вернуть их нечем (витрину-то всегда можно пересобрать).
        # Таблицу создаём сразу и безусловно: захват — это UPDATE, и по отсутствующей строке он
        # дал бы «занято», а отсутствие таблицы пришлось бы трактовать как «можно всем».
        self._nodes_table = exchange_nodes_table(MetaData(), self.db_schema)
        create_table_if_absent(self.engine, self._nodes_table)
        # Отдельный пул под аренды: иначе страницы выгрузки разбирают соединения, потоку отметки
        # живости не достаётся, и живой процесс теряет аренду (см. lease_engine).
        self._lease_engine = lease_engine(self.engine)
        self._node_lease = Lease(self._lease_engine, lambda: self._nodes_table,
                                 instance_owner(exchange_name), subject='exchange-node',
                                 key_field=NODE_KEY_FIELD, owner_field=NODE_OWNER_FIELD,
                                 heartbeat_field=NODE_HEARTBEAT_FIELD)
        self._node_lease.ensure_row(self._queue_guid, exchange_name=exchange_name)
        # Кого мы уже сообщили как владельца узла: лог пишется только на переходах, иначе
        # процесс, которому узел не достался, засыпал бы лог одинаковой строкой каждый цикл.
        self._node_holder_logged: str | None = None
        # Держит ли аренду узла внешний цикл (run_forever). Одиночный run_once отпускает её сам.
        self._keep_node_lease = False
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
        # Узел обмена берём ПЕРЕД чтением, а не перед подтверждением. Проверки перед
        # подтверждением мало: вред наносит уже сам SelectChanges второго процесса — он помечает
        # свежие изменения номером сообщения, который потом удалит первый своим подтверждением.
        # Захват должен закрывать цикл целиком.
        if not self._claim_node():
            return
        try:
            self._run_once_claimed(notify_changes)
        finally:
            # Внутри run_forever аренда живёт через все циклы: отпускать её каждую минуту значило
            # бы устраивать новую гонку на ровном месте, а читатель мигал бы между процессами.
            #
            # А вот ОДИНОЧНЫЙ run_once обязан отпустить. Он задуман как самостоятельный режим (в
            # том числе по расписанию снаружи), и процесс после него завершается. Оставь мы узел
            # захваченным — следующий запуск через минуту не смог бы его взять и молча ничего бы
            # не сделал, и так все 15 минут TTL.
            if not self._keep_node_lease:
                self._node_lease.release(self._queue_guid)

    def _claim_node(self) -> bool:
        """Захватывает узел обмена. False — читает кто-то другой, цикл пропускаем."""
        if self._node_lease.acquire(self._queue_guid):
            if self._node_holder_logged is not None:
                logger.info("Exchange node %s is ours now, reading changes", self._queue_guid)
                self._node_holder_logged = None
            return True
        holder = self._node_lease.live_owners().get(self._queue_guid, '?')
        if self._node_holder_logged != holder:
            # Только на переходе: иначе процесс, которому узел не достался, писал бы эту строку
            # каждый цикл, и лог стал бы нечитаемым.
            logger.info("Exchange node %s is being read by %s, skipping this cycle",
                        self._queue_guid, holder)
            self._node_holder_logged = holder
        return False

    def _run_once_claimed(self, notify_changes: bool) -> None:
        """Тело цикла под захваченным узлом обмена (см. run_once)."""
        # Первый вызов: грузим метаданные. Дальше не перечитываем — это делает сам data_reader
        # при появлении нового объекта/поля (get_metadata держит is_loaded=True).
        if not self.metadata.is_loaded:
            self.metadata.get_metadata()
        # Оборванные записи прошлых попыток: их данные могли закоммититься, а сигнал не уйти.
        # Разбираем ДО чтения нового пакета — повтор пакета сигнала не вернёт (второй merge
        # ничего не изменит), а здесь след ещё есть.
        self.writes.deliver_abandoned()
        # Время пакета считаем от чтения из 1С и до конца всех merge — это то, что реально
        # занимает цикл. Загрузка метаданных сюда не входит: она разовая и к пакету не относится.
        started = time.monotonic()
        self.changes.read_changes()
        # Узел могли перехватить, пока 1С формировала пакет (а он бывает на десятки мегабайт).
        # Бросить работу здесь безопасно: набор перехватившего ВКЛЮЧАЕТ наш — он прочитает
        # то же самое и сохранит сам (см. DESIGN.md «Как 1С отдаёт изменения»). Проверка не ради
        # корректности — её обеспечивает заслон перед подтверждением, — а чтобы не тратить минуты
        # на merge, который параллельно делает законный владелец, и не сталкиваться с ним на
        # уникальном ключе.
        if not self._node_lease.still_mine(self._queue_guid):
            logger.warning("Exchange node %s was taken over while the package was being read — "
                           "dropping this cycle, the package will come again", self._queue_guid)
            return
        self._save_changes()
        logger.info("Changes package %s processed in %s: %s rows",
                    self.changes.message_no, format_duration(time.monotonic() - started),
                    self.changes.rows_read())
        # Новые объекты пакета — в очередь на полную выгрузку. Отключается параметром
        # automatic_full_load: тогда репликатор только читает изменения, а выгрузку ставят руками
        # (флаг full_load_is_required) или расписанием.
        if self._automatic_full_load:
            self._require_full_load_for_new_objects()
        
        # Подтверждаем по числу ПРОЧИТАННЫХ entry, а не разобранных объектов. Раньше условием
        # было len(self.changes) > 0, и пакет из одних неподдерживаемых классов (константы,
        # регистры расчёта — их кладёт в очередь в том числе дроссель) не подтверждался: тот же
        # SelectChanges каждые 60 секунд, очередь забита, репликация ВСЕГО плана стоит, а в логе
        # только сообщение о пропуске.
        #
        # Это строго хуже смешанного пакета, где те же изменения теряются, но остальное едет.
        # Сохранить их мы всё равно не можем ни в том, ни в другом случае — значит остановка
        # ничего не спасает, а стоит всего плана. Политика одна: пакет прочитан — пакет
        # подтверждён, а о потере кричит ERROR из read_data_entries.
        if notify_changes and self.changes.entries_read > 0:
            self._confirm_package()
        else:
            logger.debug("No changes — skipping confirmation")

    def _confirm_package(self) -> None:
        """
        Подтверждает пакет — единственное НЕОБРАТИМОЕ действие цикла: 1С удаляет по нему
        регистрации изменений, и вернуть их нечем.

        Три заслона, и каждый закрывает свой путь.

        1. Проверка аренды с продлением. Потеряли узел, пока считали пакет, — не подтверждаем:
           пакет придёт снова, потерь нет. Тот, кто перехватил узел, прочитает его целиком —
           его набор ВКЛЮЧАЕТ наш (проверено на живой 1С, см. DESIGN.md «Как 1С отдаёт
           изменения»), поэтому бросить работу здесь безопасно в любой момент.

        2. Проверка по МОНОТОННЫМ часам прямо перед отправкой. Продление говорит «аренда наша ещё
           LEASE_ROLE_TTL секунд», но если мы замерли между продлением и отправкой, отправим уже
           после её истечения. Собственные часы это ловят, и им для этого не нужны ни сеть, ни БД
           — то есть они не отказывают по тем же причинам, что и отметка живости.

        3. Проверка ПОСЛЕ подтверждения. Предотвратить уже поздно, но потеря перестаёт быть
           молчаливой: в логе ERROR, в журнале след. Автоматически при этом ничего не
           перевыгружаем — потеря аренды НЕ означает потери данных: перехвативший почти наверняка
           сохранил всё сам, и часы работы 1С по такому подозрению несоразмерны.
        """
        # Отсчёт СТАРТУЕТ ДО продления, а не после: замереть можно и между коммитом продления в
        # БД и возвратом из still_mine, и этот кусок бюджет обязан покрывать.
        started_at = time.monotonic()
        if not self._node_lease.still_mine(self._queue_guid):
            logger.warning("Exchange node %s was taken over while the package was being saved — "
                           "not confirming it, the package will come again", self._queue_guid)
            return

        elapsed = time.monotonic() - started_at
        if elapsed > CONFIRM_LEASE_BUDGET:
            logger.error("Exchange node %s: %.0fs passed between renewing the lease and "
                         "confirming the package — not confirming it, the package will come again",
                         self._queue_guid, elapsed)
            return

        self.changes.notify_changes_received()

        if not self._node_lease.still_mine(self._queue_guid):
            logger.error(
                "Exchange node %s was taken over while package %s was being confirmed. The "
                "confirmation may have removed change registrations this process never received. "
                "Check whether the other reader is alive: if it is, it has the data; if it is "
                "not, run a full load of the exchange plan objects",
                self._queue_guid, self.changes.message_no)


    def _require_full_load_for_new_objects(self) -> None:
        """
        Объект пришёл в пакете → он в плане обмена. Если ни разу не выгружался целиком, помечаем
        на полную выгрузку (выполнит фоновый воркер в run_forever).

        Табличные части пропускаем: они догружаются вместе с владельцем при его full_load,
        приезжая вложенными в его entry. Отдельная сущность в OData у них есть, но грузить их
        ею незачем и дороже — страница владельца приносит его табличные части целиком.
        """
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


    def _save_changes(self) -> None:
        """
        Сохраняет объекты пакета по одному, записывая лог загрузки на каждый объект
        (onecdc_replicator_log). finish() — только после успешного save: упавший объект остаётся с
        finished_at=NULL и не двигает границу окна обработчика. Лог здесь, а не в DBWriter,
        потому что только тут есть контекст обмена (exchange_name/message_no).

        Порядок сохранения — справочники → документы → регистры (save_order_key): документы ссылаются
        на справочники, регистры — на документы, поэтому родителей пишем раньше. Внутри группы
        исходный порядок пакета (сортировка стабильна).
        """
        # Объекты, по которым прямо сейчас идёт снимок. Один запрос на пакет: пока снимок в
        # полёте, его страницы могут быть старше наших данных, и no-op пакет обязан оставить
        # след (см. DBWriter.save, always_touch).
        claimed = self._full_load_claim.live_claims()
        for object_name, data_object in sorted(self.changes.items(),
                                               key=lambda kv: save_order_key(kv[0])):
            log_id = self.onecdc_replicator_log.start(
                self.changes.exchange_name, object_name, self.changes.message_no, LOAD_TYPE_CHANGES)
            table_name = self._handler_key(object_name)
            # Сигнал снимает строку реестра — он внутри блока, а не после него: пока строка есть,
            # обязанность сообщить об изменении не исполнена, и оборванная запись видна.
            with self.writes.track(table_name, SOURCE_CHANGES) as tracked:
                tracked.result = self.writer.save(
                    object_name, data_object,
                    always_touch=self._under_full_load(object_name, claimed))
                # Одно сохранение на строку лога: счётчики и завершение — одним запросом.
                self.onecdc_replicator_log.write_result(log_id, tracked.result, finish=True)
                self._warn_about_new_columns(table_name, tracked.result)

    def _under_full_load(self, object_name: str, claimed: set[str]) -> bool:
        """
        Идёт ли сейчас полная выгрузка этого объекта — с точки зрения записи изменений.

        Табличная часть наследует захват ВЛАДЕЛЬЦА: своего захвата у неё нет, а страница снимка
        пишет её вместе с владельцем и той же записью. Без этого ТЧ остались бы без следа именно
        тогда, когда он нужен, — и снимок затирал бы их устаревшими строками.
        """
        if object_name in claimed:
            return True
        owner = self.metadata.owner_of(object_name)
        return owner is not None and owner in claimed

    def _handler_key(self, object_name: str) -> str:
        """
        Имя таблицы объекта в БД — под ним объект и известен обработчикам (см. Handler.ON).
        Подписка идёт по имени таблицы, а не по имени объекта 1С, потому что обработчик пишет SQL
        по таблицам: имя 1С он в глаза не видит, а транслит стоит у него в запросе.
        """
        return self.name_mapper.map_object_name(object_name)

    def _deliver_signal(self, conn, table_name: str, source: str | None, result,
                        forced: bool) -> None:
        """
        Сообщает обработчикам, что таблица изменилась. Вызывает реестр идущих merge — в той же
        транзакции, в которой снимает строку записи (см. WriteTracker._finish): иначе между
        коммитом данных и сигналом помещается сбой, а повтор пакета сигнала не восстановит —
        второй merge ничего не изменит, и пакет молча подтвердится.

        Обычно сигналим, только если merge реально что-то сделал: 1С регистрирует изменение
        объекта на любую перезапись, и в пакет приезжает масса записей, идентичных тому, что уже
        лежит в БД (шумные поля при сравнении не учитываются, см. DBWriter._noisy_fields).
        Обработчик всё равно выбирает данные сам, и на пустом прогоне его SELECT вернул бы пусто.

        forced=True — разбор брошенной строки: там неизвестно, что сделал прерванный merge и
        сделал ли что-нибудь, поэтому сигналим безусловно. Лишний сигнал безвреден, он
        идемпотентен.

        Сообщаем через БД: ставим метку update_requested_at в handlers тем, у кого эта таблица
        есть в update_on. Ни объектов обработчиков, ни их кода репликатору для этого не нужно,
        поэтому они могут работать в другом процессе или контейнере.
        """
        if not forced and (result is None or _rows_modified(result) <= 0):
            return
        self.handler_signals.signal(table_name, source or SOURCE_CHANGES, conn=conn)

    @staticmethod
    def _warn_about_new_columns(table_name: str, result) -> None:
        """
        Новая колонка в таблице объекта — событие, о котором стоит знать, но не повод что-то
        делать автоматически.

        Раньше на него подписчикам заказывалась полная пересборка витрины. Смысла в этом нет.
        Во-первых, витрину строит код обработчика, а он про колонку, которой вчера не было, ничего
        не знает — её добавит человек, он же и закажет пересборку. Во-вторых, пересборка эту
        колонку всё равно не наполнила бы: у строк, приехавших до её появления, в источнике лежит
        NULL, и пересборка прочитала бы тот же NULL. Дочитать значения может только полная
        выгрузка объекта — и решать, нужна ли она, тоже человеку: новый реквизит может быть в КХД
        вовсе не нужен.

        Исторически повод был другим: состав колонок «плавал» от пачки к пачке, потому что парсер
        терял пустые реквизиты (поле, пустое у всех записей пачки, не создавало колонки вовсе).
        Теперь пустой реквизит разбирается как NULL, и колонка заводится сразу — плавать нечему.
        """
        if result is not None and result.added_fields:
            logger.warning("Table %s gained columns %s. Handlers that need them must be updated, "
                           "and a full load of the object may be required to fill them for rows "
                           "loaded earlier", table_name, sorted(result.added_fields))

    def list_objects(self) -> list[str]:
        """
        Список имён объектов 1С, доступных для выгрузки (документы/справочники и регистры).

        Табличные части исключаются: они приходят вложенно с владельцем и грузятся вместе с ним, а
        отдельно full_load их и не примет (см. _refuse_table_part). Классы из METADATA_ONLY_TYPES
        исключаются тоже: их метаданные читаются ради регистра бухгалтерии, но выгружать их мы не
        беремся (см. SUPPORTED_TYPES).

        Метаданные при необходимости подгружаются (первый сетевой запрос). Удобно, чтобы узнать,
        что передавать в full_load.

        Имена — как в 1С (кириллица). В full_load годится и такое имя, и имя таблицы в БД: он
        принимает обе формы (см. MetadataReader.resolve_object_name). Список имён таблиц лежит в
        реестре onecdc_metadata_objects, колонка object_full_name_en.
        """
        if not self.metadata.is_loaded:
            self.metadata.get_metadata()
        return [name for name, obj in self.metadata.items()
                if not obj.is_table_part and name.startswith(SUPPORTED_TYPES)]

    def _refuse_table_part(self, object_name: str) -> None:
        """
        Отказ выгружать табличную часть напрямую — с именем владельца, который её и привезёт.

        Табличная часть в OData — отдельная сущность, и `full_load`, натравленный прямо на неё,
        листал её плоскими строками. Пагинация при этом работала (ключ страницы дополняется
        `LineNumber`, иначе строки одного владельца равны по `Ref_Key` и порядок между запросами
        не воспроизводится), а вот ЗАПИСЬ — нет: `DBWriter.save` заменяет группу целиком, считая,
        что страница несёт её целиком. Строки одного владельца, разложенные по двум страницам,
        помечали друг друга выпавшими из набора — вторая страница гасила первую, и в таблице
        оставался хвост вместо всей части. Guard по `merged_on` тут не спасает: отметка берётся на
        каждую страницу заново, поэтому первая для второй уже «старая».

        Чинить этот режим незачем — он ничего не даёт. Строки табличной части приезжают ВЛОЖЕННЫМИ
        в entry владельца и в изменениях, и в его полной выгрузке, то есть всегда целой группой, а
        толстый владелец и так листается подобранным под его вес размером страницы. Осиротевшие
        строки (владельца удалили физически) помечает отдельный проход по его состоянию.
        """
        owner = self.metadata.owner_of(object_name)
        if owner is None:
            return
        raise ValueError(
            f'{object_name} is a table part and cannot be loaded on its own: its rows would be '
            f'split across pages, and each page marks the rows of the previous one as missing. '
            f'Load its owner instead — full_load({owner!r}) brings the table part with it.')

    @_load_mode_tag(LOAD_MODE_FULL)
    def full_load(self, object_name: str, batch_size: int = 1000,
                  date_field: str | None = None,
                  date_from: date | datetime | str | None = None,
                  date_to: date | datetime | str | None = None,
                  mark_missing: bool = True,
                  read_subconto: bool | None = None) -> int:
        """
        Полная постраничная выгрузка объекта 1С в целевую таблицу: страницами, размер которых
        подбирается по их весу (batch_size — лишь верхняя граница, см. ниже), и каждая страница
        сразу сохраняется через writer.save(full_load_started_at=...). Идемпотентно — повторный
        прогон обновляет строки по ключу. Документ/справочник — чистый upsert; регистр/табличная часть —
        own-or-skip группы целиком (группа умещается на одной странице), см. DBWriter.save.

        Документ/справочник выгружается вместе с табличными частями — они приходят вложенно в той же
        странице и сохраняются как отдельные объекты. Сортировка страниц — по первичному ключу
        (см. _full_load_key), переход к следующей странице — смещением $skip: курсора по ключу 1С
        не даёт, потому что в ключе стоит ссылка (см. DESIGN.md, «Почему страницы берутся только
        через $skip»). Глубину лечит не курсор, а нарезка окнами по периоду (см. ниже).
        Один прогон = одна строка в onecdc_replicator_log (message_no=NULL — это не пакет обмена);
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
        См. DBWriter.save. Отдельно от этого берётся started_at прогона (writer.db_now()) — он
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
        периода (см. _marking_scope). Кандидаты не переспрашиваются в 1С — не увидели, значит
        помечаем; ложная пометка снимается сама, когда строка приедет изменением (почему так —
        DESIGN.md, «Не увидели — помечаем»).

        read_subconto — читать ли субконто регистра бухгалтерии. None (по умолчанию) означает
        «как задано у репликатора», True/False перекрывают его на этот прогон. Выключено по
        умолчанию: субконто добираются из виртуальной таблицы, у которой нет ни `$skip`, ни
        работающего `Top`, поэтому адресовать её можно только списками периодов — на большом
        регистре это много запросов (см. DataReader._fill_subconto).

        Возвращает число РЕАЛЬНО изменённых строк (вставлено + обновлено + удалено, по всем
        страницам и вложенным объектам). Это проверка самого CDC: если изменения доезжают исправно,
        выгрузка находит ровно то, что уже лежит в БД, и ответ должен быть 0.
        """
        if not self.metadata.is_loaded:
            self.metadata.get_metadata()

        # Имя объекта и имя поля даты принимаются в ОБЕИХ формах — как в 1С и как в БД
        # (`Document_ЗаказКлиента` / `Document_ZakazKlienta`, `Дата` / `Data`), см.
        # MetadataReader.resolve_object_name. Ровно то же делают FullLoadCron и Handler.ON:
        # настраивая выгрузку, смотрят в базу, а не в конфигуратор.
        object_name = self.metadata.resolve_object_name(object_name)
        # Класс, метаданные которого читаются, но сохранять который мы не беремся (METADATA_ONLY_TYPES
        # — сейчас это план видов характеристик, нужный регистру бухгалтерии для видов субконто).
        # В метаданных он есть, поэтому имя разрешилось бы и выгрузка пошла бы — с результатом,
        # который никто не проверял. Отказываем сразу и по делу.
        if object_name.startswith(METADATA_ONLY_TYPES):
            raise ValueError(
                f'{object_name}: this class is read for metadata only and is not supported for '
                f'loading (supported: {", ".join(SUPPORTED_TYPES)})')
        self._refuse_table_part(object_name)
        if date_field:
            date_field = self.metadata.resolve_field_name(object_name, date_field)

        # Ключ сортировки: справочник/документ → [Ref_Key], регистраторный → [Recorder]/
        # [Recorder_Key], независимый регистр → весь первичный ключ (составной ключ).
        key_fields = self._full_load_key(object_name)
        date_filter = self._build_date_filter(object_name, date_field, date_from, date_to)

        reader = DataReader(self._odata_url, self.metadata, odata_auth=self._odata_auth,
                            request_timeout=self._request_timeout,
                            read_subconto=(self._read_subconto if read_subconto is None
                                           else read_subconto))
        # Полная выгрузка = базовая версия: ниже любого номера пакета изменений (>=1), и заодно
        # след автора строки для guard'а вставки (см. FULL_LOAD_MESSAGE_NO).
        reader.exchange_message_no = FULL_LOAD_MESSAGE_NO

        # Момент старта прогона по часам БД: по нему помечаются пропавшие строки в конце прогона
        # (guard'ы save берут свою отметку на каждую страницу, см. ниже).
        started_at = self.writer.db_now()

        # Поле, по которому объект можно порезать на периоды, если он окажется глубоким.
        partition_field = self._partition_date_field(object_name, date_field)

        log_id = self.onecdc_replicator_log.start(self._exchange_name, object_name, None, LOAD_TYPE_FULL)
        logger.info("Full load of %s started (batch_size=%s, key=%s, date_filter=%s, "
                    "partition_field=%s)", object_name, batch_size, key_fields,
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
            page_args = dict(reader=reader, key_fields=key_fields,
                             batch_size=batch_size, keys=keys, log_id=log_id)
            # Читался ли объект окнами по дате. Важно для пометки пропавших строк: окно
            # порождает «уехавшие» строки (см. _mark_missing_rows), и неважно, задал его
            # пользователь или обход окнами выбрал сам.
            windowed = False

            # Сначала читаем объект как есть — без окон и без лишних запросов. Мелкому объекту
            # (а таких большинство) окна только вредят: он укладывается в пару страниц, а за
            # обход пришлось бы заплатить запросом на каждое окно истории, даже пустое.
            # Лимит страниц ставим, только если резать вообще есть по чему.
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
                    object_name, keys, started_at, log_id=log_id,
                    date_column=(self.name_mapper.map_field_name(date_field, object_name)
                                 if date_field else None),
                    date_from=date_from, date_to=date_to)
        self.onecdc_replicator_log.write_result(log_id, finish=True)
        logger.info("Full load of %s finished (%s records, %s rows modified)",
                    object_name, total, rows_modified)
        return rows_modified


    def _load_pages(self, object_name: str, *, reader: DataReader, key_fields: list[str],
                    extra_filter: str | None, batch_size: int, keys, log_id,
                    max_pages: int | None) -> tuple[int, int, bool]:
        """
        Постраничное чтение одной выборки (объект целиком либо его окно по периоду) с записью
        каждой страницы. Возвращает (сколько записей прочитано, сколько строк изменено, дочитано ли
        до конца).

        max_pages ограничивает число страниц: превышение означает «выборка слишком глубокая», и
        вызывающий режет её на меньшие периоды (см. full_load). None — читать до конца.

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
                page = reader.read_object(object_name, top=page_size, key_fields=key_fields,
                                          extra_filter=extra_filter, skip=skip)
            except requests.HTTPError as exc:
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
                # Сигнал на каждую страницу, а не один в конце прогона: это метка времени в одной
                # колонке (onecdc_handlers.update_requested_at), а не очередь событий, — тысяча
                # страниц тысячу раз перепишет ту же метку, а не выстроит тысячу вызовов. Зато
                # витрина начинает наполняться после первой же страницы, а не через часы, когда
                # выгрузка закончится. Ставит его выход из блока, одной транзакцией со снятием
                # строки реестра.
                with self.writes.track(table_name, SOURCE_FULL_LOAD) as tracked:
                    result = self.writer.save(obj_name, data_object,
                                              full_load_started_at=page_started_at)
                    tracked.result = result
                    self.onecdc_replicator_log.write_result(log_id, result)
                    self._warn_about_new_columns(table_name, result)
                rows_modified += _rows_modified(result)
            total += page
            pages += 1
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

    def _load_by_windows(self, object_name: str, *, reader: DataReader, date_field: str,
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
        (`$top=1&$orderby=<дата>`, см. DataReader.read_date_bound).

        У документа — можно: `Date` лежит на верхнем уровне entry и входит в индекс.

        У регистра в режиме набора записей — НЕЛЬЗЯ, и это не «не поддерживается», а хуже:
        `$orderby=Period` платформа принимает с кодом 200 и МОЛЧА ИГНОРИРУЕТ. Проверено на живой
        1С: `$orderby=Period`, `$orderby=Period desc` и запрос вовсе без сортировки отдают одну и
        ту же первую entry. Поэтому «первая строка упорядоченной выборки» у такого объекта не
        значит ничего, и границы приходится нащупывать пустыми окнами
        (FULL_LOAD_EMPTY_WINDOWS_TO_STOP).
        """
        return not self._is_record_set_object(object_name)

    def _window(self, object_name: str, date_field: str,
                start: datetime | None, end: datetime | None) -> _Window:
        """Окно [start, end) с фильтром, подходящим этому объекту (см. _Window)."""
        return _Window(date_field, start, end,
                       record_set=self._is_record_set_object(object_name))

    def _primary_key_columns(self, object_name: str) -> dict:
        """
        Первичный ключ объекта в терминах целевой таблицы: {колонка: тип SQLAlchemy}. По нему
        собираются ключи прогона и по нему же идёт анти-join пометки.

        Типы берём из самого primary_key, а не из полного набора полей: там лежат те же имена типов
        1С (см. MetadataReader._read_metadata_item_key), но набор гарантированно полный. У полей
        ключа, собранных не из $metadata напрямую, соответствия в списке полей может не быть —
        и обращение по ключу роняло бы прогон.
        """
        metadata_obj = self.metadata.get(object_name)
        return {self.name_mapper.map_field_name(field, object_name): type_mapping[type_name]
                for field, type_name in metadata_obj.primary_key.items()}

    def _full_load_keys(self, object_name: str) -> FullLoadKeys:
        """Одноразовая таблица ключей прогона (см. full_load_keys)."""
        return FullLoadKeys(self.engine, target_table_name=self._handler_key(object_name),
                            key_columns=self._primary_key_columns(object_name),
                            schema=self.db_temp_schema or self.db_schema)

    def _page_keys(self, object_name: str, reader: DataReader) -> list[dict]:
        """Ключи строк одной страницы — только самого объекта: табличные части приезжают вложенно и
        помечаются вместе с владельцем (own-or-skip группы), отдельного снимка по ним нет."""
        data_object = reader.get(object_name)
        if data_object is None or data_object.data_length == 0:
            return []
        data = data_object.data
        fields = self.metadata.get(object_name).primary_key
        columns = [(field, self.name_mapper.map_field_name(field, object_name))
                   for field in fields]
        return [{column: data[field][i] for field, column in columns}
                for i in range(data_object.data_length)]

    def _mark_missing_rows(self, object_name: str, keys: FullLoadKeys, started_at,
                           log_id: int | None = None,
                           date_column: str | None = None,
                           date_from: date | datetime | str | None = None,
                           date_to: date | datetime | str | None = None) -> int:
        """
        Помечает строки, которых прогон в 1С не увидел, и сообщает об этом обработчикам.

        Не увидел — значит помечает, без переспрашивания. Кандидат мог не исчезнуть, а уехать за
        пределы прочитанного (у документа изменилась дата, у регистра — период регистратора), и
        тогда пометка ложная; снимается она сама, когда строка приедет изменением или следующей
        выгрузкой её нового периода. Почему переспрашивать перестали — см. DESIGN.md,
        «Пропавшие строки».
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
        mark_field = self.name_mapper.map_field_name(IS_DELETED_OR_EMPTY_FIELD, object_name)
        scope = self._marking_scope(target, date_column, date_from, date_to)
        with self.writes.track(table_name, SOURCE_FULL_LOAD) as tracked:
            reset_values = self._resource_reset_values(object_name, target)
            marked = keys.mark_missing(target, started_at, mark_field,
                                       reset_values=reset_values, scope=scope)
            if marked:
                logger.info("Full load of %s: %s rows are gone from 1C and were marked deleted",
                            object_name, marked)
                # В журнал пометка идёт как deleted_row_count — тем же счётчиком, которым dbmerge
                # считает строки, помеченные удалёнными. Он же решает, нужен ли сигнал: пометка —
                # такое же изменение строк, как merge.
                tracked.result = _marked_result(marked)
                if log_id is not None:
                    self.onecdc_replicator_log.write_result(log_id, tracked.result)
        # Владелец исчез — его табличные части обязаны разделить судьбу. Отдельным шагом, потому
        # что в ключах прогона их нет: они приезжают вложенно и заменяются группой при приходе
        # владельца, а он не пришёл.
        marked += self._mark_orphaned_table_parts(object_name, target, started_at, log_id)
        return marked

    def _mark_orphaned_table_parts(self, object_name: str, target, started_at,
                                   log_id: int | None) -> int:
        """
        Помечает строки табличных частей, чей владелец помечен удалённым.

        Идёт ПОСЛЕ пометки владельца и после перепроверки кандидатов: перепроверка авторитетнее
        снимка, и владелец, который не исчез, а уехал из окна выгрузки, до этого шага не дойдёт —
        его строки не помечены, значит и части не тронем.

        У каждой части своя таблица и свои подписчики, поэтому и строка реестра, и сигнал у неё
        свои (см. CDC-07: сигнал снимает строку одной с ней транзакцией).
        """
        owner_key = list(self._primary_key_columns(object_name))
        total = 0
        for part_name in self._table_parts_of(object_name):
            part_table_name = self._handler_key(part_name)
            try:
                part = Table(part_table_name, MetaData(), schema=self.db_schema,
                             autoload_with=self.engine)
            except NoSuchTableError:
                continue        # часть ни разу не грузилась — помечать нечего
            part_metadata = self.metadata.get(part_name)
            # Связь с владельцем — object_key самой части (у табличной части это Ref_Key
            # владельца по построению, см. MetadataReader._get_object_key), а не хардкод имени.
            link = [self.name_mapper.map_field_name(field, part_name)
                    for field in (part_metadata.object_key or [])]
            if not link:
                logger.warning("Full load of %s: table part %s has no link to its owner, "
                               "its rows cannot be marked", object_name, part_name)
                continue
            mark_field = self.name_mapper.map_field_name(IS_DELETED_OR_EMPTY_FIELD, part_name)
            with self.writes.track(part_table_name, SOURCE_FULL_LOAD) as tracked:
                marked = mark_orphaned_table_part(
                    self.engine, part, target, link, owner_key, started_at, mark_field)
                if marked:
                    logger.info("Full load of %s: %s rows of %s were marked deleted — their "
                                "owner is gone from 1C", object_name, marked, part_name)
                    tracked.result = _marked_result(marked)
                    if log_id is not None:
                        self.onecdc_replicator_log.write_result(log_id, tracked.result)
            total += marked
        return total

    def _table_parts_of(self, object_name: str) -> list[str]:
        """Табличные части объекта — обратное к MetadataReader.owner_of."""
        prefix = object_name + '_'
        return [name for name, obj in self.metadata.items()
                if obj.is_table_part and name.startswith(prefix)
                and self.metadata.owner_of(name) == object_name]

    def _resource_reset_values(self, object_name: str, target) -> dict:
        """Числовые ресурсы регистра гасим в NULL вместе с пометкой — ровно как при выпадении
        строки из набора (см. DBWriter._resource_reset_values): SUM игнорирует NULL, и итог
        остаётся верным даже в запросе, забывшем фильтр по is_deleted_or_empty."""
        metadata_obj = self.metadata.get(object_name)
        column_types = metadata_obj.get_column_types()
        values = {}
        for resource in metadata_obj.resources:
            column = self.name_mapper.map_field_name(resource, object_name)
            if column in target.c and isinstance(column_types.get(resource), (Integer, Numeric)):
                values[column] = None
        return values

    @staticmethod
    def _odata_datetime(value: date | datetime | str) -> str:
        """OData-литерал datetime'YYYY-MM-DDTHH:MM:SS' из datetime/date (date → полночь) или строки.
        Форматирование — odata_datetime_value: год обязан быть четырёхзначным, иначе 1С отвечает
        400 (важно для пустой даты 1С, 0001-01-01).

        ISO-строка проходит через тот же форматтер, а не подставляется как есть: 1С требует
        ПОЛНЫЙ литерал со временем, и datetime'2026-09-01' она отвергает с 400 «Ошибка при разборе
        опции запроса $filter» (проверено на живой 1С), хотя ровно такую границу естественно
        написать в расписании. Строка, которую разобрать не удалось, идёт в запрос как есть —
        подставлять её пользователь мог осознанно, а 1С сама скажет, если литерал неверен."""
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value)
            except ValueError:
                return f"datetime'{value}'"
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
        record_set = self._is_record_set_object(object_name)
        field = f'{RECORD_SET_LAMBDA}/{date_field}' if record_set else date_field
        clauses = []
        if date_from is not None:
            clauses.append(f"{field} ge {Replicator._odata_datetime(date_from)}")
        if date_to is not None:
            if isinstance(date_to, date) and not isinstance(date_to, datetime):
                next_day = date_to + timedelta(days=1)
                clauses.append(f"{field} lt {Replicator._odata_datetime(next_day)}")
            else:
                clauses.append(f"{field} le {Replicator._odata_datetime(date_to)}")
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

    def _full_load_lease_guard(self):
        """
        Условие «объект, который сейчас пишется снимком, всё ещё наш» — для вшивания в саму
        запись (см. DBWriter._still_ours).

        None, когда снимок не идёт: в режиме изменений guard'ов нет вовсе, а прямой вызов
        full_load на пустой базе может идти и без реестра. Условие поэтому строится только
        по факту захвата.

        Захват объекта живёт в реестре onecdc_metadata_objects, и условие — коррелированный
        EXISTS по нему. Дороже обычного guard'а на один подзапрос по первичному ключу небольшой
        таблицы, и это единственный способ проверить владение В МОМЕНТ записи, а не до неё.
        """
        objects = self._full_load_claim.held_objects()
        table = self.metadata.objects_table
        if not objects or table is None:
            return None
        return exists().where(
            table.c.object_full_name.in_(sorted(objects)),
            table.c[OWNER_FIELD] == self._full_load_claim.owner,
            table.c[HEARTBEAT_FIELD] >= DB_NOW_WITHOUT_TIMEZONE
            - timedelta(seconds=CLAIM_HEARTBEAT_TTL))

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
        self._keep_node_lease = True
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
        self._keep_node_lease = False
        self.close()

    def close(self) -> None:
        """
        Штатная остановка: отпускает аренду узла обмена и гасит поток отметки живости.

        Без неё сменщик ждал бы истечения TTL — а он у роли длинный (LEASE_ROLE_TTL, четверть
        часа). Разница между аккуратным рестартом и `kill -9` ровно в этом: при первом узел
        подхватывается мгновенно, при втором — через TTL.
        """
        self._node_lease.release(self._queue_guid)
        self._node_lease.close()

    @contextmanager
    def claim_full_load(self, object_full_name: str):
        """
        Занимает объект под полную выгрузку на время блока: отдаёт True, если объект свободен, и
        False, если его уже выгружает кто-то другой (фоновая выгрузка репликатора или другое
        расписание). Множество занятых — общее с _dispatch_full_loads, поэтому claim видят обе
        стороны.

        Нужен вызывающим извне цикла — прежде всего FullLoadCron: две одновременные выгрузки одного
        объекта данные не портят (у каждого снимка свой full_load_started_at, см. DBWriter.save),
        но дают 1С двойную работу и две параллельные строки в onecdc_replicator_log.

        Заслон один и живёт в БД — отметкой в onecdc_metadata_objects (см. full_load_claim). Множества
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
