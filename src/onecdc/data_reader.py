from __future__ import annotations

import requests
from typing import Any
from collections import Counter, UserDict
from datetime import datetime, date
from urllib.parse import quote
import uuid

import xmltodict

from cdc_1c.metadata_reader import (ACCOUNTING_REGISTER_TYPE, COMPOSITE_VALUE_SUFFIX,
                                    ENTITY_TYPES, EXT_DIMENSIONS_FIELDS, REGISTER_TYPES,
                                    SUPPORTED_TYPES, MetadataReader1C, resolve_timeout)
from cdc_1c.name_mapper import NameMapper1C
from cdc_1c.common_functions import (format_bytes, odata_datetime_value,
                                     parse_object_full_name, raise_for_status)
from cdc_1c.logging_config import get_logger

logger = get_logger(__name__)


def _json_safe(value: Any) -> Any:
    """Приводит значение к JSON-сериализуемому виду: UUID -> str, datetime/date -> ISO."""
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value

# REGISTER_TYPES / ENTITY_TYPES / SUPPORTED_TYPES импортируются из metadata_reader: список
# поддерживаемых классов объектов должен быть один на всю библиотеку.
METADATA_POSTFIXES = ('_RecordType','_RowType','_Balance','_Turnover','_BalanceAndTurnover')
ODATA_PREFIX = 'StandardODATA.'
# Примитивные типы OData в поле <поле>_Type: значение составного типа — не ссылка (число, строка,
# дата, булево), и в uuid-колонку оно не ложится.
EDM_TYPE_PREFIX = 'Edm.'

# Поле 1С с пометкой удаления у документов и справочников.
DELETION_MARK_FIELD = 'DeletionMark'
# Поле 1С «признак активности строки» у записей регистров: неактивная запись не участвует
# в итогах 1С, поэтому не должна участвовать и в наших расчетах.
ACTIVE_FIELD = 'Active'
# Поле 1С «версия данных» у ссылочных объектов. Меняется при КАЖДОЙ записи объекта, даже если
# ни один реквизит не изменился, поэтому в сравнении строк не участвует (см. DBWriter1C.save).
#
# Имён два, потому что документация и реальный ответ расходятся: «Приложение 12. Описание
# сущностей, предоставляемых через стандартный интерфейс OData» описывает свойство как Version
# («Версия данных»), а платформа отдаёт DataVersion — проверено и на рабочей базе, и на записанных
# ответах чужой базы в tests/responses. Расхождение системное: там же документированы Ref и
# Recorder, а на проводе приходят Ref_Key и Recorder_Key. Поэтому принимаем оба имени и берём то,
# что реально пришло в записи.
VERSION_FIELDS = ('DataVersion', 'Version')
# Спец-поле, заполняемое при загрузке. Универсальный признак «строку не учитывать»:
# пометка удаления объекта (DeletionMark), неактивная запись регистра (Active=False)
# и фиктивные записи (удаленный набор регистра, опустевшая табличная часть).
# Полное описание логики — в README_DB.md, раздел про is_deleted_or_empty.
IS_DELETED_OR_EMPTY_FIELD = 'is_deleted_or_empty'
# Спец-поле: номер пакета обмена (message_no), проставляется во все записи при чтении изменений.
EXCHANGE_MESSAGE_NO_FIELD = 'exchange_message_no'

# Номер строки внутри набора (табличная часть, набор движений регистратора). Входит в первичный
# ключ обоих, и 1С нумерует строки подряд с единицы — на этом построен ключ фиктивной записи
# (см. _make_deleted_register_record / _make_empty_table_part_record).
LINE_NUMBER_FIELD = 'LineNumber'
FIRST_LINE_NUMBER = 1

# Поля регистратора в OData. 1С называет их по-разному в зависимости от того, сколько типов
# документов может быть регистратором регистра:
# - несколько типов (составная ссылка) -> Recorder (строка) + Recorder_Type (имя типа);
# - единственный тип -> Recorder_Key (Guid), без Recorder_Type.
RECORDER_FIELDS = ('Recorder', 'Recorder_Key')
RECORDER_TYPE_FIELD = 'Recorder_Type'
# Набор записей регистра в entry регистраторного регистра (у независимого регистра его нет).
RECORD_SET_FIELD = 'd:RecordSet'

# --- Субконто регистра бухгалтерии ---
# Виртуальная таблица, отдающая движения ВМЕСТЕ с субконто. В самом наборе записей (_RowType)
# субконто нет вообще, и другого источника у них нет: соседняя функция ExtDimensions() не
# принимает ни одного параметра (ни периода, ни Top) — то есть либо вся аналитика базы одним
# ответом, либо ничего.
RECORDS_WITH_EXT_DIMENSIONS = 'RecordsWithExtDimensions'
# Ответ функции — не feed/entry, а <d:Result> со списком <d:element>; поля в элементе лежат
# ПЛОСКО, как в m:properties.
FUNCTION_RESULT_FIELD = 'd:Result'
FUNCTION_ELEMENT_FIELD = 'd:element'
# Слоты субконто: значение, тип значения и ВИД субконто — три разных поля на слот.
EXT_DIMENSION_SLOTS = (1, 2, 3)
EXT_DIMENSION_VALUE = 'd:ExtDimension{side}{slot}'
EXT_DIMENSION_VALUE_TYPE = 'd:ExtDimension{side}{slot}_Type'
EXT_DIMENSION_KIND = 'd:ExtDimensionType{side}{slot}_Key'
# Все поля вида субконто разом — по ним ищется план видов характеристик
# (см. _ensure_ext_dimension_kinds).
EXT_DIMENSION_KIND_FIELDS = tuple(EXT_DIMENSION_KIND.format(side=side, slot=slot)
                                  for side in EXT_DIMENSIONS_FIELDS
                                  for slot in EXT_DIMENSION_SLOTS)
# Класс объектов 1С, в котором лежат виды субконто, и поле предопределённого имени его элемента.
CHART_OF_CHARACTERISTIC_TYPES = 'ChartOfCharacteristicTypes'
PREDEFINED_NAME_FIELD = 'PredefinedDataName'
# Ключи внутри JSON-значения субконто.
EXT_DIMENSION_VALUE_KEY = 'value'
EXT_DIMENSION_TYPE_KEY = 'type'
# Числовые типы 1С. Нужны, чтобы согласовать два ответа платформы об одном и том же движении:
# см. _ext_dimensions_record.
NUMERIC_TYPES = ('Double', 'Int64', 'Int32', 'Int16')
# Бюджет длины СЕГМЕНТА адреса, в который сводятся периоды одного запроса за субконто
# (см. _ext_dimensions_chunk). Параметры функции 1С лежат в пути, и режет их http.sys своим
# UrlSegmentMaxLength (260 символов по умолчанию) — раньше, чем IIS дойдёт до maxUrl или
# maxQueryString. Проверено на живой 1С: 249 символов проходят, 292 дают 400 «Invalid URL».
EXT_DIMENSIONS_MAX_SEGMENT_CHARS = 260
# Ниже не опускаемся даже при отказах: короче — это уже меньше одного периода.
EXT_DIMENSIONS_MIN_SEGMENT_CHARS = 80

# Приметы отказа «адрес слишком длинный». Штатный код — 414, но IIS на превышение
# UrlSegmentMaxLength отдаёт обычный 400 и html-страницу, поэтому 400 засчитываем только по
# примете в теле: иначе любая другая ошибка 400 молча урезала бы бюджет до самого дна.
URL_TOO_LONG_CODES = (414,)
URL_TOO_LONG_MARKERS = ('Invalid URL', 'URL is invalid', 'Request URL Too Long')


def _is_url_too_long(exc: requests.HTTPError) -> bool:
    response = exc.response
    if response is None:
        return False
    if response.status_code in URL_TOO_LONG_CODES:
        return True
    return (response.status_code == 400
            and any(marker in response.text for marker in URL_TOO_LONG_MARKERS))
# Пустая ссылка 1С — нулевой GUID (в данных 1С отдаёт именно его).
EMPTY_UUID = uuid.UUID(int=0)


def _odata_literal(value: Any, type_name: str) -> str:
    """
    OData-литерал значения по типу поля (курсор по периоду, перепроверка кандидатов на пометку,
    фильтр по дате): Guid → guid'…', DateTime → datetime'…',
    числа — как есть, Boolean → true/false, остальное (String и пр.) — строка в кавычках (кавычка
    внутри экранируется удвоением). value приходит уже сконвертированным (_convert_value): UUID/datetime.
    """
    if type_name == 'Guid':
        return f"guid'{value}'"
    if type_name == 'DateTime':
        # Год — четырьмя знаками, иначе 1С отвергает литерал (см. odata_datetime_value): значения
        # сюда приходят прочитанными из самой 1С, а среди них бывает пустая дата 0001-01-01.
        v = odata_datetime_value(value) if isinstance(value, (datetime, date)) else str(value)
        return f"datetime'{v}'"
    if type_name in ('Int64', 'Int32', 'Int16', 'Double'):
        return str(value)
    if type_name == 'Boolean':
        return 'true' if value else 'false'
    return "'" + str(value).replace("'", "''") + "'"


def _odata_string_literal(value: str) -> str:
    """
    Строковый литерал для ПАРАМЕТРА функции 1С (Condition=…): значение целиком берётся в кавычки,
    а кавычки внутри удваиваются. Внутри лежит выражение в синтаксисе OData, поэтому своих кавычек
    там много (datetime'…', guid'…').
    """
    return "'" + value.replace("'", "''") + "'"


class DataObject1C(UserDict):
    def __init__(self, metadata_obj=None, records: list = []):
        super().__init__()
        self.metadata_obj = metadata_obj  # MetadataObject1C
        # Табличные части этого объекта: {имя_части: DataObject1C}. Заполняются при чтении
        # (_get_entity_records связывает владельца с его ТЧ), используются to_nested_records.
        self.table_parts: dict[str, DataObject1C] = {}
        self.data_length = 0
        self.add_records(records)

    def add_records(self, records: list):
        for record in records:
            for k,v in record.items():
                if k not in self.data.keys():
                    # встретилось новое поле, нужно добавить его во все записи, которые уже есть.
                    if self.data_length > 0:
                        self.data[k] = [None] * self.data_length
                    else:
                        self.data[k] = []
                self.data[k].append(v)

            for k in self.data.keys():
                 # если в новой записи нет какого-то поля, которое уже есть в данных, то нужно добавить это поле со значением None
                if k not in record.keys():
                    self.data[k].append(None)

            self.data_length += 1

    def to_records_mapped(self, column_mapping: dict[str, str] | None = None,
                          json_safe: bool = False) -> list[dict]:
        """
        Преобразует колоночное хранилище (dict of lists) в список записей (list of dict)
        для передачи в dbmerge. Если задан column_mapping, имена колонок заменяются на лету,
        без отдельной переименованной копии данных (значения колонок переиспользуются по ссылке).

        json_safe=True приводит значения к JSON-сериализуемому виду (UUID->str, datetime->ISO) —
        для экспорта в JSON; по умолчанию False, чтобы dbmerge получал исходные типы.
        """
        keys = list(self.data.keys())
        out_keys = [column_mapping.get(k, k) for k in keys] if column_mapping else keys
        cols = list(self.data.values())
        if json_safe:
            return [dict(zip(out_keys, (_json_safe(v) for v in row))) for row in zip(*cols)]
        return [dict(zip(out_keys, row)) for row in zip(*cols)]

    def group_by(self, key_field: str = 'Ref_Key', name_mapper: NameMapper1C | None = None,
                 json_safe: bool = False, skip_deleted: bool = False) -> dict[Any, list[dict]]:
        """
        Группирует записи объекта в {значение key_field: [записи]} одним проходом (hash group-by).
        Записи — как в to_records_mapped (с маппингом/json_safe). Индекс не хранится в объекте, а
        строится на вызов: платим только при экспорте, без накладных в add_records и без устаревания.

        skip_deleted=True пропускает записи с is_deleted_or_empty (удалённые/фиктивные строки) —
        для вложенных табличных частей: опустевшая ТЧ приходит фиктивной записью и должна дать
        пустой список, а не группу с записью-пустышкой.
        """
        col_map = name_mapper.get_column_mapping(list(self.data.keys())) if name_mapper else None
        key = name_mapper.map_field_name(key_field) if name_mapper else key_field
        del_key = (name_mapper.map_field_name(IS_DELETED_OR_EMPTY_FIELD)
                   if name_mapper else IS_DELETED_OR_EMPTY_FIELD)
        grouped: dict[Any, list[dict]] = {}
        for row in self.to_records_mapped(col_map, json_safe=json_safe):
            if skip_deleted and row.get(del_key):
                continue
            grouped.setdefault(row.get(key), []).append(row)
        return grouped

    def to_nested_records(self, name_mapper: NameMapper1C | None = None,
                          json_safe: bool = False) -> list[dict]:
        """
        Записи этого объекта (list of dict) с вложенными табличными частями — например, чтобы
        отправить во внешний приёмник (RabbitMQ и т.п.) вместо записи в БД.

        Табличные части берутся из self.table_parts (их связал контейнер при чтении), кладутся
        вложенным списком под ключом = имя части, строки группируются по Ref_Key (group_by).

        name_mapper=None → имена полей/частей как в 1С; передан — транслитерируем (как в БД).
        json_safe=True → значения JSON-сериализуемы (UUID->str, datetime->ISO), готово к json.dumps.
        """
        col_map = name_mapper.get_column_mapping(list(self.data.keys())) if name_mapper else None
        key = name_mapper.map_field_name('Ref_Key') if name_mapper else 'Ref_Key'
        records = self.to_records_mapped(col_map, json_safe=json_safe)

        for part_name, part_obj in self.table_parts.items():
            part_key = name_mapper.map_field_name(part_name) if name_mapper else part_name
            grouped = part_obj.group_by('Ref_Key', name_mapper, json_safe, skip_deleted=True)
            for rec in records:
                rec[part_key] = grouped.get(rec.get(key), [])
        return records
            


def _composite_primitive_fields(raw: dict, metadata_obj) -> dict:
    """
    Находит составные поля, в которых у ЭТОЙ записи лежит не ссылка, и отдаёт {поле: сырое значение}.

    Тип конкретного значения приходит в парном <поле>_Type: у ссылок это имя объекта 1С, у
    примитивов — Edm.Double / Edm.String / Edm.DateTime и т.п. Метаданные OData объявляют само поле
    строкой и о составе типа не говорят ничего, поэтому решать приходится по каждой записи.

    Такие значения уходят в соседнюю колонку <поле>_Value текстом, а в uuid-колонке остаётся NULL.
    Почему не одной текстовой колонкой на всё: ссылки в ней перестали бы джойниться с ключами
    других таблиц, а это основной сценарий. Почему не только uuid: непреобразуемое значение роняло
    вставку всей пачки, и объект не грузился вовсе («Дополнительные реквизиты» с числом в Значение).
    """
    primitives = {}
    for field_name, value in raw.items():
        if not field_name.endswith('_Type') or not isinstance(value, str):
            continue
        base_name = field_name.removesuffix('_Type')
        if base_name in raw and base_name + COMPOSITE_VALUE_SUFFIX in metadata_obj.keys():
            if value.startswith(EDM_TYPE_PREFIX):
                primitives[base_name] = raw[base_name]
    return primitives


class DataReader1C(UserDict):
    def __init__(self, odata_url: str, metadata: MetadataReader1C,
                 odata_auth: tuple[str, str] | None = None,
                 request_timeout: float | None = None):
        super().__init__()
        self.odata_url = odata_url
        self.metadata = metadata
        self.odata_auth = odata_auth
        self.request_timeout = request_timeout
        # номер пакета обмена, проставляется в записи при чтении изменений
        self.exchange_message_no = None  

        # Размер последнего ответа 1С в байтах — по нему вызывающий подбирает размер страницы
        # (вес одной entry у разных объектов различается на порядки, см. Replicator1C.full_load).
        self.last_response_bytes = 0

        # Объекты, ради неизвестного поля которых метаданные уже перечитывались (см. _get_record_fields).
        # Без этого каждая запись с полем вне $metadata давала бы свой полный GET $metadata + dbmerge.
        self._metadata_refreshed_for: set[str] = set()

        # Бюджет длины сегмента адреса для запросов за субконто. Опускается отказом сервера и
        # обратно не растёт — как потолок размера страницы у полной выгрузки
        # (см. _ext_dimensions_chunk).
        self._ext_dimensions_segment_limit = EXT_DIMENSIONS_MAX_SEGMENT_CHARS
        
        # {регистр: {вид субконто (Ref_Key): предопределённое имя}} — читаемые ключи JSON субконто.
        # Строится один раз на процесс, см. _ensure_ext_dimension_kinds.
        self._ext_dimension_kinds: dict[str, dict[str, str]] = {}

    def read_object(self, object_name: str, top: int | None = None,
                    key_fields: list[str] | None = None, extra_filter: str | None = None,
                    skip: int | None = None) -> int:
        """
        Читает объект 1С в reader (предыдущее содержимое очищается). Страница — сортировка по ключу
        ($orderby key_fields) и лимит $top; следующая берётся смещением $skip от начала выборки.

        Способ ровно один. Курсора «ключ больше последней строки» (keyset) здесь нет: в ключе
        страницы у любого объекта 1С стоит ссылка (`Ref_Key` у справочника и документа, `Recorder`
        у регистра, измерение `*_Key` у независимого регистра), а по ссылке 1С не даёт ни сравнения,
        ни осмысленной сортировки — см. DESIGN.md, «Почему страницы берутся только через $skip».
        Цену глубокого $skip платит не эта функция: глубокий объект вызывающий режет окнами по
        периоду и в каждом окне начинает смещение заново (см. Replicator1C._load_by_windows).

        key_fields (порядок = порядок сортировки):
        - справочник/документ: ['Ref_Key'];
        - регистраторный регистр: ['Recorder'] либо ['Recorder_Key'] — одна entry = целый набор
          записей регистратора, поэтому страница не рвёт набор;
        - независимый регистр (нет Ref_Key/Recorder): весь первичный ключ (Period + измерения).

        extra_filter — дополнительный OData-фрагмент $filter (например, диапазон по дате).

        Возвращает число прочитанных записей верхнего уровня (entry) — по нему вызывающий понимает,
        что страница последняя (меньше top).
        """
        key_fields = key_fields or ['Ref_Key']
        params = []
        if top is not None:
            params.append(f"$top={top}")
        if skip:
            params.append(f"$skip={skip}")
        # Сортируем не только по ключу страницы, но и по остальным полям первичного ключа.
        #
        # Зачем. $skip=N значит «пропусти первые N строк выборки», поэтому вторая страница
        # продолжает первую только если 1С упорядочила обе ОДИНАКОВО. Одного ключа страницы для
        # этого мало там, где по нему есть равные строки: у табличной части ключ страницы —
        # Ref_Key (владелец), а строк у одного владельца несколько, и все они по Ref_Key равны.
        # Порядок внутри такой ничьей 1С не обещает и от запроса к запросу может выдать разный —
        # тогда на границе страниц строки и дублируются, и теряются. Остальные поля ключа
        # (LineNumber) ничью разрешают: полный первичный ключ уникален по определению.
        #
        # Сам репликатор так табличные части НЕ грузит: они приезжают вложенными в entry своего
        # документа или справочника, то есть целой группой, и страницы владельца их не режут
        # (см. Replicator1C.run_once, list_objects). Отдельная сущность у табличной части в OData
        # всё же есть, и full_load, если его натравить прямо на неё, будет листать её этим самым
        # Ref_Key — ради такого случая ничья и разрешается. Измерено: 87 строк, страницы по 2,
        # $orderby=Ref_Key — 4 дубля и 5 потерянных строк; с LineNumber — ровно 87.
        #
        # По классам объектов ничья выглядит так (всё проверено на живой 1С):
        # - документ/справочник: ключ Ref_Key уникален, равных строк нет; $orderby по ссылке 1С
        #   разворачивает в автоупорядочивание (сортирует по представлению), но порядок при этом
        #   один и тот же от запроса к запросу — этого $skip и достаточно;
        # - независимый регистр сведений: поля ключа реальные и лежат на верхнем уровне entry,
        #   $orderby по ним работает (asc и desc дают разный порядок, несуществующее поле → 400),
        #   а ключ страницы — весь первичный ключ, то есть уникален по определению;
        # - регистр, подчинённый регистратору: одна entry — это ЦЕЛЫЙ набор записей регистратора
        #   (наверху лежат только Recorder/Recorder_Type, строки набора — внутри RecordSet).
        #   Recorder уникален по построению, ничьей нет, и страница набор не рвёт. Дополнять
        #   сортировку тут нечем: LineNumber и Период на верхнем уровне отсутствуют — и такой
        #   $orderby регистр МОЛЧА ИГНОРИРУЕТ (проверено: Recorder, Recorder+LineNumber, один
        #   LineNumber, несуществующее имя поля и Recorder desc дают 200 и один и тот же порядок).
        #   Мы их всё равно перечисляем: вреда нет, а разбирать классы объектов здесь значило бы
        #   держать вторую копию классификации. Порядок страниц у регистра держится не на нашем
        #   $orderby, а на собственном порядке 1С, и он устойчив: обход страницами по 2 против
        #   одного запроса дал тот же порядок на 114 и 593 наборах — без дублей и потерь.
        order_fields = list(key_fields)
        metadata_obj = self.metadata.get(object_name)
        for field in (metadata_obj.primary_key if metadata_obj is not None else {}):
            if field not in order_fields:
                order_fields.append(field)
        params.append("$orderby=" + ','.join(order_fields))
        filters = []
        if extra_filter:
            filters.append(extra_filter)
        if filters:
            # URL собираем строкой — кодируем пробелы сами (requests строку не кодирует); двоеточия
            # в datetime-литералах оставляем как есть (safe).
            params.append("$filter=" + quote(" and ".join(filters), safe="':"))
        query = '?' + '&'.join(params)
        url = f"{self.odata_url}/{object_name}{query}"
        response = requests.get(url, auth=self.odata_auth, timeout=resolve_timeout(self.request_timeout))
        raise_for_status(response, f'read {object_name}{query}')
        self.last_response_bytes = len(response.content)

        object_data = xmltodict.parse(response.text, force_list=('d:element', 'entry'))
        object_entries = (object_data.get('feed') or {}).get('entry') or []

        self.clear()
        self.read_data_entries(object_entries)
        logger.info('Read %s: %s entries, %s rows, %s', object_name, len(object_entries),
                    self.rows_read(), format_bytes(self.last_response_bytes))
        return len(object_entries)

    def read_accounting_register(self, object_name: str, top: int | None = None,
                                 after_period: datetime | None = None,
                                 condition: str | None = None) -> int:
        """
        Читает СТРАНИЦУ регистра бухгалтерии из виртуальной таблицы RecordsWithExtDimensions —
        движения вместе с субконто (в наборе записей субконто нет вовсе, см.
        RECORDS_WITH_EXT_DIMENSIONS). Возвращает число прочитанных движений.

        Страница здесь устроена иначе, чем у read_object, потому что у этой функции работают не
        query-опции, а собственные параметры. Проверено на живой 1С:

        | `$skip`                            | 400                                            |
        | `$orderby` и параметр `Order`      | **200 и молча проигнорированы** (порядок не тот)|
        | `Top`, `Condition`                 | работают                                       |
        | `Condition` по `Period` и `*_Key`  | работают, в том числе через `or`               |
        | `Condition` по `Recorder` как guid | 500                                            |
        | `Condition` по `Recorder` строкой  | **200 и НОЛЬ строк** (молча врёт)              |

        Отсюда курсор: `Period gt <последний период>` плюс `Top`. Ни смещения, ни сортировки нет,
        поэтому хвостовой неполный период отбрасывает вызывающий (см. Replicator1C._load_pages) —
        иначе движения одной секунды разорвались бы между страницами.

        condition — дополнительное условие (диапазон дат), объединяется с курсором по AND.
        """
        terms = []
        if after_period is not None:
            terms.append(f"Period gt {_odata_literal(after_period, 'DateTime')}")
        if condition:
            terms.append(condition)
        elements = self._request_ext_dimensions(object_name, ' and '.join(terms) or None, top)
        self._ensure_ext_dimension_kinds(object_name, elements)

        self.clear()
        self._add_records(object_name, [self._ext_dimensions_record(object_name, element)
                                        for element in elements])
        logger.info('Read %s: %s records, %s', object_name, len(elements),
                    format_bytes(self.last_response_bytes))
        return len(elements)

    def fill_ext_dimensions(self, object_name: str) -> int:
        """
        Дочитывает субконто к уже разобранным движениям регистра бухгалтерии — для ПАКЕТА
        ИЗМЕНЕНИЙ, который приносит набор записей без аналитики. Возвращает число запросов в 1С.

        Адресно, по регистратору, субконто не спросить: `Condition` по `Recorder` отвечает 500, а
        строковым литералом — 200 и ноль строк, то есть врёт молча (см. read_accounting_register).
        Зато период каждого движения известен из самого набора, а `Condition` по `Period` работает
        и понимает `or`. Поэтому спрашиваем периоды пакета (пачками, см.
        _ext_dimensions_chunk) и сшиваем ответ с движениями по паре
        (регистратор, номер строки). Движения чужих регистраторов той же секунды пары не находят.

        Движение без пары (фиктивная запись удалённого набора; проводка без аналитики) получает
        пустой JSON, а не NULL: колонка означает «субконто нет», а не «не читали».
        """
        data_object = self.get(object_name)
        if data_object is None or data_object.data_length == 0:
            return 0
        data = data_object.data
        # Регистратор регистра бухгалтерии 1С всегда отдаёт составным (Recorder + Recorder_Type),
        # но сшивка держится на его наличии — без него молча оставили бы субконто пустыми.
        if 'Period' not in data or 'Recorder' not in data or LINE_NUMBER_FIELD not in data:
            logger.warning('Cannot fill ext dimensions of %s: no Period/Recorder/%s in records',
                           object_name, LINE_NUMBER_FIELD)
            return 0

        remaining = sorted({p for p in data['Period'] if p is not None})
        index: dict[tuple, dict] = {}
        requests_made = 0
        periods_asked = len(remaining)
        while remaining:
            chunk = self._ext_dimensions_chunk(object_name, remaining)
            try:
                elements = self._request_ext_dimensions(
                    object_name, self._periods_condition(chunk), None)
            except requests.HTTPError as exc:
                # Пачка не влезла в лимит длины адреса, урезанный ниже умолчания. Делим бюджет и
                # повторяем ту же пачку короче (см. _ext_dimensions_chunk).
                if len(chunk) == 1 or not _is_url_too_long(exc):
                    raise
                self._ext_dimensions_segment_limit = max(
                    EXT_DIMENSIONS_MIN_SEGMENT_CHARS, self._ext_dimensions_segment_limit // 2)
                logger.warning('Ext dimensions request of %s was rejected as too long, '
                               'retrying with a %s-character budget',
                               object_name, self._ext_dimensions_segment_limit)
                continue
            self._ensure_ext_dimension_kinds(object_name, elements)
            for element in elements:
                key = self._ext_dimensions_key(element)
                if key is not None:
                    index[key] = self._ext_dimensions(object_name, element)
            remaining = remaining[len(chunk):]
            requests_made += 1

        columns: dict[str, list] = {field: [] for field in EXT_DIMENSIONS_FIELDS.values()}
        for recorder, line_number in zip(data['Recorder'], data[LINE_NUMBER_FIELD]):
            found = index.get((recorder, line_number)) or {}
            for field, column in columns.items():
                column.append(found.get(field, {}))
        data.update(columns)
        logger.info('Filled ext dimensions of %s: %s periods in %s requests',
                    object_name, periods_asked, requests_made)
        return requests_made

    @staticmethod
    def _periods_condition(periods: list) -> str:
        """Условие «период — один из перечисленных». `or` внутри Condition платформа понимает."""
        return ' or '.join(f"Period eq {_odata_literal(p, 'DateTime')}" for p in periods)

    def _ext_dimensions_chunk(self, object_name: str, periods: list) -> list:
        """
        Сколько периодов помещается в один запрос, по бюджету длины СЕГМЕНТА адреса.

        Меряем именно сегмент пути, а не строку запроса: параметры функции 1С лежат в адресе, и
        режет их http.sys ещё до IIS — у него свой лимит на длину одного сегмента URL
        (UrlSegmentMaxLength, по умолчанию 260 символов), про который не сказано ни в
        maxUrl, ни в maxQueryString. Проверено на живой 1С: пять периодов (сегмент 249 символов)
        проходят, шесть (292) дают 400 «Bad Request — Invalid URL» от IIS, а не ошибку 1С.

        Длину считаем по РАСКОДИРОВАННОМУ сегменту: http.sys меряет его до процентного
        декодирования адреса, и кириллица в имени объекта тут ничего не меняет — она в другом
        сегменте. Первый период кладём всегда: пачка из одного периода — это уже минимум, и если
        не влезет даже он, честнее отдать ошибку, чем зациклиться.

        Как выглядит запрос. Сам сегмент — его и меряем (кавычки внутри Condition удвоены: всё
        условие целиком является строковым параметром функции):

            RecordsWithExtDimensions(Condition='Period eq datetime''2012-01-11T12:00:00''
             or Period eq datetime''2012-01-11T12:00:01''
             or Period eq datetime''2012-01-11T12:00:02''
             or Period eq datetime''2012-01-12T12:00:02''
             or Period eq datetime''2012-01-13T12:00:00''')

        Здесь пять периодов и 259 символов — последняя пачка, которая укладывается в бюджет по
        умолчанию (260); шестой период дал бы 304. В адрес сегмент уходит как есть, кодируются
        только пробелы — кавычки и скобки остаются:

            …/odata/standard.odata/AccountingRegister_%D0%A5%D0%BE%D0%B7…/
            RecordsWithExtDimensions(Condition='Period%20eq%20datetime''2012-01-11T12:00:00''…')

        Один период стоит около 45 символов, поэтому пачка почти всегда короткая: 79 символов на
        один период, 124 на два, 214 на четыре, 259 на пять.
        """
        chunk: list = []
        for period in periods:
            candidate = chunk + [period]
            segment = self._ext_dimensions_segment(self._periods_condition(candidate), None)
            if chunk and len(segment) > self._ext_dimensions_segment_limit:
                break
            chunk = candidate
        return chunk

    @staticmethod
    def _ext_dimensions_segment(condition: str | None, top: int | None) -> str:
        """Сегмент адреса с вызовом функции, до процентного кодирования (см. _ext_dimensions_chunk)."""
        args = []
        if condition:
            args.append('Condition=' + _odata_string_literal(condition))
        if top is not None:
            args.append(f'Top={top}')
        return f"{RECORDS_WITH_EXT_DIMENSIONS}({','.join(args)})"

    def _request_ext_dimensions(self, object_name: str, condition: str | None,
                                top: int | None) -> list:
        """Запрос к RecordsWithExtDimensions; возвращает список элементов ответа (d:element)."""
        # Слэш оставляем как есть: имя функции — сегмент пути, а не значение. Закодированный
        # %2F часть веб-серверов (IIS по умолчанию) отвергает, не доводя запрос до 1С.
        path = quote(f"{object_name}/{self._ext_dimensions_segment(condition, top)}",
                     safe="/()=,':")
        url = f'{self.odata_url}/{path}'
        response = requests.get(url, auth=self.odata_auth,
                                timeout=resolve_timeout(self.request_timeout))
        raise_for_status(response, f'read {path}')
        self.last_response_bytes = len(response.content)
        result = xmltodict.parse(response.text, force_list=(FUNCTION_ELEMENT_FIELD,))
        return (result.get(FUNCTION_RESULT_FIELD) or {}).get(FUNCTION_ELEMENT_FIELD) or []

    def _ext_dimensions_key(self, element: dict) -> tuple | None:
        """(регистратор, номер строки) элемента виртуальной таблицы — ключ сшивки с движением."""
        recorder = element.get('d:Recorder')
        line_number = element.get('d:' + LINE_NUMBER_FIELD)
        if not isinstance(recorder, str) or not isinstance(line_number, str):
            return None
        return self._convert_value(recorder, 'Guid'), self._convert_value(line_number, 'Int64')

    def _ext_dimensions_record(self, object_name: str, element: dict) -> dict:
        """
        Движение из виртуальной таблицы: поля самого регистра разбираются как обычно, слоты
        субконто сворачиваются в JSON.

        Поля, которых в описании регистра нет, отбрасываем ЗДЕСЬ, а не в _get_record_fields:
        виртуальная таблица шире движения (PointInTime, сами слоты), и без фильтра каждое такое
        поле заводило бы в таблице лишнюю колонку и перечитывало метаданные.
        """
        metadata_obj = self.metadata[object_name]
        properties = {k: v for k, v in element.items()
                      if k.startswith('d:') and k.removeprefix('d:') in metadata_obj}
        record = self._get_record_fields(properties, object_name)

        # Незаполненное числовое поле платформа отдаёт ПО-РАЗНОМУ, смотря откуда его читать: в
        # наборе записей это <d:КоличествоDr>0</d:КоличествоDr>, в виртуальной таблице —
        # <d:КоличествоDr m:null="true"/>. Проверено на одном и том же движении. Разница не
        # безобидна: пакет изменений приходит набором записей, полная выгрузка — виртуальной
        # таблицей, и без выравнивания они переписывали бы друг друга вечно — каждый прогон
        # отчитывался бы изменёнными строками, хотя в 1С ничего не менялось.
        # Выравниваем на НУЛЕ, а не на NULL: у ресурса регистра пустого значения не бывает вовсе,
        # пустой ресурс в 1С — это ноль, и набор записей говорит ровно это. NULL в этих колонках
        # остаётся признаком погашенной строки (см. DBWriter1C._resource_reset_values).
        for field, type_name in metadata_obj.items():
            if type_name in NUMERIC_TYPES and record.get(field) is None:
                record[field] = 0

        record.update(self._ext_dimensions(object_name, element))
        return record

    def _ensure_ext_dimension_kinds(self, object_name: str, elements: list) -> None:
        """
        Готовит карту {вид субконто (Ref_Key): предопределённое имя} для регистра — по ней ключи
        JSON становятся читаемыми (см. _ext_dimensions). Считается один раз на процесс.

        План видов характеристик, хранящий виды субконто, приходится ИСКАТЬ: в поле
        `ExtDimensionTypeDr1_Key` лежит голый Guid, тип у него в `$metadata` — `Edm.Guid`, а
        навигационной ссылки на владельца 1С в этом ответе не отдаёт (проверено: у записи
        виртуальной таблицы её нет ни у одного поля вида субконто). Поэтому берём первый
        встретившийся вид и спрашиваем прямым адресом каждый ПВХ конфигурации: чей 200 — тот и
        нужен. В демо-базе бухгалтерии планов семь, то есть максимум семь дешёвых запросов, и
        только пока карта не построена.

        Если в пачке видов субконто не было вовсе, карту НЕ фиксируем: искать не по чему, а
        следующая пачка может их принести.
        """
        if object_name in self._ext_dimension_kinds:
            return
        sample = next((value for element in elements
                       for value in map(element.get, EXT_DIMENSION_KIND_FIELDS)
                       if isinstance(value, str)), None)
        if sample is None:
            return

        chart_name = self._find_ext_dimension_chart(sample)
        if chart_name is None:
            # Ключами останутся Guid — это рабочий вариант (вид субконто join-ится к ПВХ), просто
            # менее читаемый. Молчать нельзя: разница видна в данных, и объяснить её должно тут.
            logger.warning('Ext dimension kinds of %s are not resolvable: no chart of '
                           'characteristic types holds %s — JSON keys stay GUIDs',
                           object_name, sample)
            self._ext_dimension_kinds[object_name] = {}
            return

        names = self._read_ext_dimension_kinds(chart_name)
        logger.info('Ext dimension kinds of %s: %s names from %s',
                    object_name, len(names), chart_name)
        self._ext_dimension_kinds[object_name] = names

    def _find_ext_dimension_chart(self, kind_key: str) -> str | None:
        """Какой план видов характеристик содержит этот вид субконто. None — ни один."""
        for chart_name in self.metadata:
            if not chart_name.startswith(CHART_OF_CHARACTERISTIC_TYPES):
                continue
            path = quote(f"{chart_name}(guid'{kind_key}')", safe="/()='")
            response = requests.get(f'{self.odata_url}/{path}?$select=Ref_Key',
                                    auth=self.odata_auth,
                                    timeout=resolve_timeout(self.request_timeout))
            # Отсутствие элемента 1С сообщает честным 404 — это ответ, а не ошибка.
            if response.status_code == 404:
                continue
            raise_for_status(response, f'read {path}')
            return chart_name
        return None

    def _read_ext_dimension_kinds(self, chart_name: str) -> dict[str, str]:
        """
        {Ref_Key: PredefinedDataName} всех элементов плана видов характеристик. Планов видов
        субконто много не бывает (в демо-базе бухгалтерии 50 элементов), поэтому читаем целиком и
        без страниц.

        Элемент без предопределённого имени (вид субконто, заведённый пользователем руками) в
        карту не попадает — ключом у него останется Guid.
        """
        query = f'?$select=Ref_Key,{PREDEFINED_NAME_FIELD}'
        response = requests.get(f'{self.odata_url}/{quote(chart_name)}{query}',
                                auth=self.odata_auth,
                                timeout=resolve_timeout(self.request_timeout))
        raise_for_status(response, f'read {chart_name}{query}')
        entries = (xmltodict.parse(response.text, force_list=('entry',))
                   .get('feed') or {}).get('entry') or []
        names = {}
        for entry in entries:
            properties = (entry.get('content') or {}).get('m:properties') or {}
            ref_key = properties.get('d:Ref_Key')
            name = properties.get('d:' + PREDEFINED_NAME_FIELD)
            if isinstance(ref_key, str) and isinstance(name, str) and name:
                names[ref_key] = name
        return names

    def _ext_dimensions(self, object_name: str, element: dict) -> dict[str, dict]:
        """
        Слоты субконто одного движения -> {колонка: {вид субконто: {"value", "type"}}}.

        Ключ — ВИД субконто, а не номер слота: номер смысла не имеет (субконто1 счёта 10 это
        Номенклатура, счёта 60 — Контрагенты). Именем вида служит его предопределённое имя из
        плана видов характеристик (`РаботникиОрганизаций`), потому что читать и писать в запросах
        предстоит именно его; вид, которого в карте нет, остаётся Guid — см.
        _ensure_ext_dimension_kinds.

        Слот без вида или без значения пропускается: назвать такое субконто нечем. Тип значения
        храним рядом со значением — субконто бывает и не ссылкой (в демо-базе бухгалтерии
        встречаются значения перечислений, `СтавкиНДС` = `НДС18`), и тогда это единственный способ
        понять, что лежит в value.
        """
        names = self._ext_dimension_kinds.get(object_name) or {}
        columns: dict[str, dict] = {}
        for side, field in EXT_DIMENSIONS_FIELDS.items():
            found: dict[str, dict] = {}
            for slot in EXT_DIMENSION_SLOTS:
                kind = element.get(EXT_DIMENSION_KIND.format(side=side, slot=slot))
                value = element.get(EXT_DIMENSION_VALUE.format(side=side, slot=slot))
                if not isinstance(kind, str) or not isinstance(value, str):
                    continue
                key = names.get(kind, kind)
                found[key] = {EXT_DIMENSION_VALUE_KEY: value}
                value_type = element.get(EXT_DIMENSION_VALUE_TYPE.format(side=side, slot=slot))
                if isinstance(value_type, str):
                    found[key][EXT_DIMENSION_TYPE_KEY] = (value_type.removeprefix(ODATA_PREFIX)
                                                          .removeprefix(EDM_TYPE_PREFIX))
            columns[field] = found
        return columns

    def read_by_key(self, object_name: str, key_values: dict) -> int:
        """
        Читает ОДИН объект прямым адресом `Объект(Поле='значение',...)`, а не выборкой с $filter.
        Возвращает 1, если объект есть, и 0, если его нет; прочитанное кладётся в reader, как и
        после read_object.

        Нужно там, где `$filter` по ключу не работает в принципе, — у регистра, подчинённого
        регистратору. Проверено на живой 1С, и варианты тут такие:

        | запрос | ответ |
        |---|---|
        | `$filter=Recorder eq guid'…'` | 500 «Нельзя сравнивать поля неограниченной длины» |
        | `$filter=Recorder eq '…'` (строкой) | **200 и НОЛЬ строк** |
        | `Объект(Recorder='…',Recorder_Type='StandardODATA.…')` | 200, ровно одна entry |

        Средняя строка — ловушка, из-за которой этот метод и появился: строковый литерал ошибки не
        даёт, но и не находит ничего. Перепроверка на таком «ответе» решила бы, что в 1С не осталось
        ни одного набора, и пометила бы удалёнными ВСЕ строки регистра.

        Имя типа в ключе пишется с префиксом `StandardODATA.` — в данных мы его снимаем
        (см. _get_record_fields), а в адресе он обязателен: без него 1С отвечает 400 «Недопустимое
        значение … для свойства составного типа».

        Ответ здесь — одиночный `<entry>` без обёртки `<feed>`, а отсутствие объекта 1С сообщает
        честным 404 «Экземпляр сущности не найден»: это ответ, а не ошибка, поэтому наружу он не
        пробрасывается.
        """
        key = ','.join(f"{field}='{value}'" for field, value in key_values.items())
        path = quote(f"{object_name}({key})", safe="()=',")
        response = requests.get(f"{self.odata_url}/{path}", auth=self.odata_auth,
                                timeout=resolve_timeout(self.request_timeout))
        self.clear()
        if response.status_code == 404:
            return 0
        raise_for_status(response, f'read {object_name}({key})')
        self.last_response_bytes = len(response.content)
        entry = xmltodict.parse(response.text, force_list=('d:element',)).get('entry')
        if not entry:
            return 0
        self.read_data_entries([entry])
        return 1

    def read_date_bound(self, object_name: str, date_field: str, *, newest: bool,
                        extra_filter: str | None = None) -> datetime | None:
        """
        Самая ранняя (newest=False) или самая поздняя (newest=True) дата объекта — одной строкой:
        `$top=1` с сортировкой по этому полю. Нужна, чтобы обойти полную выгрузку окнами по
        периоду и не гонять `$skip` по всей таблице (см. Replicator1C.full_load).

        Запрос дешёвый именно потому, что поле даты у документа входит в индекс: 1С отдаёт первую
        строку упорядоченной выборки, а не сортирует всё.

        ВАЖНО — годится НЕ ДЛЯ ВСЯКОГО объекта. У регистра, подчинённого регистратору, entry — это
        набор записей, даты на верхнем уровне нет вовсе, и `$orderby` по ней платформа принимает с
        кодом 200 и МОЛЧА ИГНОРИРУЕТ: проверено на живой 1С, что `$orderby=Period`,
        `$orderby=Period desc` и запрос без сортировки отдают одну и ту же первую entry. Так что
        «первая строка упорядоченной выборки» там не значит ничего, и доставать дату из вложенного
        RecordSet, чтобы «починить» этот метод, НЕЛЬЗЯ — вернётся не граница, а произвольное
        значение. Кому этот метод годится, решает Replicator1C._supports_date_bounds.

        None — объект пуст (или пуст заданный диапазон), а также если в первой строке даты не
        оказалось (тогда пишем warning): границу взять неоткуда.
        """
        order = date_field + (' desc' if newest else '')
        params = ["$top=1", "$orderby=" + quote(order, safe="")]
        if extra_filter:
            params.append("$filter=" + quote(extra_filter, safe="':"))
        query = '?' + '&'.join(params)
        response = requests.get(f"{self.odata_url}/{object_name}{query}", auth=self.odata_auth,
                                timeout=resolve_timeout(self.request_timeout))
        raise_for_status(response, f'read {object_name}{query}')

        entries = (xmltodict.parse(response.text, force_list=('entry',)).get('feed') or {}).get('entry') or []
        if not entries:
            return None
        properties = (entries[0].get('content') or {}).get('m:properties') or {}
        value = properties.get('d:' + date_field)
        if not isinstance(value, str):
            logger.warning('No %s in the first row of %s — period partitioning is off',
                           date_field, object_name)
            return None
        return self._convert_value(value, 'DateTime', context=f'{object_name}.{date_field}')

    def rows_read(self) -> int:
        """Сколько строк дали прочитанные entry — с учётом табличных частей и наборов записей
        (одна entry регистра/документа разворачивается в сотни и тысячи строк)."""
        return sum(data_object.data_length for data_object in self.values())

    def read_data_entries(self, object_entries: list) -> Counter:
        """
        Разбирает entry ответа 1С в объекты reader. Возвращает счётчик entry по объектам —
        вызывающий логирует итог одной строкой: entry в ответе бывают тысячами, и лог на каждую
        забивает вывод (см. read_object / ChangeReader1C.read_changes).

        Объекты неподдерживаемых классов (см. SUPPORTED_TYPES) пропускаются с предупреждением в
        логе: пакет изменений подтверждается целиком, поэтому такие изменения 1С больше не пришлёт —
        молчать об этом нельзя. Лог один на пакет, а не на entry.

        Уровень именно WARNING, а не ERROR: неподдерживаемый класс — это состав плана обмена, а не
        сбой прогона. Останавливать на нём репликацию нечем и незачем — пакет подтверждается, цикл
        идёт дальше, а решение (убрать объект из плана обмена или ждать поддержки) принимает
        человек, прочитав предупреждение.
        """
        parsed: Counter = Counter()
        unsupported: Counter = Counter()
        for object_entry in object_entries:

            object_full_name = (object_entry.get('category') or {}).get('@term')
            object_name, object_type = parse_object_full_name(object_full_name)

            parsed[object_name] += 1

            if object_type not in SUPPORTED_TYPES:
                unsupported[object_full_name] += 1
                continue

            if object_name and self.metadata.get(object_name) is None:
                self.metadata.get_metadata()
                if self.metadata.get(object_name) is None:
                    logger.warning(f'Metadata not found for {object_name}')

            properties = (object_entry.get('content') or {}).get('m:properties') or {}

            if object_type in REGISTER_TYPES:
                self._get_register_records(object_name, properties)

            if object_type in ENTITY_TYPES:
                self._get_entity_records(object_name, properties)

        if unsupported:
            logger.warning(
                "Objects of unsupported classes were skipped and their changes are lost "
                "(the exchange message is confirmed as a whole, 1C will not send them again): %s",
                ', '.join(f'{name} ({n} entries)' for name, n in unsupported.items()))

        return parsed


    def _add_records(self, object_name, new_records: list):
        if new_records:
            if object_name in self.keys():
                self[object_name].add_records(new_records)
            else:
                metadata_obj = self.metadata.get(object_name)
                self[object_name] = DataObject1C(metadata_obj=metadata_obj, records=new_records)

    @staticmethod
    def _convert_value(value: Any, type_name: str, context: str = '') -> Any:
        # Конвертируем значение в нужный тип данных на основе метаданных.
        if value is None:
            return None
        if not isinstance(value, str):
            logger.warning(f'Expected value to be a string for conversion, got {type(value).__name__}: {value!r}')
            return value
        try:
            if type_name == 'Boolean':
                return value.lower() == 'true'
            if type_name in ('Int64', 'Int32', 'Int16'):
                return int(value)
            if type_name == 'Double':
                return float(value)
            if type_name == 'DateTime':
                return datetime.fromisoformat(value)
            if type_name == 'Guid':
                if value == '00000000-0000-0000-0000-000000000000':
                    return None
                else:
                    return uuid.UUID(value)
        except (ValueError, TypeError) as e:
            # Раньше здесь возвращалось исходное значение — и падала вся пачка на вставке
            # (одно '6.4' в uuid-колонке останавливало загрузку объекта навсегда). Пишем NULL:
            # объект грузится, а строка с проблемой видна и в логе, и в БД.
            logger.warning('Failed to convert %s value %r to type %s: %s — writing NULL',
                           context or 'field', value, type_name, e)
            return None
        return value

    def _get_record_fields(self, properties: dict, object_name: str | None = None) -> dict:
        if object_name not in self.metadata.keys():
            self.metadata.get_metadata()
            if object_name not in self.metadata.keys():
                logger.error(f'Metadata not found for {object_name}')
                raise ValueError(f'Metadata not found for {object_name}')            
        
        metadata_obj = self.metadata[object_name]

        # Сырые значения собираем до конвертации: у составного поля тип значения виден только
        # из парного <поле>_Type, а решать надо ДО приведения к Guid — иначе число уже потеряно.
        raw = {}
        for k, v in properties.items():
            if k.startswith('d:') and isinstance(v, str):
                field_name = k.removeprefix('d:')
                # У поля _Type в значении полное имя типа ("StandardODATA.Catalog_Контрагенты");
                # префикс убираем, остаётся имя объекта 1С либо примитив вида Edm.Double.
                raw[field_name] = v.removeprefix(ODATA_PREFIX) if field_name.endswith('_Type') else v

        primitives = _composite_primitive_fields(raw, metadata_obj)

        fields = {}
        for field_name, value in raw.items():
            if field_name not in metadata_obj.keys():
                # поле не найдено в метаданных, пробуем их перечитать — но не более одного раза
                # на объект, иначе поле, которого в $metadata нет вовсе (например, отброшенный
                # по неизвестному типу реквизит), даёт по запросу метаданных на каждую запись.
                if object_name not in self._metadata_refreshed_for:
                    self._metadata_refreshed_for.add(object_name)
                    self.metadata.get_metadata()
                    metadata_obj = self.metadata[object_name]
                if field_name not in metadata_obj.keys():
                    logger.warning(f'Metadata field {field_name} not found for object {object_name}')


            type_name = metadata_obj.get(field_name) or 'String'

            if field_name.endswith('_Type'):
                # В колонке _Type храним имя типа без префикса поставщика: у ссылок это уже сделано
                # выше (StandardODATA.), у примитивов убираем Edm. — в БД полезно 'Double', а не
                # 'Edm.Double'. Распознавание примитива идёт по сырому значению (см. primitives).
                value = value.removeprefix(EDM_TYPE_PREFIX)

            if field_name in primitives:
                # Не ссылка: в uuid-колонке ей места нет — значение уходит в соседнюю
                # текстовую <поле>_Value как есть, здесь остаётся NULL.
                fields[field_name + COMPOSITE_VALUE_SUFFIX] = value
                fields[field_name] = None
                continue

            converted = self._convert_value(value, type_name,
                                            context=f'{object_name}.{field_name}')

            if converted is None and field_name in metadata_obj.primary_key:
                # Поля первичного ключа в целевой таблице NOT NULL, а пустую ссылку 1С
                # (нулевой GUID) _convert_value превращает в None — для ключа оставляем
                # дефолт по типу (нулевой GUID), иначе строка не вставится.
                converted = self._default_key_value(type_name)

            fields[field_name] = converted

        # Поля ключа в целевой таблице NOT NULL, а 1С пустое измерение присылает пустым элементом
        # (в json — null) либо не присылает вовсе: пустая строка в «ИдентификаторСтроки» регистра
        # приходит именно так. Без дефолта такая запись роняет вставку всей пачки по NOT NULL.
        # Поля регистратора пропускаем: в строках набора их нет по определению, и проставляются
        # они уровнем выше, из entry (_entry_recorder_fields) — дефолт тут забил бы их нулевым guid.
        for key_field, key_type in metadata_obj.primary_key.items():
            if (fields.get(key_field) is None
                    and key_field not in RECORDER_FIELDS and key_field != RECORDER_TYPE_FIELD):
                fields[key_field] = self._default_key_value(key_type)

        # Соседняя колонка составного поля заполняется у КАЖДОЙ записи, даже когда значение —
        # ссылка и колонка остаётся пустой: иначе состав полей плавал бы от записи к записи.
        for field_name in raw:
            if field_name.endswith('_Type'):
                value_name = field_name.removesuffix('_Type') + COMPOSITE_VALUE_SUFFIX
                if value_name in metadata_obj.keys():
                    fields.setdefault(value_name, None)

        # Спец-поле «строку не учитывать»: пометка удаления у документа/справочника либо
        # неактивная запись регистра. Active сравниваем именно с False: у объектов его нет вовсе,
        # а None означает «1С не прислала» — молча гасить такую строку нельзя.
        fields[IS_DELETED_OR_EMPTY_FIELD] = (bool(fields.get(DELETION_MARK_FIELD))
                                             or fields.get(ACTIVE_FIELD) is False)
        fields[EXCHANGE_MESSAGE_NO_FIELD] = self.exchange_message_no

        return fields

    @staticmethod
    def _default_key_value(type_name: str) -> Any:
        # Дефолтное значение поля ключа (удаленная запись регистра, пустая ссылка в ключе):
        # '' для строк, 0 для чисел, нулевой GUID для ссылок — чтобы получить непустой
        # составной ключ (в целевой таблице поля ключа NOT NULL).
        if type_name == 'String':
            return ''
        if type_name in ('Int64', 'Int32', 'Int16', 'Double'):
            return 0
        if type_name == 'Boolean':
            return False
        if type_name == 'DateTime':
            return datetime(1, 1, 1)  # пустая дата 1С
        if type_name == 'Guid':
            return EMPTY_UUID  # пустая ссылка 1С
        return None

    def _entry_recorder_fields(self, object_name: str, properties: dict) -> dict:
        """
        Поля регистратора из entry набора записей: Recorder + Recorder_Type (составной
        регистратор) либо Recorder_Key (единственный тип регистратора, см. RECORDER_FIELDS).

        Ключ набора лежит на уровне entry, и внутри строк набора 1С его не повторяет (хотя в
        $metadata он объявлен и в _RowType) — поэтому значения берём отсюда и проставляем в
        каждую строку набора, как Ref_Key владельца в строки табличной части.
        """
        metadata_obj = self.metadata[object_name]
        fields = {}
        for field in RECORDER_FIELDS:
            value = properties.get('d:' + field)
            if isinstance(value, str):
                fields[field] = self._convert_value(value, metadata_obj.get(field) or 'Guid')

        recorder_type = properties.get('d:' + RECORDER_TYPE_FIELD)
        if isinstance(recorder_type, str):
            fields[RECORDER_TYPE_FIELD], _ = parse_object_full_name(recorder_type)
        return fields

    def _make_deleted_register_record(self, object_name: str, properties: dict) -> dict | None:
        """
        Создает запись для удаленного набора записей регистра (пришел пустой RecordSet).
        Заполняет полный первичный ключ из метаданных дефолтами по типу поля (кроме номера строки,
        см. _first_line_number) и проставляет реальные поля регистратора из entry
        (_entry_recorder_fields).

        Возвращает None, если полей регистратора в entry нет — тогда непонятно, чей набор
        удалять, и фиктивную запись создавать нельзя (ключ остался бы пустым).
        """
        metadata_obj = self.metadata[object_name]
        record = {field: self._default_key_value(type_name)
                  for field, type_name in metadata_obj.primary_key.items()}
        self._first_line_number(record, metadata_obj.primary_key)

        recorder_fields = self._entry_recorder_fields(object_name, properties)
        if not any(field in recorder_fields for field in RECORDER_FIELDS):
            logger.error(f'No recorder field in empty record set of {object_name}')
            return None
        record.update(recorder_fields)

        record[IS_DELETED_OR_EMPTY_FIELD] = True
        record[EXCHANGE_MESSAGE_NO_FIELD] = self.exchange_message_no
        return record

    @staticmethod
    def _first_line_number(record: dict, primary_key: dict) -> None:
        """
        Ставит фиктивной записи номер строки 1 вместо дефолтного 0.

        Смысл — чтобы такая запись не жила вечно. 1С нумерует строки набора (табличная часть, набор
        движений регистратора) подряд с единицы, поэтому как только набор снова наполнится, реальная
        первая строка придет с тем же ключом и перезапишет фиктивную: значения настоящие,
        is_deleted_or_empty=False. С номером 0 такого ключа в 1С не бывает, и запись осталась бы
        в таблице навсегда (при delete_mode='mark' удалить ее было бы уже некому).

        Если набор вдруг придет без первой строки, фиктивная запись просто останется помеченной —
        то есть поведение будет как при номере 0, не хуже.
        """
        if LINE_NUMBER_FIELD in primary_key:
            record[LINE_NUMBER_FIELD] = FIRST_LINE_NUMBER

    def _make_empty_table_part_record(self, table_part_name: str, ref_key: Any) -> dict:
        """
        Создает запись для опустевшей табличной части (пришла без строк), по аналогии с удаленным
        набором регистра. Полный ключ из метаданных заполняется дефолтами (кроме номера строки, см.
        _first_line_number), а Ref_Key — реальным значением владельца, чтобы scoped-удаление по
        Ref_Key убрало старые строки.
        """
        primary_key = self.metadata[table_part_name].primary_key
        record = {field: self._default_key_value(type_name)
                  for field, type_name in primary_key.items()}
        self._first_line_number(record, primary_key)
        record['Ref_Key'] = ref_key
        record[IS_DELETED_OR_EMPTY_FIELD] = True
        record[EXCHANGE_MESSAGE_NO_FIELD] = self.exchange_message_no
        return record

    def _get_register_records(self, object_name: str, properties: dict):
        """
        Функция забирает записи регистра.

        Регистраторный регистр приходит набором по одному регистратору (d:RecordSet с движениями);
        пустой набор = набор удалён (фиктивная запись). Независимый регистр сведений приходит плоско —
        поля прямо в properties, без RecordSet, как у справочника/документа (форма прямого
        чтения GET /InformationRegister_X).

        Признак регистраторного регистра — наличие RecordSet, а не Recorder: поле регистратора 1С
        называет то Recorder (+Recorder_Type), то Recorder_Key — см. RECORDER_FIELDS.

        Текущий объект это словарь, в котором ключом является объект 1С, например "Document_ЗаказКлиента",
        значения содержат массив записей в виде list of dict.
        """
        if RECORD_SET_FIELD not in properties:
            # Независимый регистр сведений: одна плоская запись, поля прямо в properties.
            self._add_records(object_name, [self._get_record_fields(properties, object_name)])
            return

        record_set = properties.get(RECORD_SET_FIELD)
        records = (record_set.get('d:element') or []) if isinstance(record_set, dict) else []

        if records:
            # Регистратор лежит на уровне entry и внутри строк набора не повторяется —
            # проставляем его в каждую строку (setdefault: если 1С всё же прислала, не трогаем).
            recorder_fields = self._entry_recorder_fields(object_name, properties)
            new_records = []
            for record in records:
                row = self._get_record_fields(record, object_name)
                for field, value in recorder_fields.items():
                    row.setdefault(field, value)
                new_records.append(row)
        else:
            # Регистраторный регистр с пустым набором — набор записей удалён.
            deleted_record = self._make_deleted_register_record(object_name, properties)
            new_records = [deleted_record] if deleted_record is not None else []

        self._add_records(object_name, new_records)

    def _get_record_table_parts(self, properties):
        """
        Ищем табличные части в свойствах объекта.
        Если тип данных dict и если префикс в названии 'd:', то будем считать что это табличная часть
        """
        table_parts = {k.removeprefix('d:'): v for k, v in properties.items()
                       if k.startswith('d:') and isinstance(v, dict) and v.get('@xsi:nil') != 'true'}
        return table_parts

    def _get_entity_records(self, object_name: str, properties: dict):
        """
        Функция забирает поля документа или справочника.
        Запись одна, но могут быть табличные части, которые будут записаны в отдельные элементы структуры changes
        """
        fields = self._get_record_fields(properties, object_name)
        self._add_records(object_name, [fields])

        # Строки табличных частей приходят без ссылки на владельца — проставляем Ref_Key документа.
        ref_key = fields.get('Ref_Key')
        # Пометку удаления документа/справочника распространяем на его табличные части.
        parent_deleted = fields.get(IS_DELETED_OR_EMPTY_FIELD)

        table_parts = self._get_record_table_parts(properties)

        for table_part_key, table_part in table_parts.items():
            table_part_full_name = table_part.get('@m:type')
            if table_part_full_name:
                table_part_name, _ = parse_object_full_name(table_part_full_name)
                table_part_rows = table_part.get('d:element') or []
                if table_part_rows:
                    for table_part_row in table_part_rows:
                        row = self._get_record_fields(table_part_row, table_part_name)
                        row['Ref_Key'] = ref_key
                        row[IS_DELETED_OR_EMPTY_FIELD] = parent_deleted
                        self._add_records(table_part_name, [row])
                else:
                    # Табличная часть пришла без строк — добавляем фиктивную запись,
                    # чтобы scoped-удаление по Ref_Key убрало ранее сохраненные строки.
                    ref_key = fields.get('Ref_Key')
                    if ref_key is not None and table_part_name in self.metadata.keys():
                        self._add_records(table_part_name,
                                          [self._make_empty_table_part_record(table_part_name, ref_key)])

                # Связываем объект-ТЧ с владельцем — чтобы to_nested_records нашёл его сам.
                if table_part_name in self:
                    self[object_name].table_parts[table_part_key] = self[table_part_name]
