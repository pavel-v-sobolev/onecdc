import requests
import threading
from typing import Any
from collections import UserDict

import xmltodict
from sqlalchemy import (String, Uuid, BigInteger, Integer, SmallInteger, Numeric, Boolean, DateTime,
                        Float, JSON, Engine, func, select, update)
from sqlalchemy.dialects.postgresql import JSONB
from dbmerge import dbmerge

from onecdc.name_mapper import SCOPE_OBJECT, NameMapper, field_scope
from onecdc.common_functions import format_bytes, parse_object_full_name, raise_for_status
from onecdc.logging_config import get_logger, load_mode, LOAD_MODE_METADATA

logger = get_logger(__name__)

# Таймауты HTTP-запросов к 1С по умолчанию: (connect, read) в секундах. requests с timeout=None
# висит бесконечно при недоступном сервере.
# connect ограничивает ожидание установки соединения, read — ожидание ответа.
# Read — 15 минут: пакет изменений или страница полной выгрузки бывает реально большой,
# и 1С формирует его долго; таймаут должен ловить зависший сервер, а не медленный ответ.
# С периодом опроса это не связано: цикл опроса может быть заметно короче обработки пакета.
# Применяется, когда request_timeout не задан явно (None).
DEFAULT_REQUEST_TIMEOUT: tuple[float, float] = (60, 900)


def resolve_timeout(request_timeout: float | tuple[float, float] | None):
    """request_timeout как есть, либо default, если он не задан (None).
    Гарантирует, что ни один HTTP-запрос не уходит в requests с timeout=None (вечное ожидание)."""
    return DEFAULT_REQUEST_TIMEOUT if request_timeout is None else request_timeout


# Таблица-реестр объектов 1С и состояния их полной выгрузки (см. MetadataReader).
METADATA_OBJECTS_TABLE = 'onecdc_metadata_objects'

type_mapping = {'Guid':Uuid(),
                'Int64':BigInteger(),
                # Int32 приходит там, где у объекта числовой код/номер (Code, Number) и у номеров
                # сообщений плана обмена (ReceivedNo/SentNo) — см. Приложение 12 руководства
                # разработчика. Без него поле отбрасывалось бы как «неизвестный тип».
                'Int32':Integer(),
                'Int16':SmallInteger(),
                'String':String(),
                'Double':Numeric(),
                'Boolean':Boolean(),
                'DateTime':DateTime(),
                # ХранилищеЗначения публикуется парой полей: <Имя> (Edm.Stream, см. IGNORED_TYPES)
                # и <Имя>_Base64Data (Edm.Binary). Binary реально приходит в теле ответа
                # base64-строкой (в ней бывает JSON или XML-сериализация 1С), поэтому храним как текст.
                'Binary':String(),
                # Субконто регистра бухгалтерии: тип синтетический, поля с ним в $metadata нет —
                # см. EXT_DIMENSIONS_FIELDS. JSON, а не JSONB: диалект знает только писатель
                # (DBWriter.save поднимает его до JSONB на postgres).
                'ExtDimensions':JSON()}

# Типы, которых нет в данных: их нельзя отобразить в колонку, но это не ошибка метаданных.
# Collection — табличная часть, читается отдельным объектом.
# Stream — media-link (ХранилищеЗначения): в m:properties не приходит никогда,
# значение доступно только отдельным GET по ссылке.
IGNORED_TYPES = ('Collection', 'Stream')

GUESS_UUID_TYPES = True

# Постфикс соседней колонки составного типа: в ней лежит ССЫЛКА, приведённая к uuid, — тем и
# полезна, что по ней джойнятся ключи других таблиц. Само поле остаётся текстовым и хранит
# значение как есть (см. DataReader._get_record_fields).
COMPOSITE_GUID_SUFFIX = '_Guid'
# Проблема в том, что 1С часть GUID полей присылает как строки в описании метаданных.
# В этом модуле есть логика, которая определяет тип UUID поля, по имени поля "Recorder" 
# или по наличию другого поля с постфиксом "_Type" для составных типов данных.
# На всякий случай сделан этот флаг, чтобы можно было эту логику отключить.
# Конечно, если отключить флаг, то это часть полей будут UUID, а часть VARCHAR.
# В этом случает VARCHAR поля лучше руками в базе поменять на UUID, 
# т.к. иначе будут медленно работать JOIN

# Регистр бухгалтерии в OData устроен ровно как регистраторный регистр накопления: EntityType с
# ключом Recorder(+Recorder_Type) и коллекцией RecordSet, рядом <Имя>_RecordType с описанием полей
# движения. Поэтому он разбирается тем же кодом; отличается только классификация полей
# (см. _classify_register_fields) и то, что субконто в набор записей вообще не приходят.
# Регистр расчёта сюда не входит: у него своя структура записи (периоды действия, вытеснение).
REGISTER_TYPES = ('InformationRegister','AccumulationRegister','AccountingRegister')
# Ссылочные классы: устроены одинаково (Ref + DeletionMark + реквизиты + табличные части),
# поэтому разбираются общим кодом. Регистры сюда не входят — см. REGISTER_TYPES
# (Приложение 12 руководства разработчика, разделы 12.11 и 12.12).
ENTITY_TYPES = ('Catalog','Document')
# Планы видов характеристик, планы счетов, планы видов расчёта, бизнес-процессы и задачи здесь
# СОЗНАТЕЛЬНО отсутствуют. Устроены они так же, и общий код их разобрал бы, но ни одного живого
# ответа 1С этих классов мы не видели: в тестах их метаданные собраны руками из трёх полей, в
# записанных ответах их нет, демо-база в OData их не публикует. Обещать поддержку, которую нечем
# подтвердить, дороже, чем её не обещать: объект такого класса в плане обмена будет громко
# пропущен (CHANGES LOST), а не тихо сохранён неизвестно как.
#
# Класс возвращается сюда вместе с записанными ответами 1С и тестом на них, не раньше.

# План видов характеристик: метаданные читаем, объект не сохраняем. Читаем потому, что по нему
# регистр бухгалтерии определяет виды субконто (DataReader._find_ext_dimension_chart перебирает
# планы в метаданных) — без этого ключом субконто остался бы голый Guid.
METADATA_ONLY_TYPES = ('ChartOfCharacteristicTypes',)
# Классы, которые мы умеем сохранять. Всё остальное, придя в пакете изменений, будет потеряно
# (пакет подтверждается целиком), поэтому такие объекты логируются отдельно — см. read_data_entries.
SUPPORTED_TYPES = REGISTER_TYPES + ENTITY_TYPES
# Классы, для которых строятся метаданные. Шире SUPPORTED_TYPES ровно на METADATA_ONLY_TYPES.
KNOWN_ENTITY_TYPES = ENTITY_TYPES + METADATA_ONLY_TYPES
METADATA_POSTFIXES = ('_RecordType','_RowType','_Balance','_Turnover','_BalanceAndTurnover')
ODATA_PREFIX = 'StandardODATA.'
TYPE_PREFIX = 'Edm.'

# Поля регистратора в OData: Recorder (+Recorder_Type), если регистратором может быть несколько
# типов документов, и Recorder_Key (Guid, без Recorder_Type), если тип регистратора единственный.
RECORDER_FIELDS = ('Recorder', 'Recorder_Key', 'Recorder_Type')

# --- Субконто регистра бухгалтерии ---
# Колонки, которых в $metadata нет: в описании движения (_RowType) субконто отсутствуют вовсе,
# они живут только в виртуальной таблице RecordsWithExtDimensions и собираются оттуда
# (см. DataReader). Ключ JSON-объекта — ВИД субконто (Guid), значение — {"value", "type"}.
#
# Почему JSON, а не колонки: 1С отдаёт субконто слотами (ExtDimensionDr1..3), а номер слота смысла
# не имеет — субконто1 счёта 10 это Номенклатура, счёта 60 Контрагенты. Витрина, написавшая
# `ExtDimensionDr1 = …`, была бы права ровно до следующего счёта, причём молча.
ACCOUNTING_REGISTER_TYPE = 'AccountingRegister'
EXT_DIMENSIONS_TYPE = 'ExtDimensions'
# Сторона проводки -> имя колонки.
EXT_DIMENSIONS_FIELDS = {'Dr': 'ExtDimensionsDr', 'Cr': 'ExtDimensionsCr'}
# Системные поля движений регистра (не измерения/ресурсы/реквизиты).
SYSTEM_REGISTER_FIELDS = frozenset(
    ('Period', 'LineNumber', 'Active', 'RecordType') + RECORDER_FIELDS)
# Поля period-спайна в виртуальной таблице _Turnover (агрегаты по периодам, не измерения).
TURNOVER_PERIOD_FIELDS = frozenset(
    ('Period', 'SecondPeriod', 'MinutePeriod', 'HourPeriod', 'DayPeriod', 'WeekPeriod',
     'TenDaysPeriod', 'MonthPeriod', 'QuarterPeriod', 'HalfYearPeriod', 'YearPeriod'))
TURNOVER_RESOURCE_SUFFIXES = ('Turnover', 'Receipt', 'Expense')
# Метка ресурса в виртуальной таблице _DrCrTurnover регистра бухгалтерии. Там она стоит НЕ в конце
# имени, а перед Dr/Cr: СуммаTurnover -> Сумма, ВалютнаяСуммаTurnoverDr -> ВалютнаяСуммаDr.
DR_CR_TURNOVER_MARKER = 'Turnover'

def _check_object_is_table_part(base_name:str, complextypes: dict[str, list[str]]):
    """
    Если найден блок метаданных с таким же именем и с постфиксом _RowType, значит это табличная часть
    """
    row_type = complextypes.get(base_name + '_RowType')
    return row_type is not None

def _classify_register_fields(base_name: str, properties: dict, complextypes: dict[str, list[str]]):
    """
    Делит поля движений регистра на измерения / ресурсы / реквизиты, сравнивая с виртуальными
    таблицами _Balance / _Turnover (ComplexType из $metadata). Виртуальные таблицы 1С считает
    функциями на лету — здесь они нужны ТОЛЬКО как описание типов для классификации.

    Возвращает (dimensions, resources, attributes). Если функции для регистра не опубликованы
    (нет ни _Balance, ни _Turnover) — ([], [], []). Регистр может иметь и остатки, и обороты,
    поэтому обе таблицы обрабатываются независимо.
    """
    prop_names = set(properties)
    balance = complextypes.get(base_name + '_Balance')
    turnover = complextypes.get(base_name + '_Turnover')
    # Регистр бухгалтерии: _Balance/_Turnover у него сворачивают проводку в одну сторону
    # (Account_Key, ExtDimension1, ВалютнаяСуммаBalance), а в движении лежат ПАРЫ Дт/Кт
    # (AccountDr_Key/AccountCr_Key, ВалютнаяСуммаDr/Cr) — по этим таблицам не опознаётся почти
    # ничего. Имена из _DrCrTurnover ложатся на движение один в один, поэтому для регистра
    # бухгалтерии классифицируем по ней.
    dr_cr_turnover = complextypes.get(base_name + '_DrCrTurnover')

    if balance is None and turnover is None and dr_cr_turnover is None:
        return [], [], []

    dimensions: list[str] = []
    resources: list[str] = []

    if dr_cr_turnover is not None:
        for f in dr_cr_turnover:
            if DR_CR_TURNOVER_MARKER in f:
                resources.append(f.replace(DR_CR_TURNOVER_MARKER, '', 1))
            elif f in TURNOVER_PERIOD_FIELDS or f in SYSTEM_REGISTER_FIELDS or f.endswith('_Type'):
                continue
            else:
                dimensions.append(f)

    if balance is not None:
        for f in balance:
            if f.endswith('Balance'):
                resources.append(f[:-len('Balance')])
            elif not f.endswith('_Type'):
                dimensions.append(f)
    if turnover is not None:
        for f in turnover:
            suffix = next((s for s in TURNOVER_RESOURCE_SUFFIXES if f.endswith(s)), None)
            if suffix is not None:
                resources.append(f[:-len(suffix)])
            elif f in TURNOVER_PERIOD_FIELDS or f in SYSTEM_REGISTER_FIELDS or f.endswith('_Type'):
                continue
            else:
                dimensions.append(f)

    # Оставляем только реально присутствующие в движениях поля, без дублей (порядок сохраняем).
    dimensions = [d for d in dict.fromkeys(dimensions) if d in prop_names]
    resources = [r for r in dict.fromkeys(resources) if r in prop_names]
    used = set(dimensions) | set(resources) | SYSTEM_REGISTER_FIELDS
    attributes = [f for f in properties if f not in used and not f.endswith('_Type')]
    return dimensions, resources, attributes




class MetadataObject(UserDict):
    def __init__(self, name, properties, primary_key, object_key=None,
                 dimensions=None, resources=None, attributes=None, is_table_part = False):
        super().__init__(properties)
        self.name = name
        self.primary_key = primary_key
        # Ключ для scoped-удаления при merge (см. _get_object_key):
        # регистр -> Recorder(+Recorder_Type), табличная часть -> Ref_Key,
        # документ/справочник -> None (одна запись, delete не нужен).
        self.object_key = object_key
        # Классификация полей регистра (см. _classify_register_fields); для не-регистров пусто.
        # Имена — оригинальные (1С), маппятся NameMapper-ом при использовании, как object_key.
        self.dimensions = dimensions or []   # измерения
        self.resources = resources or []     # ресурсы
        self.attributes = attributes or []   # реквизиты

        self.is_table_part = is_table_part


    def get_column_types(self) -> dict[str, Any]:
        return {col: type_mapping[typ] for col, typ in self.data.items()}



class MetadataReader(UserDict):
    def __init__(self, odata_url:str, odata_auth: tuple[str, str] | None = None,
                 request_timeout: float | None = None,
                 engine: Engine | None = None, schema: str | None = None,
                 temp_schema: str | None = None, name_mapper: "NameMapper | None" = None):
        super().__init__()
        self.odata_url=odata_url
        self.odata_auth=odata_auth
        self.request_timeout=request_timeout
        # В конструкторе метаданные НЕ загружаются (без сетевого запроса), чтобы недоступность 1С
        # на старте не роняла процесс. Загрузка — get_metadata(), которая выставляет is_loaded=True.
        self.is_loaded = False
        # get_metadata может вызываться лениво из фоновых потоков full_load (новый объект/поле)
        # параллельно с основным циклом — сериализуем перестроение словаря.
        self._lock = threading.Lock()
        # Об исчезнувших объектах предупреждаем один раз на объект, а не на каждое перечитывание
        # метаданных (см. _warn_about_absent).
        self._absent_reported: set[str] = set()

        # Маппер имён — ОДИН на процесс, а не по месту вызова: он ведёт реестр заявок
        # onecdc_name_claims (см. name_mapper), и отдельные экземпляры зря перечитывали бы его
        # и грели каждый свой кэш. Репликатор передаёт сюда свой; без engine получается
        # offline-маппер, который считает транслит и в реестр не ходит.
        self.name_mapper = name_mapper if name_mapper is not None else NameMapper(engine, schema)

        # Реестр объектов и состояния полной выгрузки (onecdc_metadata_objects). Ведётся, только если
        # передан engine (в библиотечном/тестовом сценарии без БД метаданные читаются как раньше).
        # Членство в плане обмена определяется эмпирически — по приходу объекта в пакете SelectChanges
        # (require_full_load_if_new). Состав реестра синхронизируется с $metadata через dbmerge
        # (delete: пропавшие объекты удаляются, merged_on ведёт dbmerge).
        # Таблицу создаёт сам dbmerge при первой sync; objects_table — её Table-описание оттуда же.
        self.engine = engine
        self.schema = schema
        # Схема промежуточных таблиц dbmerge (см. DBWriter); None — схема данных.
        self.temp_schema = temp_schema
        self.objects_table = None


    def _read_metadata_item_properties(self, item:dict, item_name: str | None = None):
        """
        Читаем поля объекта метаданных
        """
        item_name = item_name or item.get('@Name')
        item_properties = item.get('Property') or []

        properties = {}
        for item_property in item_properties:
            property_name = item_property['@Name']
            declared_type = item_property['@Type']

            property_type = declared_type.removeprefix(TYPE_PREFIX)

            if GUESS_UUID_TYPES:
                if property_name=='Recorder':
                    # Принудительно ставим Uuid для регистраторов, т.к. 1С почему-то присылает String
                    property_type='Guid'

            if property_type in type_mapping:
                properties[property_name] = property_type
            elif property_type.startswith(IGNORED_TYPES):
                # ожидаемо не отображается в колонку (табличная часть / media-link), не ошибка
                logger.debug(f'Property {item_name}.{property_name} of type {property_type} '
                             f'is not stored as a column')
            else:
                # Неизвестный тип берём СТРОКОЙ, а не выбрасываем поле. Выбрасывание стоило дорого
                # и не там, где кажется: поле пропадало и из ключа объекта (_read_metadata_item_key
                # берёт из ключа только то, что уцелело здесь). Пустой ключ означал «сохранить
                # невозможно», но выглядел как «сохранять нечего» — пакет подтверждался, данные
                # исчезали с одной строкой в логе. Усечённый ключ был не лучше: две разные записи
                # 1С получали один ключ, и в следующем пакете вторая затирала первую.
                #
                # Строкой безопасно: в m:properties 1С всё равно присылает текст, приводить его
                # не к чему, и упасть на вставке нечем. Данные при этом сохраняются, а не теряются.
                # Структура (StandardODATA.*) заведёт пустую колонку — маршрутизация в парсере идёт
                # по значению, а не по метаданным, и словарь в запись не попадёт. Это дешевле, чем
                # отдельная ветка, а ключ остаётся целым в любом случае.
                # В сообщении тип КАК ОБЪЯВЛЕН, с префиксом: по 'Time' не понять, что искать,
                # а по 'Edm.Time' видно и пространство имён, и что это примитив платформы.
                logger.warning(f'Property {item_name}.{property_name} has unknown type '
                               f'{declared_type}, storing it as String. Add the type to '
                               f'type_mapping if a proper column type is needed')
                properties[property_name] = 'String'
            
        if GUESS_UUID_TYPES:
            # list(): ниже в properties добавляется соседняя колонка <поле>_Value.
            for property_name in list(properties.keys()):
                # Если мы видим что есть поле с постфиксом Type, то значит
                # ищем такое же поле без постфикса, т.к. в этом случае это составной тип и нужно
                # изменить поле на Uuid т.к. 1С почему-то присылает String
                if property_name.endswith('_Type'):
                    uuid_property_name = property_name.removesuffix('_Type')
                    if uuid_property_name in properties.keys():
                        if uuid_property_name in RECORDER_FIELDS:
                            # Регистратор — тоже составное поле, но примитивом он не бывает
                            # НИКОГДА: это всегда ссылка на документ. Значит и текстовым его
                            # делать незачем, и соседняя колонка была бы вечным дублем. Оставляем
                            # как было — ссылкой.
                            #
                            # Исключение не косметическое: Recorder входит в первичный ключ
                            # КАЖДОГО регистра по регистратору, и смена его типа отправила бы в
                            # архив все такие таблицы разом, с полной перевыгрузкой каждой
                            # (см. DBWriter._retype_changed_columns).
                            properties[uuid_property_name] = 'Guid'
                            continue
                        # Составной тип бывает не только ссылочным: у «Дополнительных реквизитов»
                        # в Значение лежат и ссылки, и числа/строки/даты (тип видно в Значение_Type).
                        #
                        # Поэтому САМО поле — строка, и в нём всегда лежит значение как пришло.
                        # Иначе примитив в нём не помещался вовсе: раньше поле объявлялось uuid,
                        # значение уходило в соседнюю колонку, а здесь оставался NULL — и если
                        # поле входило в первичный ключ, две записи с разными значениями получали
                        # один и тот же ключ (нулевой guid) и склеивались.
                        #
                        # Ссылка дополнительно кладётся в соседнюю <поле>_Guid: по ней джойнятся
                        # ключи других таблиц, а это основной сценарий. Колонка заводится всегда,
                        # чтобы состав таблицы не зависел от того, попался ли примитив в первой
                        # прочитанной пачке.
                        properties[uuid_property_name] = 'String'
                        properties[uuid_property_name + COMPOSITE_GUID_SUFFIX] = 'Guid'


        return properties

    def _read_metadata_item_key(self, item:dict, properties: dict, item_name: str | None = None):
        """
        Читаем список ключевых полей объекта метаданных.

        Ключ обязан разобраться ЦЕЛИКОМ: без полного ключа нет идентичности строки, а значит
        merge либо не сможет писать вовсе, либо склеит разные записи 1С в одну. Раньше недостающие
        поля просто отбрасывались вместе с ключом, и обе беды случались молча.

        После того как неизвестный тип стал строкой (см. _read_metadata_item_properties), поле
        ключа может не найтись только у метаданных, которым нельзя доверять: Key ссылается на
        необъявленное поле либо на тип, у которого колонки не бывает (Collection/Stream). Поэтому
        здесь ERROR, а не warning: это не свойство данных, а поломка на стороне источника.
        """
        item_key = (item.get('Key') or {}).get('PropertyRef')
        # Ключа может не быть (например, у сущности без объявленного Key) — тогда пустой список,
        # иначе обращение к key_fields ниже упало бы с UnboundLocalError.
        key_fields = ([k.get('@Name') for k in item_key if k.get('@Name') is not None]
                      if item_key else [])

        missing = [k for k in key_fields if k not in properties]
        if missing:
            logger.error(f'Key of {item_name or item.get("@Name")} refers to fields that are not '
                         f'stored as columns: {missing}. Rows of this object cannot be identified, '
                         f'so they will be skipped or merged into one another')

        return {k: properties[k] for k in key_fields if k in properties}


    def _get_object_key(self, item_name: str, properties: dict, primary_key: dict):
        """
        Ключ для scoped-удаления при merge (delete_condition в dbmerge).
        Изменения приходят группами, которые целиком заменяют существующие строки:
        - регистр: набор записей одного регистратора -> Recorder (+ Recorder_Type) либо
          Recorder_Key, смотря как 1С назвала поле регистратора (см. RECORDER_FIELDS);
        - табличная часть (ключ Ref_Key + ещё поля) -> Ref_Key владельца;
        - документ/справочник (единственная запись по Ref_Key) -> None, удаление не нужно;
        - независимый регистр сведений -> None: регистратора у него нет, запись приходит поодиночке
          и адресуется полным первичным ключом, то есть группы, которую надо чистить, не существует.
        """
        if item_name.startswith(REGISTER_TYPES):
            return [c for c in RECORDER_FIELDS if c in properties] or None
        if 'Ref_Key' in primary_key and len(primary_key) > 1:
            return ['Ref_Key']
        return None

    def get_metadata(self):
        """
        Запрашиваем метаданные всех доступных объектов из odata и (если задан engine) синхронизируем
        реестр onecdc_metadata_objects с актуальным составом $metadata.
        Можно вызывать повторно для обновления (при появлении нового объекта/поля — см. data_reader);
        под блокировкой, т.к. вызывается и из фоновых потоков full_load. В конце is_loaded=True.

        В логе помечается своим режимом (METADATA), а не режимом вызвавшей операции: чтение
        $metadata общее для пакета изменений и полной выгрузки, и метка CHANGES на нём вводила бы
        в заблуждение.
        """
        with self._lock, load_mode(LOAD_MODE_METADATA):
            objects = self._fetch_and_parse_metadata()
            # ПОДМЕНА ЦЕЛИКОМ, а не пополнение на месте. Словарь читают чужие потоки — фоновая
            # выгрузка перебирает его на КАЖДОЙ странице (Replicator._full_load_tables), — и
            # блокировку они не берут. Пополнение под ними давало
            # «RuntimeError: dictionary changed size during iteration», то есть обрыв полной
            # выгрузки посреди прогона: на крупном объекте это часы работы впустую. Подмена ссылки
            # неделима, и читатель видит либо старый словарь целиком, либо новый.
            #
            # Заодно из памяти уходит объект, пропавший из $metadata: раньше словарь только
            # пополнялся, и пропавший жил до перезапуска. Это было безопасно ровно потому, что
            # реестр синхронизировался удалением строк, — теперь он их помечает (см. _sync_objects),
            # и состояние выгрузки исчезновение объекта переживает.
            self.data = objects
            if self.engine is not None:
                self._sync_objects(list(self.keys()))
            # ПОСЛЕ синхронизации: упади она — метаданные не считаются загруженными, и следующий
            # цикл попробует снова, а не пойдёт работать с objects_table = None.
            self.is_loaded = True

    def _fetch_and_parse_metadata(self) -> dict:
        """Читает $metadata и собирает НОВЫЙ словарь объектов, не трогая текущий.

        Отдельный словарь, а не запись в self: пока сборка идёт, читатели работают со старым, а
        неудача на середине (сеть, неполный ответ) не оставляет словарь наполовину разобранным.
        Публикует его вызывающий одной подменой ссылки (см. get_metadata)."""
        logger.info('Requesting metadata from 1C ODATA')
        
        url = f'{self.odata_url}/$metadata'
        response = requests.get(url,auth=self.odata_auth,
                                timeout=resolve_timeout(self.request_timeout))
        raise_for_status(response, '$metadata')
        logger.info('Metadata received (%s)', format_bytes(len(response.content)))

        metadata = xmltodict.parse(response.text,force_list=('Property','PropertyRef','ComplexType','EntityType'))
        metadata_schema = ((metadata.get('edmx:Edmx') or {}).get('edmx:DataServices') or {}).get('Schema') or {}
        metadata_entity_types = metadata_schema.get('EntityType') or []
        # Виртуальные таблицы регистров (_Balance/_Turnover) приходят как ComplexType — нужны для
        # классификации полей регистра на измерения/ресурсы/реквизиты (см. _classify_register_fields).
        # Табличные части документов и справочников также приходят как ComplexType.
        complextypes = {ct.get('@Name'): [p.get('@Name') for p in (ct.get('Property') or [])]
                        for ct in (metadata_schema.get('ComplexType') or [])}


        # Имена всех EntityType — по ним отличается независимый регистр от регистраторного:
        # у регистраторного рядом лежит <Имя>_RecordType, у независимого его нет (см. ниже).
        entity_type_names = {i.get('@Name') for i in metadata_entity_types}

        objects: dict[str, MetadataObject] = {}
        for item in metadata_entity_types:

            item_name = item.get('@Name')

            if item_name.startswith(REGISTER_TYPES) and item_name.endswith("_RecordType"):
            # регистр с постфиксом RecordType содержит описание полей регистра и описание ключа
                item_name = item_name.removesuffix("_RecordType")
                properties = self._read_metadata_item_properties(item, item_name)
                primary_key = self._read_metadata_item_key(item, properties, item_name)
                object_key = self._get_object_key(item_name, properties, primary_key)
                dimensions, resources, attributes = _classify_register_fields(
                    item_name, properties, complextypes)
                if item_name.startswith(ACCOUNTING_REGISTER_TYPE):
                    # Колонки субконто заводим ПОСЛЕ классификации: в 1С это измерения, но
                    # программно ими не пользуются (внутри JSON), и в dimensions им делать нечего.
                    properties.update({f: EXT_DIMENSIONS_TYPE
                                       for f in EXT_DIMENSIONS_FIELDS.values()})
                objects[item_name] = MetadataObject(item_name, properties, primary_key, object_key,
                                                 dimensions, resources, attributes)

            elif (item_name.startswith(REGISTER_TYPES)
                  and not item_name.endswith(METADATA_POSTFIXES)
                  and item_name + '_RecordType' not in entity_type_names):
            # Независимый регистр сведений: _RecordType 1С для него не публикует, поля и ключ
            # (измерения + Period, если регистр периодический) лежат прямо в самом EntityType.
            # Проверка именно на ОТСУТСТВИЕ соседнего _RecordType, а не на состав полей: у
            # регистраторного регистра EntityType без постфикса тоже есть, но описывает он не
            # запись, а набор записей регистратора (Recorder + коллекция RecordSet), и брать
            # ключ оттуда нельзя.
                properties = self._read_metadata_item_properties(item, item_name)
                primary_key = self._read_metadata_item_key(item, properties, item_name)
                object_key = self._get_object_key(item_name, properties, primary_key)
                dimensions, resources, attributes = _classify_register_fields(
                    item_name, properties, complextypes)
                objects[item_name] = MetadataObject(item_name, properties, primary_key, object_key,
                                                 dimensions, resources, attributes)

            elif (item_name.startswith(KNOWN_ENTITY_TYPES)
                  and not item_name.endswith(METADATA_POSTFIXES)):
            # если документ или справочник без постфикса, то
            # читаем его описание полей и ключ
            # (также может быть табличная часть документа или справочника)
                properties = self._read_metadata_item_properties(item, item_name)
                primary_key = self._read_metadata_item_key(item, properties, item_name)
                object_key = self._get_object_key(item_name, properties, primary_key)
                is_table_part = _check_object_is_table_part(item_name, complextypes)
                objects[item_name] = MetadataObject(item_name, properties, primary_key, object_key, 
                                                 is_table_part=is_table_part)

        return objects

    # --- Реестр объектов и состояния полной выгрузки (onecdc_metadata_objects) ---

    def owner_of(self, object_full_name: str) -> str | None:
        """
        Владелец табличной части, либо None — объект самостоятельный.

        Табличные части 1С публикует отдельными объектами с именем «владелец_ЧастьИмени», поэтому
        владелец ищется как САМОЕ ДЛИННОЕ известное имя, являющееся префиксом: у документа с
        подчёркиванием в собственном имени короткий префикс мог бы совпасть с чужим объектом.

        Нужно там, где часть обязана разделить судьбу владельца: под полной выгрузкой владельца
        его части пишутся той же страницей (см. Replicator._full_load_tables — обратное
        отображение), и режим записи у них должен быть тот же.
        """
        obj = self.get(object_full_name)
        if obj is None or not obj.is_table_part:
            return None
        candidates = [name for name in self
                      if name != object_full_name and object_full_name.startswith(name + '_')]
        return max(candidates, key=len) if candidates else None

    def resolve_object_name(self, name: str) -> str:
        """
        Имя объекта 1С по тому, что передал вызывающий: принимается и имя 1С как есть
        (`Document_ЗаказКлиента`), и имя ТАБЛИЦЫ в БД (`Document_ZakazKlienta`).

        Обе формы — потому что настраивают выгрузку, глядя в базу и в реестр
        onecdc_metadata_objects (там имя таблицы лежит в object_full_name_en), а не в конфигуратор,
        и то же имя стоит у обработчиков в `ON`. Отдельная таблица соответствий не нужна:
        транслитерация детерминирована, поэтому обратное соответствие ищется перебором.

        Не нашли — ValueError с обеими ожидаемыми формами: прежде такое имя доезжало до середины
        прогона и падало «no primary key», по которому догадаться было невозможно.
        """
        if name in self:
            return name
        mapper = self.name_mapper
        matches = [candidate for candidate in self if mapper.map_object_name(candidate) == name]
        if len(matches) == 1:
            return matches[0]
        if matches:
            # Транслит длинного имени усекается с хэшем, так что совпасть у двух объектов он может
            # только теоретически. Но выбирать тут наугад нельзя: выгрузка ушла бы не в тот объект.
            raise ValueError(f"Object {name!r} is ambiguous: {', '.join(sorted(matches))}")
        raise ValueError(f"Object {name!r} not found in 1C metadata; expected a 1C name like "
                         f"'Document_ЗаказКлиента' or a table name like 'Document_ZakazKlienta' "
                         f"(see onecdc_metadata_objects.object_full_name / object_full_name_en)")

    def resolve_field_name(self, object_name: str, field: str) -> str:
        """
        Имя поля 1С по имени поля или КОЛОНКИ в БД — то же правило, что у resolve_object_name.
        object_name должен быть уже разрешён (имя 1С).
        """
        fields = self.get(object_name) or {}
        if field in fields:
            return field
        mapper = self.name_mapper
        matches = [candidate for candidate in fields
                   if mapper.map_field_name(candidate, object_name) == field]
        if len(matches) == 1:
            return matches[0]
        if matches:
            raise ValueError(f"Field {field!r} of {object_name} is ambiguous: "
                             f"{', '.join(sorted(matches))}")
        raise ValueError(f"Field {field!r} not found in {object_name}; expected a 1C name like "
                         f"'Дата' or a column name like 'Data' "
                         f"(see onecdc_metadata_objects.fields / fields_en)")

    def _sync_objects(self, object_names: list[str]) -> None:
        """
        Синхронизирует реестр с актуальным составом $metadata через dbmerge (delete): новые объекты
        вставляются, пропавшие из метаданных — удаляются из таблицы реестра.
        """
        if not object_names:
            return

        # object_type (префикс имени) — неключевая колонка. Колонки full_load передаём только
        # для создания таблицы/первой вставки и исключаем из UPDATE (skip_update_fields).
        # object_full_name_en — транслитерированное имя (= имя таблицы в БД); fields/fields_en — JSON-списки
        # полей объекта: оригинальные имена 1С и их транслит (= имена колонок в БД). Для удобного
        # просмотра состава объекта. Все три синхронизируются с $metadata.
        mapper = self.name_mapper
        # Закрепляем имена всей конфигурации одной пачкой — иначе первый запуск на крупной базе
        # 1С тратит по транзакции на каждое из десятков тысяч имён (см. NameMapper.prefetch).
        # Заявляются ВСЕ объекты $metadata, а не только те, что в плане обмена: так распределение
        # имён не зависит от того, какой объект случился первым.
        mapper.prefetch(SCOPE_OBJECT, object_names)
        for object_full_name in object_names:
            # Область уникальности колонок — объект, поэтому и пачка на объект (см. field_scope).
            mapper.prefetch(field_scope(object_full_name), self.get(object_full_name) or {})
        # На json-колонке dbmerge сравнивает значения через IS DISTINCT FROM; у Postgres-типа json
        # нет оператора равенства — берём jsonb.
        json_type = JSONB() if self.engine.dialect.name == 'postgresql' else JSON()
        data = []
        for object_full_name in object_names:
            obj = self.get(object_full_name)
            field_names = list(obj.keys()) if obj is not None else []
            object_name, object_type = parse_object_full_name(object_full_name)
            data.append({
                'object_full_name': object_full_name, 
                'object_full_name_en': mapper.map_object_name(object_full_name),
                'object_name': object_name,
                'object_type': object_type,
                'fields': field_names,
                'fields_en': [mapper.map_field_name(f, object_full_name) for f in field_names],
                # эти значения устанавливаются только при insert, из update они исключены
                'full_load_is_required': False, 'last_full_load_dt': None,
                'last_full_load_rows_modified': None, 'last_full_load_minutes': None,
                # Захват объекта под выгрузку (см. full_load_claim): ставится и снимается только
                # тем, кто выгружает, поэтому здесь — лишь значения для вставки новой строки.
                'full_load_owner': None, 'full_load_heartbeat_at': None})
            
        with dbmerge(engine=self.engine, table_name=METADATA_OBJECTS_TABLE, data=data,
                     key=['object_full_name'],
                     # ПОМЕЧАЕМ, а не удаляем. В строке реестра лежит не только описание объекта,
                     # но и его состояние: захват под полную выгрузку (владелец + отметка живости),
                     # отметка «выгружался целиком», заказ выгрузки, метрики. Удаление уносило всё
                     # это разом — а объект пропадает из $metadata не только навсегда: состав
                     # OData переустанавливают внешней обработкой, и на секунды публикация неполна.
                     #
                     # Цена была велика и молчалива. Захват держит ОДИН процесс, а удаляет строку
                     # ДРУГОЙ — и первый об этом не узнает: его heartbeat обновит ноль строк, для
                     # него это неотличимо от успеха. Строка вернётся с пустым владельцем, и объект
                     # спокойно захватит кто угодно, начав выгружать его параллельно первому. То
                     # есть исключительность снималась ИЗВНЕ, без участия обоих процессов. А
                     # mark_full_loaded в конце прогона тоже обновлял ноль строк, и объект
                     # выглядел ни разу не выгруженным — то есть уходил в выгрузку по новой.
                     delete_mode='mark', delete_mark_field='absent_from_metadata',
                     merged_on_field='merged_on', schema=self.schema,
                     temp_schema=self.temp_schema,
                     data_types={'object_full_name': String(),
                                 'object_name': String(),
                                 'object_type': String(),
                                 'object_full_name_en': String(), 
                                 'fields': json_type,
                                 'fields_en': json_type,
                                 'full_load_is_required': Boolean(),
                                 'last_full_load_dt': DateTime(),
                                 'last_full_load_rows_modified': Integer(),
                                 'last_full_load_minutes': Float(),
                                 'full_load_owner': String(),
                                 'full_load_heartbeat_at': DateTime(),
                                 'absent_from_metadata': Boolean()
                                 },
                     skip_update_fields=['full_load_is_required', 'last_full_load_dt',
                                         'last_full_load_rows_modified',
                                         'last_full_load_minutes',
                                         'full_load_owner',
                                         'full_load_heartbeat_at']) as merge:
            result = merge.exec()
            self.objects_table = merge.table   # Table-описание созданной/существующей таблицы
        if result is not None and result.deleted_row_count:
            self._warn_about_absent(result.deleted_row_count)

    def _warn_about_absent(self, marked: int) -> None:
        """
        Сообщает об объектах, пропавших из `$metadata`. Один раз на объект: имена берём из самой
        таблицы, потому что dbmerge отдаёт только количество.

        Молчать нельзя. Исчезновение объекта из публикации — это либо ошибка настройки состава
        OData, либо осознанное решение; в обоих случаях оно означает, что изменения по объекту
        больше не поедут, а его таблица останется в схеме как есть.
        """
        table = self.objects_table
        with self.engine.begin() as conn:
            names = list(conn.execute(select(table.c.object_full_name)
                                      .where(table.c.absent_from_metadata.is_(True))).scalars())
        fresh = [name for name in names if name not in self._absent_reported]
        self._absent_reported.update(names)
        if fresh:
            logger.warning(
                "Objects are gone from $metadata and marked as absent in the registry (%s of %s "
                "just now): %s. Their changes will no longer arrive; their tables and their state "
                "(full load claim, history) are left untouched",
                marked, len(names), ', '.join(sorted(fresh)))

    def require_full_load_if_new(self, object_full_name: str) -> None:
        """
        Помечает объект как требующий полной выгрузки, если он ещё ни разу не выгружался целиком
        (last_full_load_dt IS NULL). Вызывается на каждый объект пакета SelectChanges — это и есть
        признак членства в плане обмена. Ключ реестра — полное имя (object_full_name): регистр и
        документ могут иметь одинаковое короткое имя. Если строки ещё нет (объект пришёл в пакете
        раньше, чем его увидела sync) — перечитываем метаданные (sync заведёт строку) и помечаем.
        """
        def _last_full_load_dt():
            table = self.objects_table
            with self.engine.begin() as conn:
                return conn.execute(select(table.c.last_full_load_dt)
                                    .where(table.c.object_full_name == object_full_name)).first()

        row = _last_full_load_dt()
        if row is None:
            # Объект ещё не в реестре — перечитываем метаданные (get_metadata → _sync_objects
            # вставит строку). Вне транзакции: get_metadata сам открывает соединения через dbmerge.
            logger.info('Object %s not in registry yet, reloading metadata', object_full_name)
            self.get_metadata()
            row = _last_full_load_dt()

        if row is not None and row.last_full_load_dt is None:
            table = self.objects_table
            with self.engine.begin() as conn:
                conn.execute(update(table).where(table.c.object_full_name == object_full_name)
                             .values(full_load_is_required=True))

    def require_full_load(self, object_full_name: str) -> None:
        """
        Помечает объект как требующий полной выгрузки БЕЗУСЛОВНО — в отличие от
        require_full_load_if_new, которая срабатывает только для ни разу не выгружавшихся.

        Нужно там, где уже загруженные данные обесценились не по вине источника: колонка сменила
        тип и старая отставлена в сторону, новая пуста (см. DBWriter._retype_changed_columns).
        Заказ снимет сам прогон выгрузки, как и для нового объекта.
        """
        table = self.objects_table
        if table is None:
            logger.warning('Cannot request a full load of %s: object registry is not set up '
                           '(no engine)', object_full_name)
            return
        with self.engine.begin() as conn:
            conn.execute(update(table).where(table.c.object_full_name == object_full_name)
                         .values(full_load_is_required=True))

    def was_fully_loaded(self, object_full_name: str) -> bool:
        """
        Выгружался ли объект целиком хоть раз (last_full_load_dt заполнен).

        Отметка лежит в БД, а не в памяти процесса, поэтому переживает перезапуск: расписание,
        поднятое заново, не станет перечитывать историю второй раз. Строки в реестре нет — считаем,
        что не выгружался: реестр синхронизируется с $metadata, и её отсутствие значит «объекта ещё
        не видели», а не «уже загружен».
        """
        table = self.objects_table
        if table is None:
            # Реестр создаётся первой синхронизацией метаданных (get_metadata → _sync_objects), и
            # к этому моменту он обычно уже есть: расписание грузит метаданные, разбирая имена.
            # Если всё же нет — сведений о выгрузке у нас нет, и безопаснее считать, что её не было:
            # лишний раз прочитать объект целиком дешевле, чем не собрать историю вовсе.
            return False
        with self.engine.connect() as conn:
            row = conn.execute(select(table.c.last_full_load_dt)
                               .where(table.c.object_full_name == object_full_name)).first()
        return row is not None and row.last_full_load_dt is not None

    def list_full_load_required(self) -> list[str]:
        """
        Полные имена объектов, ожидающих полной выгрузки (full_load_is_required).

        Помеченные отсутствующими в `$metadata` исключаются: выгружать их негде — 1С такого объекта
        не отдаст. Заказ при этом НЕ снимаем: вернётся объект в публикацию — вернётся и он.
        """
        table = self.objects_table
        with self.engine.connect() as conn:
            return list(conn.execute(
                select(table.c.object_full_name)
                .where(table.c.full_load_is_required,
                       table.c.absent_from_metadata.is_not(True))).scalars())

    def mark_full_loaded(self, object_full_name: str, rows_modified: int | None = None,
                         minutes: float | None = None) -> None:
        """
        Фиксирует успешную полную выгрузку: ставит last_full_load_dt=now(), снимает требование и
        записывает метрики прогона. Ключ — полное имя (object_full_name), а не короткое: имена
        регистра и документа могут совпасть.

        rows_modified — сколько строк выгрузка на самом деле изменила (вставила, обновила, удалила).
        Это проверка самого CDC: если изменения доезжают исправно, полная выгрузка находит ровно то,
        что уже лежит в БД, и значение должно быть 0. Ненулевое — повод разобраться, что не доехало.

        minutes — сколько прогон занял, дробное. Нужно, чтобы понимать цену перевыгрузки объекта и
        замечать деградацию.
        """
        table = self.objects_table
        with self.engine.begin() as conn:
            conn.execute(update(table).where(table.c.object_full_name == object_full_name)
                         .values(last_full_load_dt=func.now(), full_load_is_required=False,
                                 last_full_load_rows_modified=rows_modified,
                                 last_full_load_minutes=minutes))