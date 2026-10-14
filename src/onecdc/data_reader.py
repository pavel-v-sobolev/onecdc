"""
Чтение и разбор данных 1С через OData.

DataReader читает страницы объектов и сохраняет их в DataObject, где значения
сгруппированы по колонкам. Табличные части представлены отдельными DataObject
и связаны с владельцем. При включённой опции модуль дочитывает субконто регистров
бухгалтерии. Чтение пакетов изменений реализовано наследником ChangeReader.
"""

from __future__ import annotations

import requests
from typing import Any
from collections import Counter, UserDict
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from urllib.parse import quote
import uuid

from onecdc.metadata_reader import (COMPOSITE_GUID_SUFFIX, ENTITY_TYPES, REGISTER_TYPES,
                                    SUPPORTED_TYPES, MetadataReader, resolve_timeout)
from onecdc.common_functions import (format_bytes, odata_auth_header,
                                     odata_datetime_value, read_within_limit,
                                     parse_object_full_name, parse_odata, raise_for_status)
from onecdc.logging_config import get_logger

logger = get_logger(__name__)


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
# Поле 1С «версия данных» у ссылочных объектов. Меняется при каждой записи объекта, даже если
# ни один реквизит не изменился, поэтому в сравнении строк не участвует (см. DBWriter.save).
#
# Имён два, потому что документация и реальный ответ расходятся: «Приложение 12. Описание
# сущностей, предоставляемых через стандартный интерфейс OData» описывает свойство как Version
# («Версия данных»), а платформа отдаёт DataVersion — проверено и на рабочей базе, и на записанных
# ответах чужой базы в tests/responses. Расхождение системное: там же документированы Ref и
# Recorder, а в ответе OData приходят Ref_Key и Recorder_Key. Поэтому принимаем оба имени и берём то,
# что реально пришло в записи.
VERSION_FIELDS = ('DataVersion', 'Version')
# Спец-поле, заполняемое при загрузке. Универсальный признак «строку не учитывать»:
# пометка удаления объекта (DeletionMark), неактивная запись регистра (Active=False)
# и фиктивные записи (удаленный набор регистра, опустевшая табличная часть).
# Полное описание логики — в README_DB.md, раздел про is_deleted_or_empty.
IS_DELETED_OR_EMPTY_FIELD = 'is_deleted_or_empty'
# Спец-поле: номер пакета обмена (message_no), проставляется во все записи при чтении изменений.
EXCHANGE_MESSAGE_NO_FIELD = 'exchange_message_no'
# Номер пакета, которым полная выгрузка подписывает свои строки. Ниже любого номера сообщения 1С
# (те начинаются с 1), поэтому снимок не выглядит новее изменения.
#
# Это ещё и след автора записи, на котором держится guard снимка: по нему вставка отличает строку,
# переписанную потоком изменений, от той, что записала она сама несколькими миллисекундами раньше
# (см. DBWriter._group_not_touched_since). Менять значение нельзя, не поменяв guard.
FULL_LOAD_MESSAGE_NO = 0

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

# Числовые типы 1С. Нужны, чтобы согласовать два ответа платформы об одном и том же движении:
# см. _zero_empty_numerics.
NUMERIC_TYPES = ('Double', 'Int64', 'Int32', 'Int16')

# Потолок размера ответа 1С. Тело загружается целиком, и дальше от него живут ещё несколько
# представлений — текст, дерево разбора, колоночные списки: измерено ×3.6 на пике. Без потолка
# достаточно крупный пакет убивает процесс по OOM молча, а следующий цикл запрашивает его снова.
#
# Предел 512 МиБ ограничивает тело ответа, но не общую память процесса: разбор и
# параллельные выгрузки требуют дополнительного места. Для больших пакетов нужен дроссель: сформировать пакет поменьше 1С со стороны
# клиента не заставить, $top и $filter она для SelectChanges игнорирует.
MAX_RESPONSE_BYTES = 512 * 1024 * 1024

# Пустая ссылка 1С — нулевой GUID (в данных 1С отдаёт именно его).
EMPTY_UUID = uuid.UUID(int=0)


def _odata_literal(value: Any, type_name: str) -> str:
    """
    OData-литерал значения по типу поля (курсор по периоду, фильтр по дате): Guid → guid'…',
    DateTime → datetime'…', числа — как есть, Boolean → true/false, остальное (String и пр.) —
    строка в кавычках, кавычка внутри удваивается. value приходит уже сконвертированным
    (_convert_value): UUID/datetime.
    """
    if type_name == 'Guid':
        return f"guid'{value}'"
    if type_name == 'DateTime':
        # Год — четырьмя знаками, иначе 1С отвергает литерал (см. odata_datetime_value): значения
        # сюда приходят прочитанными из самой 1С, а среди них бывает пустая дата 0001-01-01.
        v = odata_datetime_value(value) if isinstance(value, (datetime, date)) else str(value)
        return f"datetime'{v}'"
    if type_name in ('Int64', 'Int32', 'Int16', 'Double'):
        # Дробное — без экспоненты: str() отдаёт для крайних величин '1e-07' и '1E+20', а такой
        # литерал 1С в $filter не принимает. format(..., 'f') разворачивает их в обычную запись.
        if isinstance(value, (Decimal, float)):
            return format(Decimal(str(value)), 'f')
        return str(value)
    if type_name == 'Boolean':
        return 'true' if value else 'false'
    return "'" + str(value).replace("'", "''") + "'"


class DataObject(UserDict):
    """
    Данные объекта 1С в виде {поле: [значения]} и связанные с ним табличные части.

    Такой формат используется при передаче в dbmerge. Если поле впервые встретилось в середине
    страницы, его список дополняется значениями NULL для всех предыдущих записей.
    """

    def __init__(self, metadata_obj=None, records: list = []):
        super().__init__()
        self.metadata_obj = metadata_obj  # MetadataObject
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

    def to_records_mapped(self, column_mapping: dict[str, str] | None = None) -> list[dict]:
        """
        Преобразует колоночное хранилище (dict of lists) в список записей (list of dict)
        для передачи в dbmerge. Если задан column_mapping, имена колонок заменяются на лету,
        без отдельной переименованной копии данных (значения колонок переиспользуются по ссылке).

        Значения отдаются как разобраны — UUID, datetime, Decimal: ровно их и ждёт dbmerge,
        чтобы в uuid-колонку уехал uuid, а в NUMERIC — точное число.
        """
        keys = list(self.data.keys())
        out_keys = [column_mapping.get(k, k) for k in keys] if column_mapping else keys
        cols = list(self.data.values())
        return [dict(zip(out_keys, row)) for row in zip(*cols)]


def _scalar_property_value(value: Any) -> tuple[bool, str | None]:
    """
    Разбирает значение свойства из m:properties: (это скаляр?, сырое значение).

    xmltodict отдаёт одно и то же свойство четырьмя разными способами, и по типу python-значения
    скаляр от табличной части не отличить:
      <d:Артикул>A-100</d:Артикул>                -> 'A-100'                  скаляр
      <d:Артикул/>                                -> None                     пустой скаляр
      <d:Цена m:type="Edm.Double">1.5</d:Цена>    -> {'@m:type':…,'#text':…}   скаляр с явным типом
      <d:Представления m:type="Collection(…)"/>   -> {'@m:type':'Collection('} табличная часть

    Раньше бралось только `isinstance(v, str)`, и пустой элемент пропадал вместе со свойством:
    очистка реквизита в 1С не отражалась в БД — колонки не было ни в данных, ни в UPDATE, и старое
    значение оставалось навсегда. Пустых элементов в ответах 1С не единицы: в одном пакете
    номенклатуры их 90 (Артикул, Описание, PredefinedDataName, КодТРУ…).

    Пустой скаляр отдаём как None: в 1С у реквизита нет отдельного «не заполнено», пусто — это
    пусто, а NULL в колонке уже и так пишется, когда поле пустое лишь у части записей пачки.

    Неоднозначные формы (m:null / xsi:nil без m:type) остаются табличными частями, как и было:
    по одному лишь значению пустая ТЧ от пустого скаляра не отличается, и трогать это здесь
    значило бы менять поведение там, где оно не сломано.
    """
    if isinstance(value, str):
        return True, value
    if value is None:
        return True, None
    if isinstance(value, dict):
        if str(value.get('@m:type', '')).startswith('Collection('):
            return False, None
        if '#text' in value:
            return True, value['#text']
    return False, None


def _composite_reference_fields(raw: dict, metadata_obj) -> set:
    """
    Составные поля, в которых у этой записи лежит ссылка (а не число, строка или дата).

    Тип конкретного значения приходит в парном <поле>_Type: у ссылок это имя объекта 1С, у
    примитивов — Edm.Double / Edm.String / Edm.DateTime и т.п. Метаданные OData объявляют само поле
    строкой и о составе типа не говорят ничего, поэтому решать приходится по каждой записи.

    Само поле хранит значение как пришло, текстом, всегда. Ссылка дополнительно кладётся в
    соседнюю <поле>_Guid — по ней джойнятся ключи других таблиц, а это основной сценарий.

    Почему не наоборот (uuid в самом поле, примитив в соседней колонке), как было раньше: тогда у
    примитива в самом поле оставался NULL, а поле бывает и в первичном ключе — и две записи с
    разными значениями получали один и тот же ключ, потому что NULL в ключе заменяется нулевым
    guid. Дальше либо склейка, либо падение всей пачки на уникальном индексе.
    """
    references = set()
    for field_name, value in raw.items():
        if not field_name.endswith('_Type') or not isinstance(value, str):
            continue
        base_name = field_name.removesuffix('_Type')
        if base_name in raw and base_name + COMPOSITE_GUID_SUFFIX in metadata_obj.keys():
            if not value.startswith(EDM_TYPE_PREFIX):
                references.add(base_name)
    return references


class DataReader(UserDict):
    """
    Читать страницы OData и хранить разобранные объекты по их полным именам.

    Перед каждым чтением данные очищаются, а служебные кэши сохраняются. Для полной
    выгрузки создаётся отдельный читатель на прогон; ChangeReader используется между
    циклами CDC. Метаданные и NameMapper передаются извне.
    """

    def __init__(self, odata_url: str, metadata: MetadataReader,
                 odata_auth: tuple[str, str] | None = None,
                 request_timeout: float | None = None,
                 max_response_bytes: int | None = MAX_RESPONSE_BYTES):
        super().__init__()
        self.odata_url = odata_url
        self.metadata = metadata
        # Пара (пользователь, пароль) уходит в requests не кортежем, а своим заголовком:
        # его Basic кодируется в latin-1 и падает на кириллице (см. odata_auth_header).
        self.odata_auth = odata_auth_header(odata_auth)
        self.request_timeout = request_timeout
        # {объект: причина} последнего разбора — см. read_data_entries.
        self.failed_objects: dict[str, str] = {}
        # Сколько байт ответа согласны принять; None — без ограничения (см. read_within_limit).
        self.max_response_bytes = max_response_bytes
        # номер пакета обмена, проставляется в записи при чтении изменений
        self.exchange_message_no = None  

        # Размер последнего ответа 1С в байтах — по нему вызывающий подбирает размер страницы
        # (вес одной entry у разных объектов различается на порядки, см. Replicator.full_load).
        self.last_response_bytes = 0

        # Объекты, ради неизвестного поля которых метаданные уже перечитывались (см. _get_record_fields).
        # Без этого каждая запись с полем вне $metadata давала бы свой полный GET $metadata + dbmerge.
        self._metadata_refreshed_for: set[str] = set()

        # Пары (объект, поле), о которых уже предупреждали. Перечитывание метаданных ограничено
        # одним разом на объект, а сама строка лога — нет: поле вне $metadata давало запись на
        # каждую запись страницы, то есть десятки тысяч одинаковых строк на прогон.
        self._warned_unknown_fields: set[tuple[str, str]] = set()

    def read_object(self, object_name: str, top: int | None = None,
                    key_fields: list[str] | None = None, extra_filter: str | None = None,
                    skip: int | None = None) -> int:
        """
        Прочитать страницу объекта, заменив предыдущее содержимое читателя.

        key_fields задаёт поля сортировки; top и skip — число entry и смещение от начала.
        extra_filter добавляет условие OData. Возвращается число entry верхнего уровня,
        а не суммарное число вложенных строк.

        Документ или справочник содержит одну основную запись и табличные части;
        entry регистра по регистратору — весь его набор записей. Поэтому размер страницы
        не определяет объём ответа. Ключи независимого регистра берутся из метаданных.

        Используется $skip: сравнение ссылочных ключей не позволяет построить надёжный
        keyset-курсор в 1С. Деление по датам и подбор размера выполняет Replicator.
        Ограничения порядка страниц при изменении источника описаны в DESIGN.md.
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
        # продолжает первую только если 1С упорядочила обе одинаково. Одного ключа страницы для
        # этого мало там, где по нему есть равные строки: у табличной части ключ страницы —
        # Ref_Key (владелец), а строк у одного владельца несколько, и все они по Ref_Key равны.
        # Порядок внутри такой ничьей 1С не обещает и от запроса к запросу может выдать разный —
        # тогда на границе страниц строки и дублируются, и теряются. Остальные поля ключа
        # (LineNumber) ничью разрешают: полный первичный ключ уникален по определению.
        #
        # Сам репликатор так табличные части НЕ грузит: они поступают вложенными в entry своего
        # документа или справочника, то есть целой группой, и страницы владельца их не режут
        # (см. Replicator.run_once, list_objects). Отдельная сущность у табличной части в OData
        # всё же есть, и прямой read_object может читать её по Ref_Key. Публичный full_load
        # такой вызов запрещает, поскольку запись требует целой группы владельца. Измерено: 87 строк, страницы по 2,
        # $orderby=Ref_Key — 4 дубля и 5 потерянных строк; с LineNumber — ровно 87.
        #
        # По классам объектов ничья выглядит так (всё проверено на реальной 1С):
        # - документ/справочник: ключ Ref_Key уникален, равных строк нет; $orderby по ссылке 1С
        #   разворачивает в автоупорядочивание (сортирует по представлению), но порядок при этом
        #   один и тот же от запроса к запросу — этого $skip и достаточно;
        # - независимый регистр сведений: поля ключа реальные и лежат на верхнем уровне entry,
        #   $orderby по ним работает (asc и desc дают разный порядок, несуществующее поле → 400),
        #   а ключ страницы — весь первичный ключ, то есть уникален по определению;
        # - регистр, подчинённый регистратору: одна entry — это целый набор записей регистратора
        #   (наверху лежат только Recorder/Recorder_Type, строки набора — внутри RecordSet).
        #   Recorder уникален по построению, ничьей нет, и страница набор не рвёт. Дополнять
        #   сортировку тут нечем: LineNumber и Период на верхнем уровне отсутствуют — и такой
        #   $orderby регистр молча игнорирует (проверено: Recorder, Recorder+LineNumber, один
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
        context = f'read {object_name}{query}'
        text_body = self._get_within_limit(url, context)

        feed = parse_odata(text_body, 'feed', context,
                           force_list=('d:element', 'entry'))
        object_entries = (feed or {}).get('entry') or []

        self.clear()
        self.read_data_entries(object_entries)
        logger.info('Read %s: %s entries, %s rows, %s', object_name, len(object_entries),
                    self.rows_read(), format_bytes(self.last_response_bytes))
        return len(object_entries)

    def _get_within_limit(self, url: str, context: str) -> str:
        """
        Выполнить GET с ограничением размера; вернуть текст ответа и сохранить его размер.

        stream=True позволяет прекратить чтение при превышении предела, не загружая всё тело.
        Тело ошибочного ответа читается для извлечения диагностического сообщения 1С.
        """
        with requests.get(url, auth=self.odata_auth, stream=True,
                          timeout=resolve_timeout(self.request_timeout)) as response:
            if not response.ok:
                raise_for_status(response, context)
            body = read_within_limit(response, self.max_response_bytes, context)
            encoding = response.encoding or 'utf-8'
        self.last_response_bytes = len(body)
        return body.decode(encoding, errors='replace')

    def _zero_empty_numerics(self, object_name: str, record: dict) -> None:
        """
        Заполнить нулями отсутствующие числовые поля записи регистра.

        Незаполненное число может прийти как <d:КоличествоDr m:null="true"/>. Без явного типа
        эта форма неотличима от пустой табличной части, поэтому _scalar_property_value пропускает
        поле. Без дополнительной обработки в колонку был бы записан NULL.

        В 1С незаполненный числовой ресурс означает ноль. В целевой БД NULL имеет другой смысл:
        DBWriter._resource_reset_values использует его для исключённых из расчёта строк.

        Нормализация нужна для обоих источников: набора записей и виртуальной таблицы. На демобазе
        БП 15.09.2026 оба источника возвращали m:null для ВалютнаяСумма* и Количество* и ноль
        для СуммаВР* и СуммаПР*. Раньше нормализовалась только виртуальная таблица: CDC записывал
        NULL, а полная выгрузка заменяла его нулём. Повторная выгрузка поэтому показывала изменения
        даже при неизменных данных 1С.
        """
        metadata_obj = self.metadata.get(object_name)
        if metadata_obj is None:
            return
        for field, type_name in metadata_obj.items():
            if type_name in NUMERIC_TYPES and record.get(field) is None:
                record[field] = 0

    def read_date_bound(self, object_name: str, date_field: str, *, newest: bool,
                        extra_filter: str | None = None) -> datetime | None:
        """
        Самая ранняя (newest=False) или самая поздняя (newest=True) дата объекта — одной строкой:
        `$top=1` с сортировкой по этому полю. Нужна, чтобы обойти полную выгрузку окнами по
        периоду и не гонять `$skip` по всей таблице (см. Replicator.full_load).

        Запрос дешёвый именно потому, что поле даты у документа входит в индекс: 1С отдаёт первую
        строку упорядоченной выборки, а не сортирует всё.

        Метод применим не ко всем объектам. У регистра, подчинённого регистратору, entry — это
        набор записей, даты на верхнем уровне нет, и `$orderby` по ней платформа принимает с
        кодом 200 и молча игнорирует: проверено на реальной 1С, что `$orderby=Period`,
        `$orderby=Period desc` и запрос без сортировки отдают одну и ту же первую entry. Так что
        «первая строка упорядоченной выборки» там не значит ничего, и доставать дату из вложенного
        RecordSet, чтобы «починить» этот метод, нельзя — вернётся не граница, а произвольное
        значение. Кому этот метод годится, решает Replicator._supports_date_bounds.

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

        entries = (parse_odata(response.text, 'feed', f'read {object_name} date bound',
                               force_list=('entry',)) or {}).get('entry') or []
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
        """Вернуть число разобранных строк, включая вложенные табличные части и наборы записей."""
        return sum(data_object.data_length for data_object in self.values())

    def read_data_entries(self, object_entries: list) -> Counter:
        """
        Разбирает entry ответа 1С в объекты reader. Возвращает счётчик entry по объектам —
        вызывающий логирует итог одной строкой: entry в ответе бывают тысячами, и лог на каждую
        забивает вывод (см. read_object / ChangeReader.read_changes).

        Объекты неподдерживаемых классов (см. SUPPORTED_TYPES) пропускаются, и это **потеря
        данных**: пакет подтверждается целиком, значит 1С такие изменения больше не пришлёт, а
        дочитать их полной выгрузкой нельзя — разобрать этот класс мы не умеем.

        Уровень ERROR, а не WARNING. Формально это действительно состав плана обмена, а не сбой
        прогона, и раньше здесь стоял WARNING именно поэтому. Но предупреждение раз в пакет никто
        не читает, а цена — молча исчезающие изменения; из всех строк лога эта заслуживает
        внимания больше прочих. Лог один на пакет, а не на entry: entry бывают тысячами.
        """
        parsed: Counter = Counter()
        unsupported: Counter = Counter()
        # {объект: причина} — что не удалось разобрать в этом пакете. Решение о судьбе пакета
        # принимает вызывающий: читателю неоткуда знать, временная это беда или навсегда.
        self.failed_objects: dict[str, str] = {}
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

            # Разбор одной entry не вправе уронить весь пакет. До этой обёртки любое неустранимое
            # исключение на одном объекте (табличная часть, чей _RowType не включён в состав
            # OData; объект, пропавший из $metadata; конфликт типов колонки) выбрасывало нас из
            # всего разбора: пакет не дочитан, ничего не сохранено, подтверждение не отправлено —
            # и так каждый цикл, с паузой до получаса. Один объект останавливал весь план обмена.
            #
            # Решает не обёртка сама по себе, а то, что делает с ней вызывающий: пакет с
            # неразобранными объектами не подтверждается, пока у них не кончатся попытки
            # (см. Replicator._run_once_claimed). Здесь мы только доносим факт и причину.
            try:
                if object_type in REGISTER_TYPES:
                    self._get_register_records(object_name, properties)

                if object_type in ENTITY_TYPES:
                    self._get_entity_records(object_name, properties)
            except Exception as error:
                self.failed_objects[object_name] = f'{type(error).__name__}: {error}'
                logger.exception('Failed to parse %s from the changes package', object_name)

        if unsupported:
            logger.error(
                "CHANGES LOST: objects of unsupported classes were skipped — %s. The exchange "
                "message is confirmed as a whole, so 1C will not send them again, and a full load "
                "cannot recover them either: these classes are not readable at all. Remove such "
                "objects from the exchange plan (supported classes: %s)",
                ', '.join(f'{name} ({n} entries)' for name, n in unsupported.items()),
                ', '.join(SUPPORTED_TYPES))

        return parsed


    def _add_records(self, object_name, new_records: list):
        if new_records:
            if object_name in self.keys():
                self[object_name].add_records(new_records)
            else:
                metadata_obj = self.metadata.get(object_name)
                self[object_name] = DataObject(metadata_obj=metadata_obj, records=new_records)

    @staticmethod
    def _convert_value(value: Any, type_name: str, context: str = '') -> Any:
        # Конвертируем значение в нужный тип данных на основе метаданных.
        if value is None:
            return None
        if not isinstance(value, str):
            # Само значение — в DEBUG: в логе оно бизнес-данные, а для разбора нужно не всегда.
            logger.warning('Expected a string for conversion of %s to %s, got %s',
                           context or 'field', type_name, type(value).__name__)
            logger.debug('Value of %s was %r', context or 'field', value)
            return value
        try:
            if type_name == 'Boolean':
                return value.lower() == 'true'
            if type_name in ('Int64', 'Int32', 'Int16'):
                return int(value)
            if type_name == 'Double':
                # Decimal из исходной строки, а не float: колонка в БД — NUMERIC, она хранит
                # десятичное значение точно, и промежуточный float этому замыслу противоречит.
                # Числа 1С допускают до 38 разрядов, а у float точность кончается на 2**53:
                # проверено, '9007199254740993' превращается в 9007199254740992.0, а
                # '12345678901234567890.12' — ещё и в экспоненциальную форму. Значение при этом
                # искажается молча и стабильно: и поток изменений, и полная выгрузка читают через
                # один и тот же конвертер, поэтому согласуются друг с другом, и сверка
                # rows_modified расхождения не покажет.
                return Decimal(value)
            if type_name == 'DateTime':
                return datetime.fromisoformat(value)
            if type_name == 'Guid':
                if value == '00000000-0000-0000-0000-000000000000':
                    return None
                else:
                    return uuid.UUID(value)
        except (ValueError, TypeError, InvalidOperation) as e:
            # Раньше здесь возвращалось исходное значение — и падала вся пачка на вставке
            # (одно '6.4' в uuid-колонке останавливало загрузку объекта навсегда). Пишем NULL:
            # объект грузится, а строка с проблемой видна и в логе, и в БД.
            # Значение в WARNING не пишем: в колонке 1С лежат бизнес-данные (ИНН, суммы, ФИО),
            # а лог обычно хранится с более слабым доступом, чем сама БД. Для разбора его видно
            # в DEBUG, который включают осознанно.
            logger.warning('Failed to convert %s to type %s: %s — writing NULL',
                           context or 'field', type_name, e)
            logger.debug('Value of %s that failed conversion: %r', context or 'field', value)
            return None
        return value

    def _is_known_table_part(self, object_name: str | None, field_name: str) -> bool:
        """
        Свойство — это табличная часть владельца? Отвечают метаданные, а не форма значения.

        Форма ненадёжна. Опустевшую табличную часть 1С кодирует по-разному, и из четырёх известных
        форм по значению узнаётся только одна — `<d:X m:type="Collection(...)"/>`. Остальные три
        (`xsi:nil="true"`, `m:null="true"`, пустой элемент `<d:X/>`) неотличимы от пустого скаляра:
        надгробие не ставилось, и ранее загруженные строки части оставались жить, а пустой элемент
        после CDC-01 ещё и заводил владельцу лишнюю скалярную колонку.

        А имя — признак твёрдый: табличные части 1С публикует отдельными объектами с именем
        «владелец_ЧастьИмени», на этом же соглашении держится MetadataReader.owner_of. Если такой
        объект в метаданных есть, свойство — табличная часть, как бы 1С ни записала пустоту.

        Классификация по форме остаётся запасным путём: часть, которой в метаданных ещё нет
        (появилась между их чтением и пакетом), узнаётся по `Collection(...)` как раньше.
        """
        if not object_name:
            return False
        part = self.metadata.get(f'{object_name}_{field_name}')
        return part is not None and part.is_table_part

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
            if not k.startswith('d:'):
                continue
            field_name = k.removeprefix('d:')
            if self._is_known_table_part(object_name, field_name):
                continue          # это табличная часть, колонки у неё быть не должно
            is_scalar, value = _scalar_property_value(v)
            if not is_scalar:
                continue
            # У поля _Type в значении полное имя типа ("StandardODATA.Catalog_Контрагенты");
            # префикс убираем, остаётся имя объекта 1С либо примитив вида Edm.Double.
            if field_name.endswith('_Type') and value is not None:
                value = value.removeprefix(ODATA_PREFIX)
            raw[field_name] = value

        references = _composite_reference_fields(raw, metadata_obj)

        fields = {}
        for field_name, value in raw.items():
            if field_name not in metadata_obj.keys():
                # поле не найдено в метаданных, пробуем их перечитать — но не более одного раза
                # на объект, иначе поле, которого в $metadata нет (например, отброшенный
                # по неизвестному типу реквизит), даёт по запросу метаданных на каждую запись.
                if object_name not in self._metadata_refreshed_for:
                    self._metadata_refreshed_for.add(object_name)
                    self.metadata.get_metadata()
                    metadata_obj = self.metadata[object_name]
                if (field_name not in metadata_obj.keys()
                        and (object_name, field_name) not in self._warned_unknown_fields):
                    self._warned_unknown_fields.add((object_name, field_name))
                    logger.warning('Metadata field %s not found for object %s — reading it as '
                                   'String (reported once per field)', field_name, object_name)


            type_name = metadata_obj.get(field_name) or 'String'

            if field_name.endswith('_Type') and value is not None:
                # В колонке _Type храним имя типа без префикса поставщика: у ссылок это уже сделано
                # выше (StandardODATA.), у примитивов убираем Edm. — в БД полезно 'Double', а не
                # 'Edm.Double'. Распознавание примитива идёт по сырому значению (см. primitives).
                value = value.removeprefix(EDM_TYPE_PREFIX)

            if field_name in references:
                # Ссылка: кладём её ещё и в соседнюю uuid-колонку, чтобы по ней джойнились ключи
                # других таблиц. Само поле остаётся текстом со значением как пришло — ниже.
                fields[field_name + COMPOSITE_GUID_SUFFIX] = self._convert_value(
                    value, 'Guid', context=f'{object_name}.{field_name}')

            converted = self._convert_value(value, type_name,
                                            context=f'{object_name}.{field_name}')

            if converted is None and field_name in metadata_obj.primary_key:
                # Поля первичного ключа в целевой таблице NOT NULL, а пустую ссылку 1С
                # (нулевой GUID) _convert_value превращает в None — для ключа оставляем
                # дефолт по типу (нулевой GUID), иначе строка не вставится.
                converted = self._default_key_value(type_name)

            fields[field_name] = converted

        # Поля ключа в целевой таблице NOT NULL, а 1С пустое измерение присылает пустым элементом
        # (в json — null) либо не присылает: пустая строка в «ИдентификаторСтроки» регистра
        # приходит именно так. Без дефолта такая запись роняет вставку всей пачки по NOT NULL.
        # Поля регистратора пропускаем: в строках набора их нет по определению, и проставляются
        # они уровнем выше, из entry (_entry_recorder_fields) — дефолт тут забил бы их нулевым guid.
        for key_field, key_type in metadata_obj.primary_key.items():
            if (fields.get(key_field) is None
                    and key_field not in RECORDER_FIELDS and key_field != RECORDER_TYPE_FIELD):
                fields[key_field] = self._default_key_value(key_type)

        # Соседняя колонка составного поля заполняется у каждой записи, даже когда значение —
        # примитив и ссылки в нём нет: иначе состав полей плавал бы от записи к записи.
        for field_name in raw:
            if field_name.endswith('_Type'):
                guid_name = field_name.removesuffix('_Type') + COMPOSITE_GUID_SUFFIX
                if guid_name in metadata_obj.keys():
                    fields.setdefault(guid_name, None)

        # Спец-поле «строку не учитывать»: пометка удаления у документа/справочника либо
        # неактивная запись регистра. Active сравниваем именно с False: у объектов его нет,
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

        # Числовым ресурсам явно присваиваем NULL.
        #
        # Так сбрасываются суммы у строки, совпавшей с фиктивной записью по ключу, и у остальных
        # строк опустевшего набора: DBWriter._resource_reset_values определяет поля для сброса
        # по полям записи. Без явно указанных ресурсов результат зависел от состава пакета.
        # Если в нём был непустой набор того же регистра, DataObject дополнял фиктивную запись
        # его колонками, и суммы сбрасывались. Пакет с единственным пустым набором оставлял
        # суммы распроведённого документа в таблице (CDC-23).
        #
        # NULL и ноль здесь имеют разный смысл: NULL обозначает удалённую строку, ноль — пустой
        # ресурс существующей строки (см. _zero_empty_numerics). В отличие от NULL, ноль также
        # учитывается в COUNT и AVG, поэтому заменять NULL на ноль в этой ветке нельзя.
        #
        # Строковые ресурсы не трогаем — суммировать их некому, а NULL стёр бы содержимое.
        for resource in metadata_obj.resources:
            if metadata_obj.get(resource) in NUMERIC_TYPES:
                record[resource] = None

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

        Результат добавляется в DataObject соответствующего регистра: значения хранятся
        по колонкам в списках, а не как list of dict.
        """
        if RECORD_SET_FIELD not in properties:
            # Независимый регистр сведений: одна плоская запись, поля прямо в properties.
            record = self._get_record_fields(properties, object_name)
            self._zero_empty_numerics(object_name, record)
            self._add_records(object_name, [record])
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
                self._zero_empty_numerics(object_name, row)
                new_records.append(row)
        else:
            # Регистраторный регистр с пустым набором — набор записей удалён.
            deleted_record = self._make_deleted_register_record(object_name, properties)
            new_records = [deleted_record] if deleted_record is not None else []

        self._add_records(object_name, new_records)

    def _get_record_table_parts(self, properties, object_name: str | None = None):
        """
        Ищем табличные части в свойствах объекта: сначала по метаданным (см. _is_known_table_part),
        и только потом — по форме значения, для частей, которых в метаданных ещё нет.

        Классификатор формы общий с _get_record_fields — чтобы одно и то же свойство не попало
        разом и в колонку, и в табличную часть: скаляр с явным типом
        (<d:Цена m:type="Edm.Double">1.5</d:Цена>) в xmltodict тоже dict.
        """
        table_parts = {}
        for k, v in properties.items():
            if not k.startswith('d:'):
                continue
            field_name = k.removeprefix('d:')
            if self._is_known_table_part(object_name, field_name):
                # Пустоту 1С кодирует четырьмя способами, и в трёх из них значение — не словарь
                # либо словарь без @m:type. Приводим к общему виду: строк нет, а имя объекта
                # выводится из имени владельца (см. _get_entity_records).
                table_parts[field_name] = v if isinstance(v, dict) else {}
                continue
            if not isinstance(v, dict) or v.get('@xsi:nil') == 'true':
                continue
            is_scalar, _ = _scalar_property_value(v)
            if not is_scalar:
                table_parts[field_name] = v
        return table_parts

    def _get_entity_records(self, object_name: str, properties: dict):
        """
        Разобрать запись справочника или документа и связанные с ней табличные части.

        Основная запись и части сохраняются как отдельные DataObject. Части дополнительно
        связываются с владельцем для экспорта вложенной структуры.
        """
        fields = self._get_record_fields(properties, object_name)
        self._add_records(object_name, [fields])

        # Строки табличных частей приходят без ссылки на владельца — проставляем Ref_Key документа.
        ref_key = fields.get('Ref_Key')
        # Пометку удаления документа/справочника распространяем на его табличные части.
        parent_deleted = fields.get(IS_DELETED_OR_EMPTY_FIELD)

        table_parts = self._get_record_table_parts(properties, object_name)

        for table_part_key, table_part in table_parts.items():
            table_part_full_name = table_part.get('@m:type')
            if table_part_full_name:
                table_part_name, _ = parse_object_full_name(table_part_full_name)
            else:
                # @m:type у опустевшей части 1С указывает не всегда — имя выводим из имени
                # владельца. Сюда доходят только части, подтверждённые метаданными
                # (см. _is_known_table_part), поэтому имя заведомо верное.
                table_part_name = f'{object_name}_{table_part_key}'

            table_part_rows = table_part.get('d:element') or []
            if table_part_rows:
                for table_part_row in table_part_rows:
                    row = self._get_record_fields(table_part_row, table_part_name)
                    row['Ref_Key'] = ref_key
                    row[IS_DELETED_OR_EMPTY_FIELD] = parent_deleted
                    self._add_records(table_part_name, [row])
            elif ref_key is not None and table_part_name in self.metadata.keys():
                # Табличная часть пришла без строк — добавляем фиктивную запись, чтобы
                # scoped-удаление по Ref_Key убрало ранее сохранённые строки.
                self._add_records(table_part_name,
                                  [self._make_empty_table_part_record(table_part_name, ref_key)])

