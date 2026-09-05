
from datetime import datetime

from sqlalchemy import (Engine, Index, JSON, MetaData, Table, Integer, Numeric,
                        tuple_, select, or_, and_, exists)
from sqlalchemy.dialects.postgresql import JSONB
from dbmerge import dbmerge, mergeResult

from cdc_1c.data_reader import (DataObject1C, EXCHANGE_MESSAGE_NO_FIELD,
                                IS_DELETED_OR_EMPTY_FIELD, VERSION_FIELDS)
from cdc_1c.common_functions import DB_NOW_WITHOUT_TIMEZONE
from cdc_1c.name_mapper import NameMapper1C
from cdc_1c.logging_config import get_logger

logger = get_logger(__name__)

# Диалекты, где JSON-колонку надо объявлять как JSONB (см. save): у postgres-типа json нет
# оператора сравнения, и dbmerge на нём отказывается писать. Список — как в dbmerge.
JSONB_DIALECTS = ('postgresql', 'cockroachdb')

# Служебные поля, которыми управляет dbmerge (момент merge/первой вставки строки).
MERGED_ON_FIELD = 'merged_on'
INSERTED_ON_FIELD = 'inserted_on'

# Порядок сохранения объектов: ссылочные (справочники и родня) → документы → регистры. Документы
# ссылаются на справочники (по *_Key), регистры — на документы (Recorder), поэтому родителей
# сохраняем раньше. Табличные части (Catalog_X_Y / Document_X_Y) попадают в группу своего владельца
# по префиксу. Состав ссылочных классов — см. ENTITY_TYPES в metadata_reader.
SAVE_ORDER_PREFIXES = ('Catalog', 'ChartOfCharacteristicTypes', 'ChartOfAccounts',
                       'ChartOfCalculationTypes', 'BusinessProcess', 'Task',
                       'Document', 'InformationRegister', 'AccumulationRegister',
                       'AccountingRegister')


def save_order_key(object_name: str) -> int:
    """Приоритет объекта в порядке сохранения (см. SAVE_ORDER_PREFIXES); неизвестные типы — в конец."""
    for i, prefix in enumerate(SAVE_ORDER_PREFIXES):
        if object_name.startswith(prefix):
            return i
    return len(SAVE_ORDER_PREFIXES)


class DBWriter1C:
    """
    Сохраняет объекты 1С (DataObject1C) в БД через dbmerge, по одному вызовом save().
    Имена таблиц и колонок переводятся NameMapper1C, типы и первичный ключ берутся из метаданных.

    Заполняет служебные merged_on/inserted_on и создаёт индекс по merged_on (для инкрементальной
    материализации). Лог загрузки (replicator_1c_log) пишет оркестратор Replicator1C — у writer-а нет
    контекста обмена (его можно использовать и для полной перевыгрузки через read_object, где нет
    номера пакета).

    Гонка полной выгрузки с потоком изменений (см. save, full_load_started_at): снимок полной
    выгрузки читается долго и к моменту записи может устареть, поэтому он не трогает строки,
    переписанные уже после отметки своей СТРАНИЦЫ (guard'ы по merged_on). Всё, что старше этой
    отметки, он вправе перезаписать — за счёт этого полная выгрузка остаётся рабочим способом
    выровнять данные, если изменение потерялось или не зарегистрировалось в 1С. Изменения
    авторитетны и идут в порядке пакетов, поэтому guard'ами не ограничиваются.
    """

    def __init__(self, engine: Engine, name_mapper: NameMapper1C, schema: str | None = None,
                 temp_schema: str | None = None):
        self.engine = engine
        self.name_mapper = name_mapper
        self.schema = schema
        # Схема промежуточных таблиц dbmerge. None — та же, что у данных (умолчание dbmerge).
        # Отдельная схема удобна тем, что в ней по определению нет ничего ценного: временную
        # таблицу, оставшуюся после падения процесса, там видно и не жалко удалить.
        self.temp_schema = temp_schema
        # Таблицы, для которых индекс по merged_on уже обеспечен в этом процессе (чтобы не рефлексить
        # и не дёргать checkfirst на каждом save).
        self._indexed_tables: set[str] = set()

    def save(self, object_name: str, data_object: DataObject1C,
             full_load_started_at: datetime | None = None) -> mergeResult | None:
        """
        Сохраняет один объект через dbmerge.

        Режим изменений (full_load_started_at=None, по умолчанию): для регистров/табличных частей —
        scoped-удаление по object_key (набор группы заменяется целиком), для документов/справочников —
        чистый upsert по ключу. Изменения авторитетны, поэтому guard'ами не ограничиваются.

        Режим полной выгрузки (передан full_load_started_at — отметка ЭТОЙ страницы: граница по
        реестру незавершённых merge, взятая перед её чтением, см. Replicator1C._load_pages):
        применяются guard'ы по merged_on, чтобы устаревший снимок не затирал изменения, пришедшие
        уже после этой отметки:
        - документ/справочник: upsert без удаления + update_condition (перезаписываем строку, только
          если её не переписывали с момента отметки; удаление у документов мягкое — строка остаётся,
          это update);
        - регистр/табличная часть: own-or-skip группы целиком (группа умещается на одной странице) —
          scoped-удаление с guard'ом, update_condition и insert_condition, чтобы «горячую» группу
          (есть строка, переписанная после отметки) не трогать, а остальные заменить снимком.

        Строки старше отметки страницы снимок перезаписывает — это и делает полную выгрузку способом
        выровнять данные, а не только добить то, что ни разу не менялось.

        Возвращает mergeResult, либо None на ранних выходах (пустой набор / нет метаданных) —
        лог загрузки принимает None (write_result тогда просто не прибавляет счётчики).
        """
        if data_object.data_length == 0:
            return None

        logger.info(f"Saving {data_object.data_length} records of {object_name}")
        metadata_obj = data_object.metadata_obj
        if metadata_obj is None or not metadata_obj.primary_key:
            logger.warning(f'No metadata/primary key for {object_name}, skipping save')
            return None

        table_name = self.name_mapper.map_object_name(object_name)

        col_map = self.name_mapper.get_column_mapping(list(data_object.data.keys()))
        records = data_object.to_records_mapped(col_map)

        key = [self.name_mapper.map_field_name(k) for k in metadata_obj.primary_key]
        data_types = {self.name_mapper.map_field_name(col): typ
                      for col, typ in metadata_obj.get_column_types().items()}
        # JSON-колонку (субконто регистра бухгалтерии) на postgres поднимаем до JSONB: dbmerge
        # сравнивает старое значение с новым через IS DISTINCT FROM, а у типа json такого
        # оператора нет — с обычным JSON он откажется писать вовсе.
        if self.engine.dialect.name in JSONB_DIALECTS:
            data_types = {col: JSONB() if isinstance(typ, JSON) and not isinstance(typ, JSONB)
                          else typ for col, typ in data_types.items()}

        object_key = metadata_obj.object_key
        started_at = full_load_started_at
        skip_compare = self._noisy_fields(records)

        if not object_key:
            # Документ/справочник (одна запись по ключу): чистый upsert без удаления.
            with dbmerge(engine=self.engine, table_name=table_name, data=records,
                         key=key, data_types=data_types,
                         merged_on_field=MERGED_ON_FIELD, inserted_on_field=INSERTED_ON_FIELD,
                         skip_compare_fields=skip_compare,
                         delete_mode='no', schema=self.schema,
                         temp_schema=self.temp_schema) as merge:
                result = merge.exec(
                    update_condition=self._not_touched_since(merge.table, started_at)
                                     if started_at is not None else None)
        else:
            # Регистр/табличная часть: набор по object_key целиком заменяет существующий.
            # Выпавшие из набора строки помечаем, а не удаляем: исчезновение строки — такое же
            # событие, как изменение, и без следа его не увидит ни обработчик (нечему поднять
            # merged_on), ни guard полной выгрузки. Помечаем только те группы, что пришли в наборе.
            mapped_object_key = [self.name_mapper.map_field_name(k) for k in object_key]
            with dbmerge(engine=self.engine, table_name=table_name, data=records,
                         key=key, data_types=data_types,
                         merged_on_field=MERGED_ON_FIELD, inserted_on_field=INSERTED_ON_FIELD,
                         skip_compare_fields=skip_compare,
                         delete_mode='mark',
                         delete_mark_field=self.name_mapper.map_field_name(IS_DELETED_OR_EMPTY_FIELD),
                         delete_mark_values=self._resource_reset_values(metadata_obj, records),
                         schema=self.schema, temp_schema=self.temp_schema) as merge:
                scoped = self._scoped_delete_condition(merge.table, merge.temp_table, mapped_object_key)
                if started_at is not None:
                    # own-or-skip группы: не трогаем то, что переписано после старта прогона.
                    result = merge.exec(
                        delete_condition=and_(scoped, self._not_touched_since(merge.table, started_at)),
                        update_condition=self._not_touched_since(merge.table, started_at),
                        insert_condition=self._group_not_touched_since(merge, mapped_object_key,
                                                                       started_at))
                else:
                    result = merge.exec(delete_condition=scoped)

        self._ensure_merged_on_index(table_name)
        return result

    def _noisy_fields(self, records: list[dict]) -> list[str]:
        """
        Поля, отличие в которых само по себе не считается изменением строки (skip_compare_fields):
        exchange_message_no и версия данных меняются при каждой записи объекта в 1С, даже если ни
        один реквизит не изменился. Без этого шумный объект переписывал бы строку впустую, поднимая
        merged_on — а на merged_on завязаны и инкрементальная материализация, и guard'ы полной
        выгрузки. Писаться поля при этом продолжают: строку обновило что-то другое — обновятся и они.

        Версия данных ищется под обоими известными именами (VERSION_FIELDS): как поле называется в
        ответе, зависит от платформы, а не от нас. Берём те, что есть в записи.
        """
        present = records[0].keys()
        candidates = (EXCHANGE_MESSAGE_NO_FIELD, *VERSION_FIELDS)
        return [col for col in map(self.name_mapper.map_field_name, candidates) if col in present]

    def _resource_reset_values(self, metadata_obj, records: list[dict]) -> dict:
        """
        Чем ещё пометить строку, выпавшую из набора (delete_mark_values): числовые ресурсы регистра
        гасим в NULL. SUM игнорирует NULL, поэтому итог остаётся верным даже в запросе, забывшем
        фильтр по is_deleted_or_empty. Строковые ресурсы не трогаем — суммировать их некому.

        Берём только те ресурсы, что есть в текущем наборе: dbmerge требует существующую колонку,
        а таблицу он создаёт по этим же данным (набор из одной фиктивной записи ресурсов не несёт).
        Ресурсы известны не всегда — классификация полей опирается на виртуальные таблицы 1С
        (см. _classify_register_fields); нет их — гасить нечего.
        """
        column_types = metadata_obj.get_column_types()
        present = records[0].keys()
        values = {}
        for resource in metadata_obj.resources:
            column = self.name_mapper.map_field_name(resource)
            if column in present and isinstance(column_types.get(resource), (Integer, Numeric)):
                values[column] = None
        return values

    def db_now(self) -> datetime:
        """
        Текущее время ПО ЧАСАМ БД — момент старта прогона полной выгрузки. Берётся из БД, а не из
        Python, чтобы сравниваться с merged_on, который dbmerge штампует тем же now(). Вызывать
        один раз на прогон, до чтения первой страницы: старт заведомо раньше любого чтения, поэтому
        отметка защищает с запасом.

        Отсюда берётся только started_at прогона для пометки пропавших строк (см. full_load_keys).
        Guard'ы ниже и верхняя граница окна обработчиков считаются иначе — по реестру незавершённых
        merge (WriteTracker.boundary): «сейчас» перешагнуло бы строки, уже помеченные прошедшим
        merged_on, но ещё не закоммиченные.

        Отметка возвращается БЕЗ часового пояса — см. DB_NOW_WITHOUT_TIMEZONE.
        """
        with self.engine.connect() as conn:
            return conn.scalar(select(DB_NOW_WITHOUT_TIMEZONE))

    # Guard'ы полной выгрузки по merged_on. Смысл один на все три: снимок читается долго и к моменту
    # записи может устареть, поэтому он не трогает то, что переписали уже после отметки его страницы.
    # Всё, что старше отметки, снимок вправе перезаписать — данные из 1С он прочитал позже, значит
    # они не старее. merged_on IS NULL — строка из времён, когда поля ещё не было: считаем старой.
    #
    # Сравниваются, по сути, не отметки времени, а порядок чтения из 1С; отметки лишь позволяют его
    # восстановить: merged_on < started_at => транзакция изменения началась до метки => пакет пришёл
    # из 1С до метки => страница выгрузки (её читают уже после метки) не старее. Поэтому не страшно,
    # что now() в PostgreSQL — время начала транзакции и коммит может лечь позже метки.
    # Условие корректности: merged_on строки никогда не должен быть РАНЬШЕ момента получения пакета
    # из 1С. Сейчас так и есть — его штампует dbmerge в момент записи. Сломается, если брать его из
    # поля 1С или переиспользовать отметку прошлой загрузки.

    @staticmethod
    def _not_touched_since(table, started_at: datetime):
        """update_condition/delete_condition: трогаем целевую строку, только если её не переписывали
        с момента старта прогона."""
        col = table.c[MERGED_ON_FIELD]
        return or_(col.is_(None), col < started_at)

    @staticmethod
    def _group_not_touched_since(merge, mapped_object_key: list[str], started_at: datetime):
        """insert_condition: не вставлять строку в группу (по object_key), где хоть одну строку
        переписали после старта прогона — иначе снимок воскресил бы строку, удалённую изменением.
        В insert-фазе строки по PK ещё нет, поэтому проверяем на уровне группы коррелированным
        NOT EXISTS по отдельному алиасу целевой таблицы."""
        g = merge.table.alias()
        conds = [g.c[col] == merge.temp_table.c[col] for col in mapped_object_key]
        conds.append(g.c[MERGED_ON_FIELD] >= started_at)
        return ~exists().where(and_(*conds))

    def target_table(self, table_name: str) -> Table:
        """Table-описание целевой таблицы по отражению из БД. Нужно тем, кто пишет в неё не через
        dbmerge, — например пометке пропавших строк полной выгрузки (см. full_load_keys)."""
        return Table(table_name, MetaData(), schema=self.schema, autoload_with=self.engine)

    def _ensure_merged_on_index(self, table_name: str) -> None:
        """
        Индекс по merged_on — по нему обработчики выбирают изменившееся за своё окно
        (`merged_on > last_run_at`). Только средствами SQLAlchemy, идемпотентно (checkfirst):
        таблицу уже создал dbmerge. Результат кэшируется на инстансе — рефлексия/checkfirst
        выполняются один раз на таблицу.
        """
        if table_name in self._indexed_tables:
            return
        tbl = Table(table_name, MetaData(), schema=self.schema, autoload_with=self.engine)
        if MERGED_ON_FIELD in tbl.c:
            ix_name = NameMapper1C._fit_length(f'ix_{table_name}_merged_on')
            Index(ix_name, tbl.c[MERGED_ON_FIELD]).create(self.engine, checkfirst=True)
        self._indexed_tables.add(table_name)

    @staticmethod
    def _scoped_delete_condition(table, temp_table, mapped_object_key: list[str]):
        """
        Ограничивает удаление строками тех групп (по object_key), что присутствуют в staging-таблице
        (temp_table). Какие именно строки внутри групп удалить (отсутствующие в источнике),
        dbmerge определяет по PK.

        Внешние колонки — целевой таблицы, значения групп берём подзапросом из temp_table:
          один столбец:   target.Ref_Key IN (SELECT Ref_Key FROM temp)
          несколько:      (target.Recorder, target.Recorder_Type) IN (SELECT Recorder, Recorder_Type FROM temp)
        """
        if len(mapped_object_key) == 1:
            col = mapped_object_key[0]
            return table.c[col].in_(select(temp_table.c[col]))

        target_cols = [table.c[col] for col in mapped_object_key]
        temp_cols = [temp_table.c[col] for col in mapped_object_key]
        return tuple_(*target_cols).in_(select(*temp_cols))
