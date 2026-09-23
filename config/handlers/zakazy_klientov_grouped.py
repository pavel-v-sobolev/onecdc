"""
Витрина с ДРУГИМ ключом: агрегат по (Номер документа, Год, Артикул), а не по строкам регистра.
Второй пример: как та же схема работает, когда ключ витрины не совпадает с ключом источника.

Ключ группы здесь составной — (Number, Year). Это меняет две вещи: запрос «что пересобрать» отдаёт
две колонки, а фильтры собираются row-value сравнением `tuple_(a, b).in_(...)`.

Главное же отличие в том, что ключ ВЫЧИСЛЯЕТСЯ из данных: Number берётся из документа, Year — из его
даты. Значит, изменение в 1С может не изменить строку, а перенести её в другую группу: доехал
документ — строки ушли из группы-заглушки в реальную; поправили дату через границу года — ушли из
(N, 2025) в (N, 2026). Новую группу видно в источнике, а старой нет уже нигде, и удалить её нечем.

Поэтому витрина хранит колонки-массивы "*_Keys": GUID-ы объектов 1С, из которых собрана строка.
Обработчик берёт GUID-ы изменившихся объектов и получает по ним два списка групп — что эти объекты
давали раньше (по массивам витрины) и что дают сейчас (по построчному слою), — после чего
пересобирает оба. Покинутая группа при этом чистится сама.

Смена ключа требует двух слоёв вьюшек:
  1) _rows_view — построчный: все JOIN'ы описаны один раз, каждый источник отдаёт свой merged_on
     отдельной колонкой, ссылки на источники выходят наружу как есть;
  2) _view — агрегат поверх него, он и есть источник merge; здесь же ссылки сворачиваются в массивы.

Соединения — LEFT, потому что объекты 1С приезжают в обмене независимо: движения регистра могут
доехать раньше своего документа или номенклатуры. INNER JOIN спрятал бы такие строки до прибытия
документа; LEFT показывает их сразу, ценой NULL в ключе группы — отсюда COALESCE ниже.

Пересборка нарезана по месяцам (см. rebuild), и месяц витрина хранит своей колонкой "Period" —
иначе блоку пришлось бы отбирать группы, а так он просто заменяет месяц целиком. Месяц берётся из
даты ДОКУМЕНТА, из которой берётся и Year: тогда у группы он ровно один, и блок её не разрежет.
От периода движения он может отличаться, и в ключ группировки не входит — в агрегате берётся
MAX(), чтобы зерно группы осталось прежним.

Во время пересборки группа может переехать в другой месяц, чей блок давно позади: пересборка идёт
минутами, и дату документа успевают поправить. Убирает покинутую группу ИНКРЕМЕНТ, который цикл
выполняет между блоками, — по массивам "*_Keys" он находит, что изменившийся документ давал
раньше. Никакой доработки блокам для этого не понадобилось.
"""

from sqlalchemy import text, tuple_

from dbmerge import dbmerge

from onecdc import Handler, HandlerContext


def ddl(context: HandlerContext) -> str:
    """
    DDL витрины. Функция, а не константа: имя схемы приходит в контексте, поэтому подставляет его
    обычная f-строка — `"{schema}"."Таблица"` в тексте и есть подстановка, никакого своего
    механизма. Схема не задана — работаем в схеме по умолчанию.
    """
    schema = context.schema or 'public'
    return f"""
CREATE OR REPLACE VIEW "{schema}"."ZakazyKlientovGrouped_rows_view"
AS
(
SELECT
	COALESCE(z."Number",'') "Number",
	COALESCE(EXTRACT(YEAR FROM z."Date")::int, 0) "Year",
	COALESCE(n."Code",'') "Artikul",
	s."Zakazano" * (NOT s."is_deleted_or_empty")::int *  --обнулим значение для удаленных строк
		(CASE WHEN s."RecordType"='Receipt' THEN 1 ELSE -1 END) --проверим тип операции + или -
			"Zakazano",
	s."merged_on",
	z."merged_on" "ZakazKlienta_merged_on",
	n."merged_on" "Nomenklatura_merged_on",
	--Месяц, по которому нарезана пересборка. Берётся из даты ДОКУМЕНТА, а не из периода движения:
	--Year в ключе группы берётся оттуда же, поэтому у группы месяц ровно один. Периодов движения у
	--одной группы может быть несколько, и блок разрезал бы её пополам.
	--Дата не доехала — та же заглушка, что у Number и Year: своя группа, свой блок.
	COALESCE(date_trunc('month', z."Date")::date, DATE '0001-01-01') "Period",
	--Ссылки на источники: ниже они сворачиваются в массивы, по которым ищутся покинутые группы.
	--Регистратор и документ — один и тот же GUID, поэтому колонка одна на оба источника.
	s."Recorder",
	s."Nomenklatura_Key"
FROM "{schema}"."AccumulationRegister_ZakazyKlientov" s
LEFT JOIN "{schema}"."Document_ZakazKlienta" z ON z."Ref_Key" = s."Recorder" AND
	s."Recorder_Type"='Document_ЗаказКлиента'
LEFT JOIN "{schema}"."Catalog_Nomenklatura" n ON n."Ref_Key"=s."Nomenklatura_Key"
);

CREATE OR REPLACE VIEW "{schema}"."ZakazyKlientovGrouped_view"
AS
(
SELECT
	"Number",
	"Year",
	"Artikul",
	SUM("Zakazano") "Zakazano",
	--MAX, а не GROUP BY: зерно группы не меняется. Попади "Period" в ключ группировки, группа с
	--двумя месяцами разъехалась бы на две строки с одинаковым первичным ключом.
	MAX("Period") "Period",
	MAX("merged_on") "merged_on",
	--Массивы собираются по всей группе, а не по одной строке: если строку удалят, группа должна
	--достаться остальным. array_agg(DISTINCT ...) сортирует, поэтому массив стабилен и не даёт
	--ложных UPDATE. FILTER отсекает NULL, COALESCE подставляет пустой массив.
	COALESCE(array_agg(DISTINCT "Recorder") FILTER (WHERE "Recorder" IS NOT NULL),
		ARRAY[]::uuid[]) "Recorder_Keys",
	COALESCE(array_agg(DISTINCT "Nomenklatura_Key") FILTER (WHERE "Nomenklatura_Key" IS NOT NULL),
		ARRAY[]::uuid[]) "Nomenklatura_Keys"
FROM "{schema}"."ZakazyKlientovGrouped_rows_view"
GROUP BY "Number","Year","Artikul"
);

CREATE TABLE IF NOT EXISTS "{schema}"."ZakazyKlientovGrouped" (
	"Number" varchar,
	"Year" int4,
	"Artikul" varchar,
	"Zakazano" numeric,
	"Period" date,
	"merged_on" timestamptz,
	"Recorder_Keys" uuid[],
	"Nomenklatura_Keys" uuid[],
	CONSTRAINT "ZakazyKlientovGrouped_pkey"
		PRIMARY KEY ("Number", "Year", "Artikul")
);

CREATE INDEX IF NOT EXISTS "ix_ZakazyKlientovGrouped_merged_on" ON
	"{schema}"."ZakazyKlientovGrouped" USING btree (merged_on);

-- GIN на каждый массив: под запрос «какие группы породил вот этот набор GUID-ов» (оператор &&).
-- Btree тут не работает: ищем не по значению массива, а по вхождению элемента.
CREATE INDEX IF NOT EXISTS "ix_ZakazyKlientovGrouped_Recorder_Keys" ON
	"{schema}"."ZakazyKlientovGrouped" USING gin ("Recorder_Keys");

CREATE INDEX IF NOT EXISTS "ix_ZakazyKlientovGrouped_Nomenklatura_Keys" ON
	"{schema}"."ZakazyKlientovGrouped" USING gin ("Nomenklatura_Keys");

-- Индекс под source_condition: агрегатная вьюшка фильтруется по ключу группы, а Number приходит
-- из документа.
CREATE INDEX IF NOT EXISTS "ix_Document_ZakazKlienta_Number" ON
	"{schema}"."Document_ZakazKlienta" USING btree ("Number");

-- Индекс по колонке соединения со справочником. Репликатор такие индексы не создаёт — он заводит
-- только merged_on, а что с чем соединяется, знает витрина, а не он.
CREATE INDEX IF NOT EXISTS "ix_AccumulationRegister_ZakazyKlientov_Nomenklatura_Key" ON
	"{schema}"."AccumulationRegister_ZakazyKlientov" USING btree ("Nomenklatura_Key");

-- Индекс по месяцу: по нему нарезана пересборка (см. rebuild ниже), блок = месяц.
CREATE INDEX IF NOT EXISTS "ix_ZakazyKlientovGrouped_Period" ON
	"{schema}"."ZakazyKlientovGrouped" USING btree ("Period");
"""


def groups_to_handle_sql(context: HandlerContext) -> str:
    """
    Группы, которые надо пересчитать за окно (:last_run_at — конец прошлого прогона):
    PREVIOUS — что изменившиеся объекты давали раньше, CURRENT — что дают сейчас.

    Две тонкости, из-за которых половины записаны по-разному:
      - в PREVIOUS оба условия на одной таблице, там OR работает через индексы; в CURRENT они на
        разных таблицах соединения, и OR пришлось бы проверять уже после JOIN'а — поэтому UNION ALL;
      - набор изменившегося подставляется через ARRAY(SELECT ...), а не IN (SELECT ...): под OR
        IN-подзапрос не позволяет планировщику использовать индексы.

    Окно приходит параметром `:last_run_at` — его связывает вызывающий (см. handle).
    """
    schema = context.schema or 'public'
    return f"""
WITH changed_recorders AS (
    SELECT DISTINCT "Recorder" AS id
      FROM "{schema}"."AccumulationRegister_ZakazyKlientov"
     WHERE "merged_on" > :last_run_at
    UNION
    SELECT "Ref_Key" FROM "{schema}"."Document_ZakazKlienta"
     WHERE "merged_on" > :last_run_at
),
changed_nomenklatura AS (
    SELECT "Ref_Key" AS id FROM "{schema}"."Catalog_Nomenklatura"
     WHERE "merged_on" > :last_run_at
)

-- PREVIOUS: группы, которые изменившиеся объекты давали раньше
SELECT "Number", "Year" FROM "{schema}"."ZakazyKlientovGrouped"
 WHERE "Recorder_Keys" && ARRAY(SELECT id FROM changed_recorders)
    OR "Nomenklatura_Keys" && ARRAY(SELECT id FROM changed_nomenklatura)

UNION ALL

-- CURRENT: группы, которые они дают сейчас
SELECT "Number", "Year" FROM "{schema}"."ZakazyKlientovGrouped_rows_view"
 WHERE "merged_on" > :last_run_at
UNION ALL
SELECT "Number", "Year" FROM "{schema}"."ZakazyKlientovGrouped_rows_view"
 WHERE "ZakazKlienta_merged_on" > :last_run_at
UNION ALL
SELECT "Number", "Year" FROM "{schema}"."ZakazyKlientovGrouped_rows_view"
 WHERE "Nomenklatura_merged_on" > :last_run_at
"""


def rebuild_blocks_sql(context: HandlerContext) -> str:
    """
    Месяцы, которые надо пересобрать. Объединение двух источников не для красоты: месяц, которого
    в источнике больше нет, а в витрине он есть, иначе не был бы очищен НИКОГДА — блок по нему
    просто не запустился бы.

    Метка YYYY-MM сравнивается как строка, и такой формат сортируется правильно сам по себе.
    """
    schema = context.schema or 'public'
    return f"""
SELECT DISTINCT "Period" FROM "{schema}"."ZakazyKlientovGrouped_rows_view"
UNION
SELECT DISTINCT "Period" FROM "{schema}"."ZakazyKlientovGrouped"
ORDER BY 1
"""


class ZakazyKlientovGrouped(Handler):
    # Имена ТАБЛИЦ в целевой БД (транслит), а не имена объектов 1С — те же, что стоят в SQL выше:
    #   AccumulationRegister_ZakazyKlientov  ← РегистрНакопления.ЗаказыКлиентов
    #   Document_ZakazKlienta                ← Документ.ЗаказКлиента
    #   Catalog_Nomenklatura                 ← Справочник.Номенклатура
    # Читаются все три, поэтому все три и перечислены: по этому же списку считается верхняя
    # граница окна.
    ON = ["AccumulationRegister_ZakazyKlientov", "Document_ZakazKlienta", "Catalog_Nomenklatura"]

    def setup(self, context: HandlerContext) -> None:
        self.execute(context, ddl(context))

    def rebuild(self, context: HandlerContext):
        """
        Пересборка ПО МЕСЯЦАМ. Между блоками цикл применяет накопившиеся изменения, поэтому витрина
        не стоит холодной все те десятки минут, что идёт пересборка (см. Handler.rebuild).

        Блок берёт из источника весь месяц и весь месяц в витрине и заменяет. Отбирать группы, как
        это делает инкремент, не нужно: инкремент видит лишь часть групп месяца, а блок — все.
        Заодно это само чинит переезд группы между месяцами (поправили дату документа): из старого
        месяца её удалит его блок, в новом создаст его собственный.
        """
        for (period,) in self.query(context, rebuild_blocks_sql(context)):
            label = period.strftime('%Y-%m')
            if context.rebuild_from and label <= context.rebuild_from:
                continue                  # этот месяц уже посчитан до перезапуска процесса

            with dbmerge(context.engine, table_name="ZakazyKlientovGrouped", schema=context.schema,
                         source_table_name="ZakazyKlientovGrouped_view",
                         source_schema=context.schema, delete_mode='delete') as merge:
                merge.exec(source_condition=merge.source_table.c["Period"] == period,
                           delete_condition=merge.table.c["Period"] == period)

            context.logger.info("Data Mart updated for %s", label)
            yield label

    def handle(self, context: HandlerContext) -> None:
        # Выборка групп уходит в запрос dbmerge дважды, поэтому её не выполняют, а готовят:
        # text() с окном, связанным как обычный параметр.
        groups_to_handle = text(groups_to_handle_sql(context)).bindparams(
            last_run_at=context.last_run_at)

        with dbmerge(context.engine, table_name="ZakazyKlientovGrouped", schema=context.schema,
                     # Промежуточную таблицу merge кладём туда же, куда её кладёт репликатор
                     # (см. runner.py): не задана — схема данных.
                     source_table_name="ZakazyKlientovGrouped_view", source_schema=context.schema,
                     delete_mode='delete') as merge:
            # Ключ группы составной, поэтому сравнение row-value: (Number, Year) IN (SELECT
            # Number, Year ...). В остальном ничего не меняется по сравнению с одиночным ключом.
            source_key = tuple_(merge.source_table.c["Number"], merge.source_table.c["Year"])
            target_key = tuple_(merge.table.c["Number"], merge.table.c["Year"])

            # Берём из вьюшки все строки этих групп и удаляем в витрине тоже по ним, а не по
            # содержимому staging-таблицы. Группы, из которой всё уехало, в staging нет — по нему
            # её было бы не удалить. Удаление по ключу ГРУППЫ (а не по PK) заодно убирает те
            # Artikul, что из группы выпали, в том числе при переименовании Code.
            merge.exec(source_condition=source_key.in_(groups_to_handle),
                       delete_condition=target_key.in_(groups_to_handle))

        context.logger.info(
            f'Data Mart changes applied since {context.last_run_at}; '
            f'Next run will apply since {context.boundary}')
