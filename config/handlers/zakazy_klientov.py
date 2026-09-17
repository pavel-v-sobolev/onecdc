"""
Витрина: строки регистра «Заказы клиентов» с артикулом из справочника номенклатуры.

Обработчик вызывают автоматически, когда в БД реально изменился регистр или номенклатура,
и передают окно (context.last_run_at, context.boundary] — выбирать данные обработчик должен
по нему.

Ключевая мысль всей конструкции: объекты 1С приезжают в обмене независимо, номенклатура может
доехать позже регистра. Поэтому вьюшка отдаёт merged_on КАЖДОГО источника отдельной колонкой, и в
пересчёт попадает строка, у которой стал свежее хоть один из них. Если ориентироваться только на
merged_on регистра, строка, дождавшаяся своей номенклатуры, в инкремент уже не попадёт и навсегда
останется с NULL-артикулом.
"""

from sqlalchemy import and_, select, tuple_, union_all
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
CREATE OR REPLACE VIEW "{schema}"."ZakazyKlientov_view"
AS
(
SELECT
	s."Recorder",
	s."Recorder_Type",
	s."LineNumber",
	s."Period",
	s."Zakazano" * (NOT s."is_deleted_or_empty")::int * --обнулим значение для удаленных строк
		(CASE WHEN s."RecordType"='Receipt' THEN 1 ELSE -1 END) --проверим тип операции + или -
    		"Zakazano",
	n."Code" "Artikul",
	s."merged_on",
	n."merged_on" "Nomenklatura_merged_on",
	s."is_deleted_or_empty"
FROM "{schema}"."AccumulationRegister_ZakazyKlientov" s
LEFT JOIN "{schema}"."Catalog_Nomenklatura" n ON n."Ref_Key"=s."Nomenklatura_Key"
);

CREATE TABLE IF NOT EXISTS "{schema}"."ZakazyKlientov" (
	"Recorder" uuid,
	"Recorder_Type" varchar,
	"LineNumber" int8,
	"Period" timestamp,
	"Zakazano" numeric,
	"Artikul" varchar,
	"merged_on" timestamp,
	"Nomenklatura_merged_on" timestamp,
	"is_deleted_or_empty" boolean,
	CONSTRAINT "ZakazyKlientov_pkey"
		PRIMARY KEY ("Recorder", "Recorder_Type", "LineNumber")
);

-- Индекс по merged_on нужен не этой витрине, а той, что может строиться поверх неё: её обработчик
-- будет спрашивать «что здесь изменилось». Индекса по Nomenklatura_merged_on нет намеренно: он
-- существовал ради SELECT max(...) из самой витрины, а границу теперь приносит context.
CREATE INDEX IF NOT EXISTS "ix_ZakazyKlientov_merged_on" ON
	"{schema}"."ZakazyKlientov" USING btree (merged_on);

-- Индекс по колонке соединения со справочником: без него ветка UNION ALL, которую ведёт свежая
-- номенклатура, дотягивается до регистра полным сканом. Репликатор такие индексы не создаёт — он
-- заводит только merged_on, а что с чем соединяется, знает витрина, а не он.
CREATE INDEX IF NOT EXISTS "ix_AccumulationRegister_ZakazyKlientov_Nomenklatura_Key" ON
	"{schema}"."AccumulationRegister_ZakazyKlientov" USING btree ("Nomenklatura_Key");

-- Индекс по периоду: по нему нарезана пересборка (см. rebuild ниже), блок = месяц.
CREATE INDEX IF NOT EXISTS "ix_AccumulationRegister_ZakazyKlientov_Period" ON
	"{schema}"."AccumulationRegister_ZakazyKlientov" USING btree ("Period");
"""


def rebuild_blocks_sql(context: HandlerContext) -> str:
    """
    Месяцы, за которые в регистре есть движения, по возрастанию. Порядок важен: метка последнего
    блока — это точка возобновления, и «всё, что меньше» должно быть уже сделано. Метка в формате
    YYYY-MM именно поэтому: она сравнивается как строка, и такой формат сортируется правильно сам.

    Арифметику календаря делает БД: прибавить месяц к дате в Python — либо лишняя зависимость,
    либо трюк вроде «28-е плюс четыре дня».
    """
    schema = context.schema or 'public'
    return f"""
SELECT DISTINCT to_char(date_trunc('month', "Period"), 'YYYY-MM') AS label,
       date_trunc('month', "Period")::date AS begins,
       (date_trunc('month', "Period") + interval '1 month')::date AS ends
  FROM "{schema}"."AccumulationRegister_ZakazyKlientov"
 WHERE "Period" IS NOT NULL
 ORDER BY label
"""


class ZakazyKlientov(Handler):
    # Имена ТАБЛИЦ в целевой БД (транслит), а не имена объектов 1С — те же, что стоят в SQL ниже:
    #   AccumulationRegister_ZakazyKlientov  ← РегистрНакопления.ЗаказыКлиентов
    #   Catalog_Nomenklatura                 ← Справочник.Номенклатура
    # Читаются обе, поэтому обе и перечислены: по этому же списку считается верхняя граница окна
    # (незавершённый merge любой из них прижимает границу к своему старту).
    ON = ["AccumulationRegister_ZakazyKlientov", "Catalog_Nomenklatura"]

    def setup(self, context: HandlerContext) -> None:
        # Один раз за процесс, а не на каждый вызов: полная выгрузка сигналит постранично, и
        # CREATE OR REPLACE VIEW на каждую страницу брал бы блокировки на пустом месте.
        self.execute(context, ddl(context))

    def rebuild(self, context: HandlerContext):
        """
        Пересборка ПО МЕСЯЦАМ. Между блоками цикл применяет накопившиеся изменения, поэтому витрина
        не стоит холодной все те десятки минут, что идёт пересборка (см. Handler.rebuild).

        Блок читает данные АКТУАЛЬНЫЕ на момент своего выполнения — никакого снимка на старте
        пересборки. Иначе блок затёр бы изменения, применённые между блоками.

        Удаления нет и здесь: ключ витрины совпадает с ключом строки регистра, поэтому строка,
        у которой поправили дату документа, просто обновится на месте — копии в старом месяце не
        остаётся. Витрине, где ключ другой, месяц пришлось бы заменять целиком: см. соседний
        zakazy_klientov_grouped.py.
        """
        for label, begin, end in self.query(context, rebuild_blocks_sql(context)):
            if context.rebuild_from and label <= context.rebuild_from:
                continue                      # этот месяц уже посчитан до перезапуска процесса

            with dbmerge(context.engine, table_name="ZakazyKlientov", schema=context.schema,
                         temp_schema=context.temp_schema,
                         source_table_name="ZakazyKlientov_view",
                         source_schema=context.schema) as merge:
                period = merge.source_table.c["Period"]
                merge.exec(source_condition=and_(period >= begin, period < end))

            context.logger.info("Data Mart updated for %s", label)
            yield label

    def handle(self, context: HandlerContext) -> None:
        # delete_mode не задан (по умолчанию 'no'): удалять из витрины нечего. Строки из целевых
        # таблиц физически не исчезают — выпавшую из набора движений репликатор помечает
        # is_deleted_or_empty и гасит ресурсы, а вьюшка её не фильтрует. Значит такая строка
        # приедет сюда как обычное изменение и обновит строку витрины нулём и флагом.
        with dbmerge(context.engine, table_name="ZakazyKlientov", schema=context.schema,
                     # Промежуточную таблицу merge кладём туда же, куда её кладёт репликатор
                     # (см. runner.py): не задана — схема данных.
                     temp_schema=context.temp_schema,
                     source_table_name="ZakazyKlientov_view",
                     source_schema=context.schema) as merge:

            # Ключ витрины совпадает с ключом строки регистра, и удаления нет, поэтому единица
            # пересчёта — СТРОКА: берём те, у которых стал свежее хоть один источник. (Там, где
            # ключ витрины другой — например, агрегат по GROUP BY, — так нельзя: см. соседний
            # zakazy_klientov_grouped.py, где пересчитывается группа целиком.)
            #
            # Вьюшка у веток одна и та же: логика соединений не дублируется, различаются они
            # только колонкой в WHERE. ALL — потому что дедупликация не нужна: результат уходит
            # в IN (...), где дубликаты не мешают.
            #
            # Подзапрос обращается к той же вьюшке, что и внешний запрос, и это НЕ делает его
            # коррелированным: у внутреннего SELECT своя область видимости, и имя вьюшки внутри
            # относится к нему. Алиас тут не нужен — он лишь дал бы второе имя в EXPLAIN.
            changed = merge.source_table
            row_key = (changed.c["Recorder"], changed.c["Recorder_Type"], changed.c["LineNumber"])

            # Нижняя граница окна берётся из контекста как есть: там всегда дата, и на первом
            # прогоне (или пересборке) это начало времён, а не None — проверять нечего.
            changed_by_register = select(*row_key).where(
                changed.c["merged_on"] > context.last_run_at)
            changed_by_nomenklatura = select(*row_key).where(
                changed.c["Nomenklatura_merged_on"] > context.last_run_at)

            changed_rows = union_all(changed_by_register, changed_by_nomenklatura)

            merge.exec(
                source_condition=tuple_(merge.source_table.c["Recorder"],
                                        merge.source_table.c["Recorder_Type"],
                                        merge.source_table.c["LineNumber"]).in_(changed_rows))

            # Почему changed_rows собран через UNION ALL, а не одним WHERE с OR (то есть не
            # через self.changed_since). Написать `merged_on > last_run_at OR
            # Nomenklatura_merged_on > last_run_at` — читается лучше, но на объёме это узкое
            # место, и дело НЕ в отсутствии
            # индексов. Ветки такого OR живут на разных таблицах соединения, поэтому ни по одной
            # из них нельзя отфильтровать до джойна: условие проверяется только на готовой паре и
            # вырождается в Join Filter поверх ПОЛНОГО соединения. BitmapOr из индексных сканов
            # Postgres собирает охотно, но только когда все ветки OR на одной таблице, а
            # преобразовывать OR в UNION через границу джойна он не умеет. Тип соединения тут ни
            # при чём: замена LEFT на INNER плана не меняет.
            #
            # В варианте с UNION ALL в каждой ветке остаётся предикат ровно по ОДНОЙ базовой
            # таблице. Такой предикат планировщик опускает внутрь вьюшки, в скан этой таблицы, и
            # ветка заходит через её индекс merged_on (их заводит сам репликатор, см.
            # DBWriter._ensure_merged_on_index). LEFT JOIN не мешает: `n.merged_on >
            # last_run_at` для несопоставленных строк даёт NULL, предикат null-rejecting, и
            # внешнее соединение в
            # этой ветке схлопывается во внутреннее.
            #
            # Замер на синтетике (регистр 2 млн строк, окно ~0.5%): OR — 620 мс с полным сканом
            # регистра, UNION ALL из той же вьюшки — 98 мс без единого seq scan. Переписывать
            # соединения руками ради этого не нужно: руками получилось 83 мс, разница в пределах
            # накладных расходов на вьюшку.
            #
            # Ещё два условия, без которых выигрыша не будет:
            #  - random_page_cost под SSD (1.1 вместо дефолтных 4): иначе планировщик предпочтёт
            #    seq scan регистра nested loop'у по индексу, и UNION ALL даст лишь ~2x вместо ~6x;
            #  - индекс по колонке соединения со справочником (Nomenklatura_Key) — репликатор
            #    заводит только merged_on, поэтому этот индекс создаётся в setup().

        context.logger.info(
            f'Data Mart changes applied since {context.last_run_at}; '
            f'Next run will apply since {context.boundary}')
