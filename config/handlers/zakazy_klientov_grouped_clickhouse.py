"""
Витрина-агрегат по (Номер документа, Год, Артикул) в ClickHouse: пересчёт групп из
zakazy_klientov_grouped.py с записью в ClickHouse. Почему пересчитывается группа целиком, зачем
два слоя представлений и откуда заглушки в ключе группы, объяснено там. Как ClickHouse читает
представления PostgreSQL и откуда берётся база onecdc_pg — в zakazy_klientov_clickhouse.py.
Здесь описано то, что ClickHouse меняет в пересчёте групп.

1) Группы для пересчёта находит один запрос в ClickHouse. Текущие группы изменившихся строк
   (CURRENT) он берёт из построчного представления PostgreSQL, прежние группы (PREVIOUS) — из самой
   витрины: в её массивах Recorder_Keys ищутся документы, изменившиеся за окно.

2) Массив Nomenklatura_Keys не нужен. Ключ группы (Number, Year) берётся только из документа:
   изменение номенклатуры меняет Artikul внутри той же группы, и эту группу находит CURRENT.
   Прежняя группа нужна, только когда меняется сам ключ группы, а он меняется вместе с документом.
   Поэтому и в PREVIOUS ищутся только документы, а не регистраторы изменившихся движений.

3) Заглушка месяца для документа, который ещё не поступил, — 1900-01-01, а не 0001-01-01: тип Date32
   в ClickHouse начинается с 1900 года.
"""

from sqlalchemy import Engine, text, tuple_

from dbmerge import dbmerge

from onecdc import Handler, HandlerContext


def postgres_ddl(context: HandlerContext) -> str:
    """Построчное и агрегатное представления и индексы под них в PostgreSQL."""
    schema = context.schema or 'public'
    return f"""
CREATE OR REPLACE VIEW "{schema}"."ZakazyKlientovGroupedClickHouse_rows_view"
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
	COALESCE(date_trunc('month', z."Date")::date, DATE '1900-01-01') "Period",
	s."Recorder"
FROM "{schema}"."AccumulationRegister_ZakazyKlientov" s
LEFT JOIN "{schema}"."Document_ZakazKlienta" z ON z."Ref_Key" = s."Recorder" AND
	s."Recorder_Type"='Document_ЗаказКлиента'
LEFT JOIN "{schema}"."Catalog_Nomenklatura" n ON n."Ref_Key"=s."Nomenklatura_Key"
);

CREATE OR REPLACE VIEW "{schema}"."ZakazyKlientovGroupedClickHouse_view"
AS
(
SELECT
	"Number",
	"Year",
	"Artikul",
	SUM("Zakazano") "Zakazano",
	MAX("Period") "Period",
	MAX("merged_on") "merged_on",
	COALESCE(array_agg(DISTINCT "Recorder") FILTER (WHERE "Recorder" IS NOT NULL),
		ARRAY[]::uuid[]) "Recorder_Keys"
FROM "{schema}"."ZakazyKlientovGroupedClickHouse_rows_view"
GROUP BY "Number","Year","Artikul"
);

CREATE INDEX IF NOT EXISTS "ix_Document_ZakazKlienta_Number" ON
	"{schema}"."Document_ZakazKlienta" USING btree ("Number");

CREATE INDEX IF NOT EXISTS "ix_AccumulationRegister_ZakazyKlientov_Nomenklatura_Key" ON
	"{schema}"."AccumulationRegister_ZakazyKlientov" USING btree ("Nomenklatura_Key");
"""


def clickhouse_ddl(database: str) -> str:
    """Таблица витрины в ClickHouse."""
    return f"""
CREATE TABLE IF NOT EXISTS `{database}`.`ZakazyKlientovGrouped` (
	Number String,
	Year Int32,
	Artikul String,
	Zakazano Nullable(Decimal(18, 3)),
	Period Date32,
	merged_on DateTime64(6, 'UTC'),
	Recorder_Keys Array(UUID),
	-- Под поиск прежних групп (hasAny), как GIN в zakazy_klientov_grouped.py. Индекс пропуска
	-- данных не находит строки, а отбрасывает гранулы, в которых искомых GUID-ов заведомо нет.
	INDEX ix_Recorder_Keys Recorder_Keys TYPE bloom_filter GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (Number, Year, Artikul)
"""


def groups_to_handle_sql(database: str, source_database: str) -> str:
    """
    Группы, которые надо пересчитать за окно (:last_run_at — конец прошлого прогона), запросом
    в ClickHouse: PREVIOUS — что изменившиеся документы давали раньше, CURRENT — что дают сейчас.

    Каждая ветка, которая читает PostgreSQL, сравнивает merged_on с константой: такое условие
    ClickHouse передаёт в PostgreSQL, и тот идёт по индексу merged_on. PREVIOUS читает витрину без
    FINAL: старая версия строки может добавить лишнюю группу, но она просто пересчитается заново.
    """
    return f"""
SELECT Number, Year FROM `{database}`.`ZakazyKlientovGrouped`
 WHERE hasAny(Recorder_Keys, (SELECT groupArray(Ref_Key)
                                FROM `{source_database}`.`Document_ZakazKlienta`
                               WHERE merged_on > :last_run_at))
UNION ALL
SELECT Number, Year FROM `{source_database}`.`ZakazyKlientovGroupedClickHouse_rows_view`
 WHERE merged_on > :last_run_at
UNION ALL
SELECT Number, Year FROM `{source_database}`.`ZakazyKlientovGroupedClickHouse_rows_view`
 WHERE ZakazKlienta_merged_on > :last_run_at
UNION ALL
SELECT Number, Year FROM `{source_database}`.`ZakazyKlientovGroupedClickHouse_rows_view`
 WHERE Nomenklatura_merged_on > :last_run_at
"""


def postgres_months_sql(context: HandlerContext) -> str:
    schema = context.schema or 'public'
    return f'SELECT DISTINCT "Period" FROM "{schema}"."ZakazyKlientovGroupedClickHouse_rows_view"'


def clickhouse_months_sql(database: str) -> str:
    return f"SELECT DISTINCT Period FROM `{database}`.`ZakazyKlientovGrouped`"


class ZakazyKlientovGroupedClickHouse(Handler):
    # Таблицы PostgreSQL, которые читают представления (см. zakazy_klientov_grouped.py).
    ON = ["AccumulationRegister_ZakazyKlientov", "Document_ZakazKlienta", "Catalog_Nomenklatura"]

    def __init__(self, clickhouse: Engine, source_database: str, database: str | None = None,
                 name: str | None = None):
        """
        clickhouse — engine clickhousedb:// (драйвер clickhouse-connect) из runner.py.
        source_database — база ClickHouse с движком PostgreSQL, которая смотрит на схему репликатора.
        database — база витрины в ClickHouse. Не задана — база из адреса подключения, а если
        её нет и там — default.
        """
        super().__init__(name)
        self.clickhouse = clickhouse
        self.source_database = source_database
        self.database = database or clickhouse.url.database or 'default'

    def setup(self, context: HandlerContext) -> None:
        self.execute(context, postgres_ddl(context))
        with self.clickhouse.begin() as conn:
            conn.execute(text(clickhouse_ddl(self.database)))

    def rebuild(self, context: HandlerContext):
        """
        Пересобрать витрину по месяцам, заменяя месяц целиком.

        Месяцы берутся и из источника, и из витрины: месяц, который из источника пропал, остался
        в витрине, и его блок должен её очистить. Источник и витрина на разных серверах, поэтому
        списки объединяются в Python.
        """
        months = {period for (period,) in self.query(context, postgres_months_sql(context))}
        with self.clickhouse.connect() as conn:
            months |= {period for (period,) in
                       conn.execute(text(clickhouse_months_sql(self.database)))}

        for period in sorted(months):
            label = period.strftime('%Y-%m')
            if context.rebuild_from and label <= context.rebuild_from:
                continue                  # этот месяц уже посчитан до перезапуска процесса

            with dbmerge(self.clickhouse, table_name="ZakazyKlientovGrouped", schema=self.database,
                         source_table_name="ZakazyKlientovGroupedClickHouse_view",
                         source_schema=self.source_database, delete_mode='delete') as merge:
                merge.exec(source_condition=merge.source_table.c["Period"] == period,
                           delete_condition=merge.table.c["Period"] == period)

            context.logger.info("Data Mart updated for %s", label)
            yield label

    def handle(self, context: HandlerContext) -> None:
        # Выборка групп уходит в merge дважды, поэтому её не выполняют, а готовят: text() с окном,
        # связанным как обычный параметр, — как в zakazy_klientov_grouped.py.
        groups_to_handle = text(groups_to_handle_sql(self.database, self.source_database)).bindparams(
            last_run_at=context.last_run_at)

        with dbmerge(self.clickhouse, table_name="ZakazyKlientovGrouped", schema=self.database,
                     source_table_name="ZakazyKlientovGroupedClickHouse_view",
                     source_schema=self.source_database, delete_mode='delete') as merge:
            source_key = tuple_(merge.source_table.c["Number"], merge.source_table.c["Year"])
            target_key = tuple_(merge.table.c["Number"], merge.table.c["Year"])

            # Группы dbmerge выбирает сам и в PostgreSQL отдаёт их списком: IN с подзапросом
            # ClickHouse туда не передаёт. Удаление идёт в ClickHouse по тем же группам.
            merge.exec(source_condition=source_key.in_(groups_to_handle),
                       delete_condition=target_key.in_(groups_to_handle))

        context.logger.info(
            f'Data Mart changes applied since {context.last_run_at}; '
            f'Next run will apply since {context.boundary}')
