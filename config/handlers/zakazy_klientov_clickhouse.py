"""
Витрина строк регистра «Заказы клиентов» в ClickHouse: тот же расчёт, что в zakazy_klientov.py,
но результат записывается в ClickHouse.

Репликатор по-прежнему пишет в PostgreSQL, и представление с соединениями живёт там же. ClickHouse
читает его сам, через базу с движком PostgreSQL: данные идут с сервера на сервер, минуя Python.
Такую базу один раз создаёт администратор ClickHouse. Пароль PostgreSQL хранится в именованной
коллекции ClickHouse и в SHOW CREATE не виден; onecdc его не знает:

    CREATE NAMED COLLECTION onecdc_pg AS
        host = 'pg-host', port = 5432, database = 'dwh', schema = 'onecdc',
        user = 'clickhouse_reader', password = '...';
    CREATE DATABASE onecdc_pg ENGINE = PostgreSQL(onecdc_pg);

schema — схема репликатора (ONECDC_DB_SCHEMA). Пользователю нужно право SELECT на её таблицы
и представления. Пояс времени сессии PostgreSQL должен совпадать с поясом сервера ClickHouse:
timestamptz приходит в ClickHouse без пояса, и окно merged_on > last_run_at сравнивается в нём.

В остальном обработчик повторяет zakazy_klientov.py:

1) Источник — то же представление, только читается через базу onecdc_pg: source_table_name
   и source_schema. Условие окна то же: UNION ALL по merged_on каждого источника. ClickHouse не
   передаёт в PostgreSQL IN с подзапросом, и dbmerge выполняет подзапрос сам, а в PostgreSQL отдаёт
   готовый список ключей. Поэтому из PostgreSQL читаются только изменившиеся строки.

2) Подключение к ClickHouse передаётся в конструктор: HandlerLoop знает только целевую БД репликатора
   (context.engine). Адрес и пароль ClickHouse в этом файле не пишутся: engine создаёт runner.py
   из переменной окружения.

3) Витрина — ReplacingMergeTree. Изменённая строка записывается новой версией, а прежняя исчезает
   при фоновом слиянии кусков, поэтому читать витрину нужно с FINAL. Ключ merge dbmerge берёт из
   ORDER BY таблицы: по нему же ReplacingMergeTree схлопывает версии.
"""

from sqlalchemy import Engine, and_, select, text, tuple_, union_all
from dbmerge import dbmerge

from onecdc import Handler, HandlerContext


def postgres_ddl(context: HandlerContext) -> str:
    """Представление-источник и индексы под него в PostgreSQL — те же, что в zakazy_klientov.py."""
    schema = context.schema or 'public'
    return f"""
CREATE OR REPLACE VIEW "{schema}"."ZakazyKlientovClickHouse_view"
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

CREATE INDEX IF NOT EXISTS "ix_AccumulationRegister_ZakazyKlientov_Nomenklatura_Key" ON
	"{schema}"."AccumulationRegister_ZakazyKlientov" USING btree ("Nomenklatura_Key");

CREATE INDEX IF NOT EXISTS "ix_AccumulationRegister_ZakazyKlientov_Period" ON
	"{schema}"."AccumulationRegister_ZakazyKlientov" USING btree ("Period");
"""


def clickhouse_ddl(database: str) -> str:
    """
    Таблица витрины в ClickHouse.

    Количество в 1С — Число(15, 3). Не Nullable только ключ, merged_on и флаг: их репликатор заполняет
    всегда, и NULL здесь означал бы ошибку в источнике. dbmerge откажется его загружать, а не запишет
    1970-01-01 или false. Period бывает пуст: так выглядит запись о пустом наборе движений
    (README_DB.md, раздел про is_deleted_or_empty).
    """
    return f"""
CREATE TABLE IF NOT EXISTS `{database}`.`ZakazyKlientov` (
	Recorder UUID,
	Recorder_Type String,
	LineNumber Int64,
	Period Nullable(DateTime),
	Zakazano Nullable(Decimal(15, 3)),
	Artikul Nullable(String),
	merged_on DateTime64(6, 'UTC'),
	Nomenklatura_merged_on Nullable(DateTime64(6, 'UTC')),
	is_deleted_or_empty Bool
)
ENGINE = ReplacingMergeTree
ORDER BY (Recorder, Recorder_Type, LineNumber)
"""


def rebuild_blocks_sql(context: HandlerContext) -> str:
    """Месяцы с движениями регистра в хронологическом порядке, как в zakazy_klientov.py."""
    schema = context.schema or 'public'
    return f"""
SELECT DISTINCT to_char(date_trunc('month', "Period"), 'YYYY-MM') AS label,
       date_trunc('month', "Period")::date AS begins,
       (date_trunc('month', "Period") + interval '1 month')::date AS ends
  FROM "{schema}"."AccumulationRegister_ZakazyKlientov"
 WHERE "Period" IS NOT NULL
 ORDER BY label
"""


class ZakazyKlientovClickHouse(Handler):
    # Таблицы PostgreSQL, которые читает представление (см. zakazy_klientov.py).
    ON = ["AccumulationRegister_ZakazyKlientov", "Catalog_Nomenklatura"]

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
        """Пересобрать витрину по месяцам, как в zakazy_klientov.py: удалять здесь нечего."""
        for label, begin, end in self.query(context, rebuild_blocks_sql(context)):
            if context.rebuild_from and label <= context.rebuild_from:
                continue                      # этот месяц уже посчитан до перезапуска процесса

            with dbmerge(self.clickhouse, table_name="ZakazyKlientov", schema=self.database,
                         source_table_name="ZakazyKlientovClickHouse_view",
                         source_schema=self.source_database) as merge:
                period = merge.source_table.c["Period"]
                merge.exec(source_condition=and_(period >= begin, period < end))

            context.logger.info("Data Mart updated for %s", label)
            yield label

    def handle(self, context: HandlerContext) -> None:
        with dbmerge(self.clickhouse, table_name="ZakazyKlientov", schema=self.database,
                     source_table_name="ZakazyKlientovClickHouse_view",
                     source_schema=self.source_database) as merge:

            # Окно — как в zakazy_klientov.py: ветка UNION ALL на каждый источник. Каждая ветка
            # сравнивает колонку с константой, такое условие ClickHouse передаёт в PostgreSQL,
            # и тот идёт по индексу merged_on. Ключи из веток dbmerge подставляет в условие
            # списком, и по ним из PostgreSQL читаются только изменившиеся строки.
            changed = merge.source_table
            row_key = (changed.c["Recorder"], changed.c["Recorder_Type"], changed.c["LineNumber"])

            changed_by_register = select(*row_key).where(
                changed.c["merged_on"] > context.last_run_at)
            changed_by_nomenklatura = select(*row_key).where(
                changed.c["Nomenklatura_merged_on"] > context.last_run_at)

            merge.exec(source_condition=tuple_(*row_key).in_(
                union_all(changed_by_register, changed_by_nomenklatura)))

        context.logger.info(
            f'Data Mart changes applied since {context.last_run_at}; '
            f'Next run will apply since {context.boundary}')
