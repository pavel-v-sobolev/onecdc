"""
Переезд служебных отметок на timestamptz и миграция действующих баз.

Наивная отметка — это настенное время пояса сессии, а оно немонотонно: в поясе с сезонным
переводом осенние часы отступают назад (проверено ниже на самом PostgreSQL), и час инкремента
теряется молча — merged_on строк этого часа оказывается ниже last_run_at (CDC-25). Значение в
повторяющийся час двусмысленно ПО СУЩЕСТВУ, поэтому чинит это только тип.

Миграция обязана быть идемпотентной и безопасной для чужих колонок: даты, приехавшие из 1С,
остаются наивными — в 1С поясов нет, это бизнес-значения, а не моменты.
"""

import uuid

import pytest
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, inspect, text

from onecdc.db_logs import (align_merge_timestamps, align_timestamp_columns,
                            create_table_if_absent)


def _column_types(db, table_name: str) -> dict:
    return {c['name']: c['type'] for c in inspect(db.engine).get_columns(table_name,
                                                                        schema=db.schema)}


def _has_timezone(db, table_name: str, column: str) -> bool:
    return bool(_column_types(db, table_name)[column].timezone)


def _create_naive_table(db, table_name: str, columns=('merged_on', 'inserted_on', 'Период')):
    cols = ', '.join(f'"{c}" timestamp' for c in columns)
    with db.engine.begin() as conn:
        conn.execute(text(f'CREATE TABLE "{db.schema}"."{table_name}" (id int, {cols})'))


# --- почему вообще переезжаем ------------------------------------------------------------------

def test_a_naive_clock_goes_backwards_in_a_zone_with_dst(db):
    """
    Основание всей работы, а не теория: один и тот же пояс, одна сессия, реальное время растёт —
    а настенное отступает. Именно его мы и хранили в служебных отметках.
    """
    with db.engine.connect() as conn:
        conn.execute(text("SET TIME ZONE 'Europe/Berlin'"))
        earlier, later = conn.execute(text("""
            SELECT (timestamptz '2026-10-25 00:59:00+00')::timestamp,
                   (timestamptz '2026-10-25 01:05:00+00')::timestamp""")).one()
        # Шесть минут реального времени спустя часы показывают на 54 минуты РАНЬШЕ.
        assert later < earlier
        # С поясом того же не происходит: это момент, а не показание часов.
        assert conn.execute(text("""
            SELECT timestamptz '2026-10-25 01:05:00+00' > timestamptz '2026-10-25 00:59:00+00'
            """)).scalar()


# --- миграция таблиц данных ---------------------------------------------------------------------

def test_merge_stamps_are_migrated_and_1c_dates_are_left_alone(db):
    """
    Переводим ТОЛЬКО свои колонки. «Период» приехал из 1С: поясов там нет, и истолковать эту дату
    в поясе сервера значило бы поменять её смысл и поставить его в зависимость от настройки БД.
    """
    _create_naive_table(db, 'Document_Zakaz')

    migrated = align_merge_timestamps(db.engine, db.schema)

    assert migrated == {'Document_Zakaz': ['inserted_on', 'merged_on']}
    assert _has_timezone(db, 'Document_Zakaz', 'merged_on')
    assert _has_timezone(db, 'Document_Zakaz', 'inserted_on')
    assert not _has_timezone(db, 'Document_Zakaz', 'Период'), 'дата 1С должна остаться наивной'


def test_the_value_keeps_its_meaning(db):
    """
    Миграция без USING: PostgreSQL истолковывает старое наивное значение в поясе сессии — ровно
    так, как оно записывалось. Момент остаётся тем же, разрыва в данных нет.
    """
    _create_naive_table(db, 'T', columns=('merged_on',))
    with db.engine.begin() as conn:
        conn.execute(text(f'INSERT INTO "{db.schema}"."T" VALUES (1, now())'))
        before = conn.execute(text(f'SELECT merged_on FROM "{db.schema}"."T"')).scalar()

    align_merge_timestamps(db.engine, db.schema)

    with db.engine.connect() as conn:
        after = conn.execute(text(f'SELECT merged_on FROM "{db.schema}"."T"')).scalar()
    assert after.tzinfo is not None
    # То же настенное время в поясе сессии — значит тот же момент.
    assert after.replace(tzinfo=None) == before


def test_running_it_again_changes_nothing(db):
    """
    Идемпотентность: процессов может стартовать несколько, и каждый делает свой обход.

    Кэш процесса здесь сбрасываем намеренно — иначе проверялся бы он, а не сама миграция.
    """
    from onecdc.db_logs import _MERGE_TIMESTAMPS_CHECKED

    _create_naive_table(db, 'T', columns=('merged_on',))

    assert align_merge_timestamps(db.engine, db.schema) == {'T': ['merged_on']}
    _MERGE_TIMESTAMPS_CHECKED.clear()                       # как будто стартовал второй процесс
    assert align_merge_timestamps(db.engine, db.schema) == {}, 'второй проход ничего не находит'
    assert _has_timezone(db, 'T', 'merged_on')


def test_the_scan_is_done_once_per_process(db):
    """
    Компонентов, зовущих обход на старте, несколько (репликатор, цикл обработчиков), а платить
    за него каждым незачем: новых наивных таблиц за время работы процесса не появится — их
    заводят сразу с поясом.
    """
    _create_naive_table(db, 'first', columns=('merged_on',))
    assert align_merge_timestamps(db.engine, db.schema) == {'first': ['merged_on']}

    _create_naive_table(db, 'second', columns=('merged_on',))
    assert align_merge_timestamps(db.engine, db.schema) == {}, 'второй обход в том же процессе'
    assert not _has_timezone(db, 'second', 'merged_on')


def test_both_columns_of_a_table_are_migrated_in_one_statement(db, caplog):
    """
    Под не-UTC такой ALTER переписывает таблицу целиком, поэтому две колонки — один оператор,
    а не два прохода по данным.
    """
    import logging

    _create_naive_table(db, 'T')
    with caplog.at_level(logging.INFO, logger='onecdc.db_logs'):
        align_merge_timestamps(db.engine, db.schema)

    migrations = [r for r in caplog.records if 'to timestamptz' in r.getMessage()]
    assert len(migrations) == 1
    assert 'inserted_on, merged_on' in migrations[0].getMessage()


# --- служебные таблицы: сверка с объявлением ----------------------------------------------------

def test_a_service_table_created_by_an_older_version_is_migrated(db):
    """
    Таблица, заведённая прошлой версией библиотеки, сама себя не починит: create(checkfirst=True)
    существующую не трогает. Поэтому тип сверяется там же, где сверяется состав колонок.
    """
    name = f'onecdc_probe_{uuid.uuid4().hex[:8]}'
    naive = Table(name, MetaData(), Column('id', Integer), Column('started_at', DateTime),
                  Column('note', String), schema=db.schema)
    naive.create(db.engine)

    aware = Table(name, MetaData(), Column('id', Integer),
                  Column('started_at', DateTime(timezone=True)),
                  Column('note', String), schema=db.schema)
    create_table_if_absent(db.engine, aware)

    assert _has_timezone(db, name, 'started_at')


def test_a_column_declared_without_a_zone_is_left_naive(db):
    """Сверяем с ОБЪЯВЛЕНИЕМ, а не переводим всё подряд: наивные колонки бывают осознанными."""
    name = f'onecdc_probe_{uuid.uuid4().hex[:8]}'
    table = Table(name, MetaData(), Column('id', Integer), Column('naive_at', DateTime),
                  schema=db.schema)
    table.create(db.engine)

    assert align_timestamp_columns(db.engine, table) == set()
    assert not _has_timezone(db, name, 'naive_at')


# --- зависимые вьюшки ---------------------------------------------------------------------------

def test_a_dependent_view_is_rebuilt_around_the_alter(db):
    """
    PostgreSQL не даёт менять тип колонки, на которую смотрит вьюшка: «cannot alter type of a
    column used by a view or rule». А витрины в нашей же документации строятся именно так — значит
    миграция обязана снимать вьюшки и ставить обратно, иначе она падает ровно там, где продуктом
    пользуются полностью.
    """
    _create_naive_table(db, 'T', columns=('merged_on',))
    with db.engine.begin() as conn:
        conn.execute(text(f'CREATE VIEW "{db.schema}"."T_view" AS '
                          f'SELECT id, merged_on FROM "{db.schema}"."T"'))

    migrated = align_merge_timestamps(db.engine, db.schema)

    assert migrated == {'T': ['merged_on']}, 'сама вьюшка в список миграции не попадает'
    assert _has_timezone(db, 'T', 'merged_on')
    assert _has_timezone(db, 'T_view', 'merged_on'), 'вьюшка должна поехать за таблицей'


def test_a_view_over_a_view_is_rebuilt_in_dependency_order(db):
    """Второй пример витрины именно такой: построчный слой и агрегат поверх него."""
    _create_naive_table(db, 'T', columns=('merged_on',))
    with db.engine.begin() as conn:
        conn.execute(text(f'CREATE VIEW "{db.schema}"."rows_view" AS '
                          f'SELECT id, merged_on FROM "{db.schema}"."T"'))
        conn.execute(text(f'CREATE VIEW "{db.schema}"."agg_view" AS '
                          f'SELECT id, max(merged_on) merged_on FROM "{db.schema}"."rows_view" '
                          f'GROUP BY id'))

    align_merge_timestamps(db.engine, db.schema)

    assert _has_timezone(db, 'rows_view', 'merged_on')
    assert _has_timezone(db, 'agg_view', 'merged_on')


def test_the_view_keeps_its_grants_and_comment(db):
    """
    DROP/CREATE теряет не только текст вьюшки. Права на неё — это доступ BI к витрине: молча
    потерять их значит сломать чтение у тех, кто про миграцию и не знал.
    """
    role = f'onecdc_reader_{uuid.uuid4().hex[:8]}'
    view = f'"{db.schema}"."T_view"'
    _create_naive_table(db, 'T', columns=('merged_on',))
    with db.engine.begin() as conn:
        conn.execute(text(f'CREATE ROLE "{role}"'))
        conn.execute(text(f'CREATE VIEW {view} AS SELECT id, merged_on FROM "{db.schema}"."T"'))
        conn.execute(text(f'GRANT SELECT ON {view} TO "{role}"'))
        conn.execute(text(f"COMMENT ON VIEW {view} IS 'витрина заказов'"))
    try:
        align_merge_timestamps(db.engine, db.schema)

        with db.engine.connect() as conn:
            assert conn.execute(text(
                f"SELECT has_table_privilege('{role}', '{view}', 'SELECT')")).scalar(), \
                'право SELECT потеряно'
            assert conn.execute(text(
                f"SELECT obj_description('{view}'::regclass, 'pg_class')")).scalar() \
                == 'витрина заказов'
    finally:
        with db.engine.begin() as conn:
            conn.execute(text(f'DROP OWNED BY "{role}"'))
            conn.execute(text(f'DROP ROLE "{role}"'))


def test_a_materialized_view_blocks_only_its_own_table(db, caplog):
    """
    Матпредставление пересоздать дёшево нельзя: это перезаполнение и потеря собственных индексов.
    Такую таблицу не трогаем и говорим об этом прямо — но остальные таблицы схемы мигрируют.
    """
    import logging

    _create_naive_table(db, 'blocked', columns=('merged_on',))
    _create_naive_table(db, 'free', columns=('merged_on',))
    with db.engine.begin() as conn:
        conn.execute(text(f'CREATE MATERIALIZED VIEW "{db.schema}"."mv" AS '
                          f'SELECT id, merged_on FROM "{db.schema}"."blocked"'))

    with caplog.at_level(logging.ERROR, logger='onecdc.db_logs'):
        migrated = align_merge_timestamps(db.engine, db.schema)

    assert migrated == {'free': ['merged_on']}
    assert not _has_timezone(db, 'blocked', 'merged_on')
    assert 'materialized view' in '\n'.join(r.getMessage() for r in caplog.records)


def test_a_failure_leaves_the_table_and_its_views_intact(db, monkeypatch):
    """
    DDL в PostgreSQL транзакционный, и вся операция идёт одной транзакцией: сбой на пересоздании
    вьюшки не имеет права оставить схему без неё.
    """
    from onecdc import db_logs

    _create_naive_table(db, 'T', columns=('merged_on',))
    with db.engine.begin() as conn:
        conn.execute(text(f'CREATE VIEW "{db.schema}"."T_view" AS '
                          f'SELECT id, merged_on FROM "{db.schema}"."T"'))

    def boom(*args, **kwargs):
        raise RuntimeError('не смогли поднять вьюшку')
    monkeypatch.setattr(db_logs, '_restore_views', boom)

    with pytest.raises(RuntimeError):
        align_merge_timestamps(db.engine, db.schema)

    assert not _has_timezone(db, 'T', 'merged_on'), 'тип должен был откатиться'
    assert 'T_view' in inspect(db.engine).get_view_names(schema=db.schema), 'вьюшка пропала'
