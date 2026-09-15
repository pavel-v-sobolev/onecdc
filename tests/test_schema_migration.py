"""
Переезд состава служебных таблиц на ДЕЙСТВУЮЩЕЙ установке.

`create(checkfirst=True)` заводит таблицу целиком, но давно созданную не трогает. Поэтому колонка,
добавленная в служебную таблицу новой версией библиотеки, на свежей базе появляется, а на
работающей — нет, и первый же запрос падает с «column does not exist».

Оффлайн-тесты такого не ловят по построению: каждый начинает с пустой схемы. Оба дефекта ниже
нашлись живым прогоном против dev-контура, по одному за прогон, — отсюда и файл: правило «добавил
колонку — не забудь позвать переезд» в этом проекте забывали дважды, поэтому переезд переехал
внутрь create_table_if_absent, а тесты держат именно это свойство.
"""

from sqlalchemy import Column, Integer, MetaData, String, Table, text

from onecdc.db_logs import REPLICATOR_LOG, ReplicatorLog, create_table_if_absent
from onecdc.write_tracker import WRITES_TABLE, WriteTracker


def _columns(db, table_name: str) -> set[str]:
    with db.engine.connect() as conn:
        return set(conn.execute(text(
            "select column_name from information_schema.columns "
            "where table_schema = :s and table_name = :t"),
            {'s': db.schema, 't': table_name}).scalars().all())


def test_creating_a_table_also_migrates_an_existing_one(db):
    """Общее свойство, из которого следуют оба случая ниже: кто завёл таблицу, тот её и обновил."""
    def table() -> Table:
        return Table('onecdc_migration_probe', MetaData(),
                     Column('id', Integer, primary_key=True),
                     Column('added_later', String(64)),
                     schema=db.schema)

    with db.engine.begin() as conn:
        conn.execute(text(f'CREATE TABLE "{db.schema}".onecdc_migration_probe (id integer)'))

    existing, added = create_table_if_absent(db.engine, table())

    assert added == {'added_later'}, 'колонка не доехала на существующую таблицу'
    assert existing == {'id'}, 'вызывающему нужен состав ДО переезда: по нему переносят данные'
    # Второй вызов ничего не меняет: переезд идемпотентен, его зовут на каждом старте процесса.
    assert create_table_if_absent(db.engine, table())[1] == set()


def test_the_registry_of_an_older_install_gets_the_new_column(db):
    """Реестр идущих записей: колонка signal_source добавлена позже."""
    # Таблица в том виде, в каком её создавала прошлая версия — без signal_source.
    with db.engine.begin() as conn:
        conn.execute(text(
            f'CREATE TABLE "{db.schema}"."{WRITES_TABLE}" ('
            ' id varchar(64) PRIMARY KEY,'
            ' owner varchar(255) NOT NULL,'
            ' object_name varchar(255) NOT NULL,'
            ' started_at timestamp NOT NULL,'
            ' heartbeat_at timestamp NOT NULL)'))

    tracker = WriteTracker(db.engine, db.schema, 'План1')
    try:
        # Разбор брошенных читает signal_source — на старой таблице это и падало.
        assert tracker.deliver_abandoned() == 0
        assert 'signal_source' in _columns(db, WRITES_TABLE), 'колонка не доехала'
    finally:
        tracker.close()


def test_the_replicator_log_of_an_older_install_gets_the_new_column(db):
    """
    Тот же дефект во второй таблице — журнале загрузки, куда позже добавили `type`.

    Живой прогон упал здесь сразу после того, как первый случай был починен точечно: своим вызовом
    переезда рядом с созданием. Это и показало, что чинить надо не таблицу, а правило.
    """
    with db.engine.begin() as conn:
        conn.execute(text(
            f'CREATE TABLE "{db.schema}"."{REPLICATOR_LOG}" ('
            ' id serial PRIMARY KEY,'
            ' exchange varchar,'
            ' object varchar,'
            ' message_no integer,'
            ' started_at timestamp)'))

    log = ReplicatorLog(db.engine, db.schema)

    # Первая же запись в журнал и падала с «column "type" does not exist».
    log_id = log.start('План1', 'Catalog_X', 7, 'changes')

    assert log_id
    assert {'type', 'finished_at', 'total_time'} <= _columns(db, REPLICATOR_LOG)
