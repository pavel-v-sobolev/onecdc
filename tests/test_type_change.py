"""
Смена типа поля на стороне 1С.

dbmerge заводит недостающие колонки, но тип существующей не меняет никогда — и первая же запись
после такой правки в конфигураторе упиралась бы в «column is of type uuid but expression is of type
character varying». Менять тип на месте не вправе и мы: в колонке лежат данные, привести их к новому
типу может быть некому, а решать за пользователя, что выбросить, библиотека не должна.

Поэтому старое отставляется в сторону, новое заводится пустым, а наполнить его историей
заказывается полной выгрузкой.
"""

import logging

import pytest
from sqlalchemy import MetaData, Table, inspect, text

from onecdc import DataObject, NameMapper
from onecdc.db_writer import DBWriter, _type_name
from onecdc.metadata_reader import MetadataObject

OBJ = "InformationRegister_X"
TABLE = "InformationRegister_X"


def _meta(obj_type: str, val_type: str = "String") -> MetadataObject:
    """Регистр с ключом (Obj, Obj_Type) и обычным полем Val."""
    return MetadataObject(OBJ, {"Obj": obj_type, "Obj_Type": "String", "Val": val_type},
                          {"Obj": obj_type, "Obj_Type": "String"}, object_key=None)


def _rec(obj, val="x"):
    return {"Obj": obj, "Obj_Type": "Catalog_Y", "Val": val,
            "is_deleted_or_empty": False, "exchange_message_no": 0}


def _writer(db, requested=None):
    return DBWriter(db.engine, NameMapper(), schema=db.schema,
                    request_full_load=None if requested is None else requested.append)


def _columns(db, table_name=TABLE):
    return {c['name']: c['type'] for c in inspect(db.engine).get_columns(table_name,
                                                                        schema=db.schema)}


# --- Сравнение типов ---

@pytest.mark.parametrize("declared, stored, same", [
    ("String", "VARCHAR(50)", True),    # пользователь сузил свою колонку — это его право
    ("String", "TEXT", True),           # объявление разное, тип один
    ("Double", "NUMERIC(15,2)", True),  # точность задал пользователь
    ("Double", "BIGINT", False),        # Int64 стал Double: Postgres молча округлил бы дробную часть
    ("Int64", "SMALLINT", False),       # разрядность выросла — в узкую колонку не влезет
    ("Guid", "VARCHAR", False),         # ссылка стала строкой (CDC-14)
])
def test_only_a_real_type_change_counts(db, declared, stored, same):
    from onecdc.metadata_reader import type_mapping
    with db.engine.begin() as conn:
        conn.execute(text(f'create table "{db.schema}".t (c {stored})'))
    actual = _columns(db, 't')['c']
    dialect = db.engine.dialect

    assert (_type_name(dialect, type_mapping[declared]) == _type_name(dialect, actual)) is same


# --- Обычная колонка: переименовывается колонка ---

def test_an_ordinary_column_is_retired_and_recreated(db, caplog):
    requested = []
    w = _writer(db, requested)
    w.save(OBJ, DataObject(_meta("Guid"), [_rec("11111111-1111-1111-1111-111111111111")]))
    assert 'Val' in _columns(db)

    # В 1С у Val тип поменялся на число.
    with caplog.at_level(logging.WARNING):
        w2 = _writer(db, requested)
        w2.save(OBJ, DataObject(_meta("Guid", val_type="Double"),
                                [_rec("11111111-1111-1111-1111-111111111111", val=5)]))

    columns = _columns(db)
    retired = [c for c in columns if c.startswith('Val_Old_')]
    assert len(retired) == 1, 'старая колонка должна остаться под другим именем'
    assert str(columns['Val']).upper().startswith('NUMERIC'), 'новая колонка нужного типа'
    assert retired[0] in caplog.text, 'в предупреждении обязано стоять новое имя старой колонки'
    assert requested == [OBJ], 'нужна полная выгрузка: новая колонка пуста'


# --- Колонка ключа: переименовывается ТАБЛИЦА ---

def test_a_key_column_retires_the_whole_table(db, caplog):
    """
    Переименовать саму колонку нельзя: Postgres тащит constraint за переименованием, колонка
    остаётся в первичном ключе, а PRIMARY KEY подразумевает NOT NULL — новую в ключ никто не
    добавит, старую больше никто не заполнит, и любая вставка упадёт по not-null.

    К тому же после смены типа ключа старые строки неопознаваемы: сопоставить их с источником
    нечем. Поэтому в сторону отставляется таблица целиком.
    """
    requested = []
    w = _writer(db, requested)
    w.save(OBJ, DataObject(_meta("Guid"), [_rec("11111111-1111-1111-1111-111111111111")]))

    with caplog.at_level(logging.WARNING):
        w2 = _writer(db, requested)
        w2.save(OBJ, DataObject(_meta("String"), [_rec("A")]))

    tables = set(inspect(db.engine).get_table_names(schema=db.schema))
    retired = [t for t in tables if t.startswith(f'{TABLE}_Old_')]
    assert len(retired) == 1, 'старая таблица должна остаться под другим именем'
    assert TABLE in tables, 'новая таблица заведена'
    assert str(_columns(db)['Obj']).upper().startswith('VARCHAR')
    assert retired[0] in caplog.text
    assert requested == [OBJ]

    # И главное: в новую таблицу пишется, а не падает по not-null (ради чего всё и затевалось).
    w2.save(OBJ, DataObject(_meta("String"), [_rec("B")]))
    with db.engine.connect() as conn:
        rows = conn.execute(text(f'select "Obj" from "{db.schema}"."{TABLE}" order by 1')).scalars().all()
    assert rows == ['A', 'B']


def test_an_unknown_column_type_is_left_alone(db, caplog):
    """
    Тип колонки, который мы не умеем читать, не трогаем: наше дело — заметить, что 1С прислала
    другое, а не наводить порядок в чужой схеме.

    Проверка на своём уровне, а не через save: колонку экзотического типа не переживёт сам dbmerge
    — он отражает целевую таблицу и строит по ней временную, а NullType не компилируется в DDL.
    Это его ограничение и к смене типа отношения не имеет.
    """
    from onecdc.metadata_reader import type_mapping
    requested = []
    w = _writer(db, requested)
    w.save(OBJ, DataObject(_meta("Guid"), [_rec("11111111-1111-1111-1111-111111111111")]))
    with db.engine.begin() as conn:
        conn.execute(text(f'alter table "{db.schema}"."{TABLE}" alter column "Val" type point '
                          f'using null'))

    w._retyped_tables.clear()
    with caplog.at_level(logging.WARNING):
        w._retype_changed_columns(TABLE, OBJ, {'Val': type_mapping['String']}, key=['Obj'])

    assert not [c for c in _columns(db) if c.startswith('Val_Old_')], 'ничего не переименовано'
    assert 'unknown column type' in caplog.text
    assert requested == [], 'полная выгрузка не заказывается: мы не знаем, менялось ли что-то'


def test_the_check_runs_once_per_table(db):
    # Отражение таблицы стоит запроса, а save зовётся на каждую страницу.
    w = _writer(db)
    w.save(OBJ, DataObject(_meta("Guid"), [_rec("11111111-1111-1111-1111-111111111111")]))
    assert TABLE in w._retyped_tables

    w._retyped_tables.clear()
    w.save(OBJ, DataObject(_meta("Guid"), [_rec("22222222-2222-2222-2222-222222222222")]))
    w.save(OBJ, DataObject(_meta("Guid"), [_rec("33333333-3333-3333-3333-333333333333")]))
    assert w._retyped_tables == {TABLE}


def test_a_long_name_survives_retirement(db, caplog):
    """
    Лимит идентификатора в Postgres — 63 БАЙТА, и он один на таблицы, колонки, индексы и
    constraint'ы. Длиннее — молча усекается (проверено на живой БД: 83 байта на входе, 63 на
    выходе), а два разных длинных имени схлопнулись бы в одно.

    Отставка имя удлиняет (`_Old_хэш`), поэтому уложиться в лимит обязаны и таблица, и её индексы.
    Кириллица тут не украшение: в ней 2 байта на символ, и именно на ней лимит достаётся быстрее.
    """
    long_obj = "InformationRegister_" + "Щ" * 30
    table = NameMapper().map_object_name(long_obj)

    def meta(obj_type):
        return MetadataObject(long_obj, {"Obj": obj_type, "Obj_Type": "String"},
                              {"Obj": obj_type, "Obj_Type": "String"}, object_key=None)

    def record(obj):
        return {"Obj": obj, "Obj_Type": "Catalog_Y",
                "is_deleted_or_empty": False, "exchange_message_no": 0}

    w = _writer(db)
    w.save(long_obj, DataObject(meta("Guid"), [record("11111111-1111-1111-1111-111111111111")]))
    w._retyped_tables.clear()
    with caplog.at_level(logging.WARNING):
        w.save(long_obj, DataObject(meta("String"), [record("A")]))

    inspector = inspect(db.engine)
    names = inspector.get_table_names(schema=db.schema)
    for name in names:
        assert len(name.encode('utf-8')) <= 63, f'имя таблицы не уложилось: {name}'
        for index in inspector.get_indexes(name, schema=db.schema):
            assert len(index['name'].encode('utf-8')) <= 63, f'имя индекса не уложилось: {index}'
    assert table in names, 'новая таблица заведена под прежним именем'
    assert len([n for n in names if n != table]) >= 1, 'старая отставлена, а не потеряна'
