"""
Точность чисел: Edm.Double разбирается в Decimal из ИСХОДНОЙ строки, а не через float.

Колонка в БД — NUMERIC, она хранит десятичное значение точно. Промежуточный float этому замыслу
противоречит: числа 1С допускают до 38 разрядов, а у float точность кончается на 2**53.

Искажение при этом МОЛЧАЛИВОЕ и стабильное: и поток изменений, и полная выгрузка читают через один
и тот же конвертер, поэтому согласуются друг с другом — сверка `rows_modified` расхождения не
покажет, потому что сравнивать не с чем. Значение просто неверно.
"""

from decimal import Decimal

import pytest
from sqlalchemy import MetaData, Table, select

from onecdc import DataObject, NameMapper
from onecdc.data_reader import DataReader, _json_safe, _odata_literal
from onecdc.db_writer import DBWriter
from onecdc.metadata_reader import MetadataObject

OBJ = "InformationRegister_N"


@pytest.mark.parametrize("raw", [
    "3333.33",                    # обычная сумма — доезжала и раньше
    "0.1",                        # двоичной дроби не существует, но кратчайшее представление спасало
    "123456789012345.67",         # 17 значащих — тоже доезжало (аудит ошибочно приводит как потерю)
    "9007199254740993",           # 2**53 + 1: вот здесь float ломался, отдавая ...992.0
    "12345678901234567890.12",    # и здесь — вместе с переходом в экспоненциальную форму
    "-0.000000000000000001",
])
def test_a_number_arrives_exactly_as_1c_sent_it(raw):
    converted = DataReader._convert_value(raw, 'Double')

    assert converted == Decimal(raw), 'значение исказилось при разборе'
    # И обратно: литерал для $filter разбирается в то же самое число. Проверяем именно так, а не
    # сравнением строк: Decimal вправе записать -0.000000000000000001 как -1E-18 — значение то же.
    assert Decimal(_odata_literal(converted, 'Double')) == Decimal(raw)


def test_a_broken_number_still_becomes_null_instead_of_breaking_the_batch():
    # Decimal бросает InvalidOperation, а не ValueError — если не поймать, падала бы вся пачка
    # вместо одной строки с NULL.
    assert DataReader._convert_value('не число', 'Double') is None


@pytest.mark.parametrize("value, expected", [
    (Decimal('123456789012345.67'), '123456789012345.67'),
    (Decimal('1E+20'), '100000000000000000000'),   # str() дал бы '1E+20'
    (Decimal('1E-7'), '0.0000001'),                # str() дал бы '1E-7'
    (5, '5'),
])
def test_an_odata_literal_never_uses_exponential_form(value, expected):
    # Экспоненциальную запись 1С в $filter не принимает.
    assert _odata_literal(value, 'Double') == expected


def test_json_export_keeps_the_previous_wire_shape():
    # json.dumps Decimal не умеет вовсе. Отдаём float — ровно то, что уходило в приёмники до
    # появления точного разбора, чтобы не менять тип поля в сообщении.
    assert _json_safe(Decimal('3333.33')) == 3333.33
    assert isinstance(_json_safe(Decimal('3333.33')), float)


def test_an_exact_number_reaches_the_database(db):
    # Сквозная проверка: NUMERIC хранит точно, и значение доезжает без потерь.
    meta = MetadataObject(OBJ, {"Kod": "String", "Summa": "Double"}, {"Kod": "String"},
                          object_key=None)
    w = DBWriter(db.engine, NameMapper(), schema=db.schema)
    records = [{"Kod": "a", "Summa": DataReader._convert_value("9007199254740993", "Double"),
                "is_deleted_or_empty": False, "exchange_message_no": 1}]

    w.save(OBJ, DataObject(meta, records))

    table = Table(OBJ, MetaData(), schema=db.schema, autoload_with=db.engine)
    with db.engine.connect() as conn:
        assert conn.execute(select(table.c.Summa)).scalar() == Decimal("9007199254740993")
