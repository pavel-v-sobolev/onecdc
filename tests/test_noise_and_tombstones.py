"""
Оффлайн-тесты записи изменений (DBWriter.save, режим изменений).

Два связанных механизма:
- шумной пакет (1С переписала объект, не изменив реквизитов) не должен трогать строку: отличие
  только в exchange_message_no / версии данных изменением не считается, merged_on остаётся прежним,
  иначе обработчик пересчитывал бы группу впустую;
- строка, выпавшая из набора регистра/табличной части, не удаляется, а помечается: числовые
  ресурсы гасятся в NULL, merged_on поднимается — событие удаления не должно исчезать бесследно.
"""


import pytest
from sqlalchemy import select, Table, MetaData

from onecdc import DataObject, NameMapper
from onecdc.data_reader import VERSION_FIELDS
from onecdc.db_writer import DBWriter
from onecdc.metadata_reader import MetadataObject

REF = "R1"


def _writer(db):
    return DBWriter(db.engine, NameMapper(), schema=db.schema)


def _rows(writer, table_name):
    tbl = Table(table_name, MetaData(), schema=writer.schema,
                autoload_with=writer.engine)
    with writer.engine.connect() as conn:
        return [dict(r) for r in conn.execute(select(tbl)).mappings()]


# --- Шум: справочник переписан без изменения реквизитов ---

# Имя поля версии проверяем обоими: платформа отдаёт DataVersion, документация описывает Version
# (см. VERSION_FIELDS). Тест на выдуманном имени прошёл бы, а на реальных данных механизм молчал.
@pytest.fixture(params=VERSION_FIELDS)
def version_field(request):
    return request.param


def _doc_meta(version_field):
    return MetadataObject("Catalog_X", {"Ref_Key": "String", "Val": "String",
                                          version_field: "String"},
                            {"Ref_Key": "String"}, object_key=None)


def _save_doc(writer, version_field, val, emn, version):
    record = {"Ref_Key": REF, "Val": val, version_field: version,
              "is_deleted_or_empty": False, "exchange_message_no": emn}
    return writer.save("Catalog_X", DataObject(_doc_meta(version_field), [record]))


def test_noisy_packet_does_not_touch_the_row(db, version_field):
    w = _writer(db)
    _save_doc(w, version_field, "a", emn=105, version="v1")
    before = _rows(w, "Catalog_X")[0]

    # 1С переписала объект: новый пакет и новая версия данных, реквизиты те же.
    result = _save_doc(w, version_field, "a", emn=106, version="v2")

    after = _rows(w, "Catalog_X")[0]
    assert result.updated_row_count == 0
    assert after["merged_on"] == before["merged_on"]        # обработчик ничего не пересчитает
    assert after["exchange_message_no"] == 105              # шумные поля тоже не переписаны
    assert after[version_field] == "v1"


def test_real_change_writes_noisy_fields_too(db, version_field):
    w = _writer(db)
    _save_doc(w, version_field, "a", emn=105, version="v1")

    result = _save_doc(w, version_field, "b", emn=106, version="v2")

    after = _rows(w, "Catalog_X")[0]
    assert result.updated_row_count == 1
    assert after["Val"] == "b"
    # строку обновило настоящее изменение — вместе с ней записались и шумные поля
    assert after["exchange_message_no"] == 106 and after[version_field] == "v2"


# --- Надгробия: строка выпала из набора движений регистра ---

_REG_META = MetadataObject(
    "AccumulationRegister_Reg",
    {"Recorder": "String", "LineNumber": "Int64", "Kolichestvo": "Double", "Comment": "String"},
    {"Recorder": "String", "LineNumber": "Int64"}, object_key=["Recorder"],
    resources=["Kolichestvo"])


def _reg_rec(line, qty):
    return {"Recorder": REF, "LineNumber": line, "Kolichestvo": qty, "Comment": "c",
            "is_deleted_or_empty": False, "exchange_message_no": 105}


def test_row_dropped_from_the_set_is_marked_with_nulled_resource(db):
    w = _writer(db)
    w.save("AccumulationRegister_Reg",
           DataObject(_REG_META, [_reg_rec(1, 10), _reg_rec(2, 20)]))
    before = {r["LineNumber"]: r for r in _rows(w, "AccumulationRegister_Reg")}

    # следующий пакет привёз набор без строки 2
    w.save("AccumulationRegister_Reg", DataObject(_REG_META, [_reg_rec(1, 10)]))

    rows = {r["LineNumber"]: r for r in _rows(w, "AccumulationRegister_Reg")}
    assert set(rows) == {1, 2}                       # строка не исчезла — осталась надгробием
    assert rows[2]["is_deleted_or_empty"]
    assert rows[2]["Kolichestvo"] is None            # SUM не заметит её и без фильтра по флагу
    assert rows[2]["Comment"] == "c"                 # не-ресурс сохранён: видно, что это была за строка
    assert rows[2]["merged_on"] > before[2]["merged_on"]   # событие видно обработчику
    # строка 1 не менялась и осталась нетронутой
    assert rows[1]["merged_on"] == before[1]["merged_on"]


def test_row_returning_to_the_set_is_resurrected(db):
    w = _writer(db)
    w.save("AccumulationRegister_Reg",
           DataObject(_REG_META, [_reg_rec(1, 10), _reg_rec(2, 20)]))
    w.save("AccumulationRegister_Reg", DataObject(_REG_META, [_reg_rec(1, 10)]))

    # строка 2 вернулась в набор
    w.save("AccumulationRegister_Reg",
           DataObject(_REG_META, [_reg_rec(1, 10), _reg_rec(2, 20)]))

    rows = {r["LineNumber"]: r for r in _rows(w, "AccumulationRegister_Reg")}
    assert not rows[2]["is_deleted_or_empty"]        # флаг снят входящим значением
    assert rows[2]["Kolichestvo"] == 20              # ресурс восстановлен
