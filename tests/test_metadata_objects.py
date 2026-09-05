"""
Оффлайн-тесты реестра объектов onecdc_metadata_objects (внутри MetadataReader).
Проверяют синхронизацию с $metadata (dbmerge delete — пропавшие объекты удаляются из реестра)
и флаги полной выгрузки (require/list/mark). Ключ реестра — полное имя объекта (object_full_name).
Без живой 1С, но с локальным PostgreSQL (см. conftest.py).
"""

from sqlalchemy import select

from onecdc.metadata_reader import MetadataReader


def _reader(db):
    return MetadataReader("http://x", engine=db.engine, schema=db.schema)


def _row(reader, object_full_name):
    t = reader.objects_table
    with reader.engine.connect() as c:
        return c.execute(select(t).where(t.c.object_full_name == object_full_name)).mappings().first()


def test_sync_inserts_and_deletes(db):
    reader = _reader(db)
    reader._sync_objects(["Catalog_A", "Document_B"])
    assert _row(reader, "Catalog_A")["object_type"] == "Catalog"
    assert _row(reader, "Document_B") is not None

    # Document_B пропал из метаданных → строка удаляется из реестра (delete_mode='delete').
    reader._sync_objects(["Catalog_A"])
    assert _row(reader, "Document_B") is None
    assert _row(reader, "Catalog_A") is not None


def test_require_full_load_if_new(db):
    reader = _reader(db)
    reader._sync_objects(["Catalog_A"])             # первая sync создаёт таблицу

    # Существующий, ещё не выгружавшийся → флаг ставится.
    reader.require_full_load_if_new("Catalog_A")
    assert _row(reader, "Catalog_A")["full_load_is_required"] in (True, 1)

    # После успешной выгрузки повторный приход НЕ требует выгрузки заново.
    reader.mark_full_loaded("Catalog_A")
    reader.require_full_load_if_new("Catalog_A")
    row = _row(reader, "Catalog_A")
    assert row["full_load_is_required"] in (False, 0)
    assert row["last_full_load_dt"] is not None


def test_require_full_load_if_new_reloads_unknown(db, monkeypatch):
    reader = _reader(db)
    reader._sync_objects(["Catalog_A"])

    # Объект пришёл в пакете, но его ещё нет в реестре → require_full_load_if_new перечитывает
    # метаданные (здесь стаб вместо сети: sync заводит строку), затем помечает на выгрузку.
    monkeypatch.setattr(reader, "get_metadata",
                        lambda: reader._sync_objects(["Catalog_A", "Catalog_New"]))

    reader.require_full_load_if_new("Catalog_New")
    assert _row(reader, "Catalog_New")["full_load_is_required"] in (True, 1)


def test_mark_full_loaded_keyed_by_full_name(db):
    # Отметка и список полной выгрузки работают по полному имени (object_full_name).
    reader = _reader(db)
    reader._sync_objects(["Document_Sales", "AccumulationRegister_Sales"])
    reader.require_full_load_if_new("Document_Sales")
    reader.require_full_load_if_new("AccumulationRegister_Sales")

    reader.mark_full_loaded("Document_Sales")       # выгружен только документ

    assert _row(reader, "Document_Sales")["full_load_is_required"] in (False, 0)
    assert _row(reader, "AccumulationRegister_Sales")["full_load_is_required"] in (True, 1)
    assert reader.list_full_load_required() == ["AccumulationRegister_Sales"]


def test_list_full_load_required_excludes_done_and_deleted(db):
    reader = _reader(db)
    reader._sync_objects(["Catalog_A", "Document_B", "Catalog_C"])
    reader.require_full_load_if_new("Catalog_A")    # требуется
    reader.require_full_load_if_new("Document_B")   # требуется, но станет удалённым
    reader.mark_full_loaded("Catalog_C")            # уже выгружен → не требуется

    reader._sync_objects(["Catalog_A", "Catalog_C"])  # Document_B удалён из реестра

    assert reader.list_full_load_required() == ["Catalog_A"]
