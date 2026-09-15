"""
Оффлайн-тесты guard'ов полной выгрузки (DBWriter.save, full_load_started_at).

Guard сравнивает merged_on строки с моментом старта прогона: снимок не трогает то, что переписали
уже после его старта, но всё, что старше прогона, перезаписывает. В тестах момент старта задаётся
явно — OLD_RUN (прогон стартовал до всех изменений, снимок устарел) и NEW_RUN (прогон стартовал
после, снимок актуален), — поэтому проверки не зависят от реального времени и от разрешения часов
СУБД. Ключи строковые: тип ключа на guard не влияет.
"""

from datetime import datetime

from sqlalchemy import select, Table, MetaData

from onecdc import DataObject, NameMapper
from onecdc.db_writer import DBWriter
from onecdc.metadata_reader import MetadataObject

REF = "R1"
REF2 = "R2"

# Прогон, стартовавший до всех изменений: его снимок заведомо устарел.
OLD_RUN = datetime(2000, 1, 1)
# Прогон, стартовавший после всех изменений: его снимок свежее всего, что лежит в таблице.
NEW_RUN = datetime(2100, 1, 1)


def _writer(db):
    return DBWriter(db.engine, NameMapper(), schema=db.schema)


def _rows(writer, table_name):
    tbl = Table(table_name, MetaData(), schema=writer.schema,
                autoload_with=writer.engine)
    with writer.engine.connect() as conn:
        return [dict(r) for r in conn.execute(select(tbl)).mappings()]


# --- Документ/справочник: одиночная запись по ключу, update-guard ---

_DOC_META = MetadataObject("Catalog_X", {"Ref_Key": "String", "Val": "String"},
                           {"Ref_Key": "String"}, object_key=None)


def _doc(records):
    return DataObject(_DOC_META, records)


def _doc_rec(ref, val, emn):
    return {"Ref_Key": ref, "Val": val, "is_deleted_or_empty": False, "exchange_message_no": emn}


def test_full_load_does_not_overwrite_change_newer_than_the_run(db):
    w = _writer(db)
    # change записал строку
    w.save("Catalog_X", _doc([_doc_rec(REF, "change", 105)]))
    # прогон стартовал ДО этого изменения → его снимок устарел, перезаписывать нельзя
    w.save("Catalog_X", _doc([_doc_rec(REF, "fullload", 0)]), full_load_started_at=OLD_RUN)

    rows = {r["Ref_Key"]: r for r in _rows(w, "Catalog_X")}
    assert rows[REF]["Val"] == "change"


def test_full_load_repairs_row_older_than_the_run(db):
    w = _writer(db)
    # change записал строку — и, допустим, следующее изменение до нас не доехало
    w.save("Catalog_X", _doc([_doc_rec(REF, "stale", 105)]))
    # прогон стартовал ПОСЛЕ → снимок новее строки, полная выгрузка её выравнивает.
    # Это и есть смысл временного guard'а: строка, once тронутая изменением, не становится
    # неприкасаемой навсегда.
    w.save("Catalog_X", _doc([_doc_rec(REF, "fullload", 0)]), full_load_started_at=NEW_RUN)

    rows = {r["Ref_Key"]: r for r in _rows(w, "Catalog_X")}
    assert rows[REF]["Val"] == "fullload"


def test_change_overwrites_full_load_row(db):
    w = _writer(db)
    # сначала легла полная выгрузка
    w.save("Catalog_X", _doc([_doc_rec(REF, "fullload", 0)]), full_load_started_at=NEW_RUN)
    # затем пришло изменение — оно авторитетно и guard'ами не ограничено
    w.save("Catalog_X", _doc([_doc_rec(REF, "change", 106)]))

    rows = {r["Ref_Key"]: r for r in _rows(w, "Catalog_X")}
    assert rows[REF]["Val"] == "change" and rows[REF]["exchange_message_no"] == 106


def test_full_load_inserts_untouched_row(db):
    w = _writer(db)
    w.save("Catalog_X", _doc([_doc_rec(REF, "change", 105)]))
    # строку REF2 изменения не приносили — устаревший прогон всё равно её вставляет (backfill),
    # вставка новых строк guard'ом не ограничена
    w.save("Catalog_X", _doc([_doc_rec(REF, "old", 0), _doc_rec(REF2, "backfill", 0)]),
           full_load_started_at=OLD_RUN)

    rows = {r["Ref_Key"]: r for r in _rows(w, "Catalog_X")}
    assert rows[REF]["Val"] == "change"          # существующую свежую не тронули
    assert rows[REF2]["Val"] == "backfill"       # новую — вставили


# --- Групповой объект (табличная часть): own-or-skip по группе (Ref_Key) ---

_TP_META = MetadataObject(
    "Document_X_Rows", {"Ref_Key": "String", "LineNumber": "Int64", "Val": "String"},
    {"Ref_Key": "String", "LineNumber": "Int64"}, object_key=["Ref_Key"])


def _tp(records):
    return DataObject(_TP_META, records)


def _tp_rec(ref, line, val, emn):
    return {"Ref_Key": ref, "LineNumber": line, "Val": val,
            "is_deleted_or_empty": False, "exchange_message_no": emn}


def test_full_load_does_not_resurrect_row_deleted_after_the_run_started(db):
    w = _writer(db)
    # change заменил набор группы REF: строки 1,2; строка 3 удалена
    w.save("Document_X_Rows", _tp([_tp_rec(REF, 1, "a", 105), _tp_rec(REF, 2, "b", 105)]))
    # прогон стартовал ДО этого изменения: его снимок видит и строку 3 — не должен её воскресить
    # и не должен тронуть «горячую» группу
    w.save("Document_X_Rows", _tp([_tp_rec(REF, 1, "a", 0), _tp_rec(REF, 2, "b", 0),
                                   _tp_rec(REF, 3, "resurrected", 0)]),
           full_load_started_at=OLD_RUN)

    rows = {(r["Ref_Key"], r["LineNumber"]): r for r in _rows(w, "Document_X_Rows")}
    assert (REF, 3) not in rows                       # воскрешение заблокировано insert_condition
    assert rows[(REF, 1)]["exchange_message_no"] == 105   # горячая группа не тронута
    assert rows[(REF, 2)]["exchange_message_no"] == 105


def test_full_load_replaces_group_older_than_the_run(db):
    w = _writer(db)
    # группу принесло изменение: строки 1,2
    w.save("Document_X_Rows", _tp([_tp_rec(REF, 1, "a", 105), _tp_rec(REF, 2, "b", 105)]))
    # прогон стартовал ПОСЛЕ: снимок новее группы и заменяет её целиком
    w.save("Document_X_Rows", _tp([_tp_rec(REF, 1, "fixed", 0)]), full_load_started_at=NEW_RUN)

    rows = {(r["Ref_Key"], r["LineNumber"]): r for r in _rows(w, "Document_X_Rows")}
    assert rows[(REF, 1)]["Val"] == "fixed"
    # строка 2 выпала из набора: не удалена, а помечена (delete_mode='mark')
    assert rows[(REF, 2)]["is_deleted_or_empty"]


def test_full_load_replaces_own_group(db):
    w = _writer(db)
    # группа REF2 создана полной выгрузкой
    w.save("Document_X_Rows", _tp([_tp_rec(REF2, 1, "x", 0), _tp_rec(REF2, 2, "y", 0)]),
           full_load_started_at=OLD_RUN)
    # повторный прогон, стартовавший позже: в источнике осталась только строка 1
    w.save("Document_X_Rows", _tp([_tp_rec(REF2, 1, "x", 0)]), full_load_started_at=NEW_RUN)

    rows = {(r["Ref_Key"], r["LineNumber"]): r for r in _rows(w, "Document_X_Rows")}
    # «свою» группу заменили снимком: строка 1 живая, строка 2 помечена
    assert not rows[(REF2, 1)]["is_deleted_or_empty"]
    assert rows[(REF2, 2)]["is_deleted_or_empty"]


# --- Группа, которую прогон обновляет и дополняет одной страницей ---
#
# Отметка здесь — настоящая (writer.db_now()), а не NEW_RUN: guard сравнивает merged_on с ней, и
# при отметке в 2100 году он не срабатывает вовсе, то есть такие тесты проходят вхолостую. Дефект
# жил ровно в этой слепой зоне.

def test_full_load_adds_a_row_to_a_group_it_updates_on_the_same_page(db):
    """
    Один прогон обязан сойтись: страница [1 изменена, 2, 3 новая] даёт в таблице три строки.

    dbmerge выполняет UPDATE перед INSERT и штампует обновлённой строке merged_on = now(), то есть
    время после отметки страницы. Пока guard вставки смотрел только на merged_on, снимок принимал
    СВОЮ же строку 1 за чужое изменение, считал группу горячей и не вставлял строку 3. Полная
    выгрузка — заявленный способ выровнять данные, а сходилась она только за два прогона.
    """
    w = _writer(db)
    w.save("Document_X_Rows", _tp([_tp_rec(REF, 1, "a", 0), _tp_rec(REF, 2, "b", 0)]))

    started = w.db_now()
    w.save("Document_X_Rows", _tp([_tp_rec(REF, 1, "a-changed", 0), _tp_rec(REF, 2, "b", 0),
                                   _tp_rec(REF, 3, "new", 0)]),
           full_load_started_at=started)

    rows = {(r["Ref_Key"], r["LineNumber"]): r for r in _rows(w, "Document_X_Rows")}
    assert sorted(line for _, line in rows) == [1, 2, 3], 'новая строка группы не вставлена'
    assert rows[(REF, 1)]["Val"] == "a-changed"
    assert not rows[(REF, 3)]["is_deleted_or_empty"]


def test_a_real_change_still_blocks_new_rows_of_its_group(db):
    """
    Обратная сторона: guard обязан остаться guard'ом.

    Отметка настоящая, как и выше, но группу переписал ПОТОК ИЗМЕНЕНИЙ (номер пакета >= 1) уже
    после старта прогона. Снимок устарел, и строку, которой изменение в группе не оставило,
    воскрешать нельзя.
    """
    w = _writer(db)
    started = w.db_now()
    # Изменение пришло ПОСЛЕ старта прогона и оставило в группе только строки 1 и 2.
    w.save("Document_X_Rows", _tp([_tp_rec(REF2, 1, "a", 105), _tp_rec(REF2, 2, "b", 105)]))

    # Снимок прогона старше: он всё ещё видит строку 3.
    w.save("Document_X_Rows", _tp([_tp_rec(REF2, 1, "a", 0), _tp_rec(REF2, 2, "b", 0),
                                   _tp_rec(REF2, 3, "resurrected", 0)]),
           full_load_started_at=started)

    rows = {(r["Ref_Key"], r["LineNumber"]): r for r in _rows(w, "Document_X_Rows")}
    assert (REF2, 3) not in rows, 'снимок воскресил строку, удалённую изменением'
    # И самой горячей группы снимок не касался.
    assert rows[(REF2, 1)]["exchange_message_no"] == 105
