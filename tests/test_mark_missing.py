"""
Оффлайн-тесты пометки строк, пропавших из 1С (full_load(mark_missing=True)).

Без живой 1С: чтение страниц подменяется, всё остальное — настоящее, включая запись в БД, таблицу
ключей прогона и финальный UPDATE. Проверяется то, ради чего механизм и сделан: строка, которой в
1С больше нет, помечается (а не удаляется), свежую строку пометка не трогает, а при выгрузке за
период кандидат сперва перепроверяется в 1С — из окна он мог уехать, а не исчезнуть.
"""

from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import MetaData, Table, select, text

from onecdc import DataObject
from onecdc.data_reader import DataReader
from onecdc.metadata_reader import MetadataObject
from onecdc.replicator import Replicator
from conftest import TEST_QUEUE_GUID

CATALOG = "Catalog_X"
REGISTER = "InformationRegister_R"


def _replicator(db):
    rep = Replicator(odata_url="http://x", odata_auth=None, exchange_name="E",
                     queue_guid=TEST_QUEUE_GUID, engine=db.engine, db_schema=db.schema)
    rep.metadata.is_loaded = True
    rep.metadata[CATALOG] = MetadataObject(
        CATALOG, {"Ref_Key": "String", "Val": "String", "Date": "DateTime"},
        {"Ref_Key": "String"}, object_key=None)
    # Независимый регистр сведений: ключ составной, регистратора нет — сегодня у него нет вообще
    # никакого механизма удаления, поэтому он здесь и проверяется.
    rep.metadata[REGISTER] = MetadataObject(
        REGISTER, {"Period": "String", "Sklad": "String", "Kolichestvo": "Int64"},
        {"Period": "String", "Sklad": "String"}, object_key=None,
        dimensions=["Sklad"], resources=["Kolichestvo"])
    return rep


def _record(**fields):
    """Запись в том виде, в каком её отдаёт reader: со спец-полями."""
    return {**fields, "is_deleted_or_empty": False, "exchange_message_no": 0}


def _pages(rep, object_name, monkeypatch, pages, recheck_answer=None):
    """
    Подменяет чтение страниц: pages — список списков записей, по одной странице на вызов.

    recheck_answer оставлен ради одного: в calls["recheck"] копятся запросы БЕЗ $top, то есть те,
    которыми шла перепроверка кандидатов. Она убрана, и тесты проверяют, что таких запросов нет.
    """
    meta = rep.metadata[object_name]
    remaining = list(pages)
    calls = {"pages": 0, "recheck": []}

    def fake_read_object(self, name, top=None, key_fields=None,
                         extra_filter=None, skip=None):
        if top is None:
            # Перепроверка кандидатов: без $top и с фильтром по ключам.
            calls["recheck"].append(extra_filter)
            records = list(recheck_answer or [])
        else:
            calls["pages"] += 1
            records = remaining.pop(0) if remaining else []
        self.clear()
        self[name] = DataObject(meta, [dict(r) for r in records])
        return len(records)

    monkeypatch.setattr(DataReader, "read_object", fake_read_object)
    # Границы периода эти тесты не проверяют: без них выгрузка идёт одной выборкой, как и до
    # нарезки на периоды (см. Replicator._period_partitions).
    monkeypatch.setattr(DataReader, "read_date_bound", lambda *a, **k: None)
    return calls


def _rows(db, table_name):
    tbl = Table(table_name, MetaData(), schema=db.schema, autoload_with=db.engine)
    with db.engine.connect() as conn:
        return {r["Ref_Key"] if "Ref_Key" in r else (r["Period"], r["Sklad"]): dict(r)
                for r in conn.execute(select(tbl)).mappings()}


def test_row_gone_from_1c_is_marked(db, monkeypatch):
    rep = _replicator(db)
    alive = [_record(Ref_Key="a", Val="1"), _record(Ref_Key="b", Val="2")]
    _pages(rep, CATALOG, monkeypatch, [alive])
    rep.full_load(CATALOG, batch_size=10)
    before = _rows(db, CATALOG)

    # Второй прогон: "b" из 1С исчез (удалён физически — в обмен такое не приходит вовсе).
    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1")]])
    rep.full_load(CATALOG, batch_size=10, mark_missing=True)

    after = _rows(db, CATALOG)
    assert after["b"]["is_deleted_or_empty"] is True
    # Помечена, а не удалена, и merged_on поднят — иначе обработчик события не увидит.
    assert after["b"]["merged_on"] > before["b"]["merged_on"]
    assert after["a"]["is_deleted_or_empty"] is False
    assert after["a"]["merged_on"] == before["a"]["merged_on"]


def test_period_run_marks_only_rows_of_that_period(db, monkeypatch):
    """
    Выгрузка ЗА ПЕРИОД читала только своё окно, поэтому и помечать вправе только его.

    Без ограничения кандидатом становилась бы вся остальная таблица, и от пометки её спасала бы
    одна перепроверка — запрос в 1С на каждую пачку ключей, на каждом ночном прогоне.
    """
    rep = _replicator(db)
    outside = _record(Ref_Key="jan", Val="1", Date=datetime(2026, 1, 15))
    inside = _record(Ref_Key="jun", Val="2", Date=datetime(2026, 6, 15))
    _pages(rep, CATALOG, monkeypatch, [[outside, inside]])
    rep.full_load(CATALOG, batch_size=10)

    # Июньское окно, и 1С отдаёт по нему пусто: июньская строка исчезла. Январская в окно не
    # входит вовсе — прогон её не видел и судить о ней не может.
    calls = _pages(rep, CATALOG, monkeypatch, [[]])
    rep.full_load(CATALOG, batch_size=10, date_field="Date",
                  date_from=date(2026, 6, 1), date_to=date(2026, 6, 30))

    rows = _rows(db, CATALOG)
    assert rows["jun"]["is_deleted_or_empty"] is True, 'строка окна исчезла — помечаем'
    assert rows["jan"]["is_deleted_or_empty"] is False, 'строку вне окна прогон не видел'
    assert len(calls["recheck"]) <= 1, 'перепроверяются только кандидаты окна, одной пачкой'


def test_marking_is_on_by_default(db, monkeypatch):
    # Умолчание — помечать. Выключенная пометка тихо ломает всё, что построено поверх: витрина
    # видит изменения по merged_on, а у неудалённой строки он не двигается.
    rep = _replicator(db)
    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1"), _record(Ref_Key="b", Val="2")]])
    rep.full_load(CATALOG, batch_size=10)

    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1")]])
    rep.full_load(CATALOG, batch_size=10)

    assert _rows(db, CATALOG)["b"]["is_deleted_or_empty"] is True


def test_marking_can_still_be_turned_off(db, monkeypatch):
    # Выключается только осознанно и только у full_load: у расписания такого параметра нет вовсе.
    rep = _replicator(db)
    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1"), _record(Ref_Key="b", Val="2")]])
    rep.full_load(CATALOG, batch_size=10)

    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1")]])
    rep.full_load(CATALOG, batch_size=10, mark_missing=False)

    assert _rows(db, CATALOG)["b"]["is_deleted_or_empty"] is False


def test_row_written_during_the_run_is_not_marked(db, monkeypatch):
    # Гонка с изменениями: строку переписали уже после старта прогона, в снимок она не попала.
    # Guard по merged_on тот же, что и у самой выгрузки: такую строку снимок не трогает.
    rep = _replicator(db)
    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1"), _record(Ref_Key="b", Val="2")]])
    rep.full_load(CATALOG, batch_size=10)
    with db.engine.begin() as conn:
        conn.execute(text(f'UPDATE "{db.schema}"."{CATALOG}" '
                          f"SET merged_on = now() + interval '1 hour' WHERE \"Ref_Key\" = 'b'"))

    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1")]])
    rep.full_load(CATALOG, batch_size=10, mark_missing=True)

    assert _rows(db, CATALOG)["b"]["is_deleted_or_empty"] is False


def test_a_row_that_moved_out_of_the_period_is_marked_anyway(db, monkeypatch):
    """
    Строка, уехавшая за пределы прочитанного, помечается удалённой — и это ОЖИДАЕМОЕ поведение.

    Раньше кандидатов переспрашивали в 1С по ключу. Спросить так можно только прямым адресом, а
    ответ «такого нет» приходит кодом 404 — и отличить его от отказа инфраструктуры можно лишь по
    телу ответа, которое веб-сервер волен подменить своей страницей (IIS так и делает по
    умолчанию). То есть правильность пометки зависела от настройки публикации, а на регистре по
    регистратору обходного пути нет вовсе: составной Recorder не фильтруется ничем.

    Поэтому переспрашивать перестали. Пометка снимается сама, когда строка приедет изменением
    (см. соседний тест) либо следующей выгрузкой её нового периода.
    """
    rep = _replicator(db)
    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1"), _record(Ref_Key="b", Val="2")]])
    rep.full_load(CATALOG, batch_size=10)

    # Строка b жива в 1С, но уехала из читаемого периода — прогон её не видит.
    calls = _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1")]])
    rep.full_load(CATALOG, batch_size=10, mark_missing=True,
                  date_field="Date", date_from=date(2026, 6, 1))

    assert _rows(db, CATALOG)["b"]["is_deleted_or_empty"] is True
    assert calls["recheck"] == [], 'запросов на перепроверку быть не должно'


def test_the_mark_is_lifted_when_the_row_arrives_with_changes(db, monkeypatch):
    """
    Обратная сторона отказа от перепроверки: ложная пометка обязана сниматься сама.

    На этом обещании всё и держится — объект, уехавший в другой период, это ЗАПИСЬ объекта в 1С,
    а значит регистрация в плане обмена и пакет изменений. Приехав, он перезаписывает строку, и
    пометка уходит вместе с остальными значениями.
    """
    rep = _replicator(db)
    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1"), _record(Ref_Key="b", Val="2")]])
    rep.full_load(CATALOG, batch_size=10)
    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1")]])
    rep.full_load(CATALOG, batch_size=10, mark_missing=True,
                  date_field="Date", date_from=date(2026, 6, 1))
    assert _rows(db, CATALOG)["b"]["is_deleted_or_empty"] is True

    # Тот же объект приезжает изменением (номер пакета >= 1, как у потока изменений).
    rep.writer.save(CATALOG, DataObject(rep.metadata[CATALOG],
                                        [_record(Ref_Key="b", Val="2", exchange_message_no=7)]))

    assert _rows(db, CATALOG)["b"]["is_deleted_or_empty"] is False, 'пометка не снялась'


def test_independent_register_row_is_marked_and_resource_reset(db, monkeypatch):
    rep = _replicator(db)
    both = [_record(Period="2026-06-01", Sklad="s1", Kolichestvo=5),
            _record(Period="2026-06-01", Sklad="s2", Kolichestvo=7)]
    _pages(rep, REGISTER, monkeypatch, [both])
    rep.full_load(REGISTER, batch_size=10)

    _pages(rep, REGISTER, monkeypatch, [[both[0]]])
    rep.full_load(REGISTER, batch_size=10, mark_missing=True)

    gone = _rows(db, REGISTER)[("2026-06-01", "s2")]
    assert gone["is_deleted_or_empty"] is True
    # Ресурс гасится в NULL: SUM игнорирует NULL, и итог остаётся верным даже в запросе,
    # забывшем фильтр по is_deleted_or_empty.
    assert gone["Kolichestvo"] is None


def test_keys_table_is_dropped_after_the_run(db, monkeypatch):
    rep = _replicator(db)
    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1")]])
    rep.full_load(CATALOG, batch_size=10, mark_missing=True)

    with db.engine.connect() as conn:
        left = conn.execute(text("SELECT tablename FROM pg_tables WHERE schemaname = :s "
                                 "AND tablename LIKE 'tmpkeys%'"), {"s": db.schema}).all()
    assert left == []


def test_empty_object_without_table_is_not_a_failure(db, monkeypatch):
    # Объект пуст и в 1С, и в БД: таблицу создаёт первая сохранённая страница, а её не было.
    # Помечать нечего — прогон обязан пройти спокойно.
    rep = _replicator(db)
    _pages(rep, CATALOG, monkeypatch, [[]])

    assert rep.full_load(CATALOG, batch_size=10, mark_missing=True) == 0


