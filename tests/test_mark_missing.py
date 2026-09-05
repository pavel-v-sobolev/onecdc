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
    recheck_answer — что 1С отвечает на перепроверку по ключу (запрос без пагинации).
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


def test_candidate_still_in_1c_survives_recheck(db, monkeypatch):
    # Выгрузка за период: строки может не быть в окне, потому что у неё изменилась дата, а не
    # потому что её удалили. Перед пометкой кандидат перепроверяется запросом по ключу.
    rep = _replicator(db)
    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1"), _record(Ref_Key="b", Val="2")]])
    rep.full_load(CATALOG, batch_size=10)

    calls = _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1")]],
                   recheck_answer=[_record(Ref_Key="b", Val="2")])
    rep.full_load(CATALOG, batch_size=10, mark_missing=True,
                  date_field="Date", date_from=date(2026, 6, 1))

    assert calls["recheck"], "перепроверка не выполнялась"
    assert "Ref_Key eq 'b'" in calls["recheck"][0]
    assert _rows(db, CATALOG)["b"]["is_deleted_or_empty"] is False


def test_candidate_absent_in_1c_is_marked_after_recheck(db, monkeypatch):
    rep = _replicator(db)
    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1"), _record(Ref_Key="b", Val="2")]])
    rep.full_load(CATALOG, batch_size=10)

    _pages(rep, CATALOG, monkeypatch, [[_record(Ref_Key="a", Val="1")]], recheck_answer=[])
    rep.full_load(CATALOG, batch_size=10, mark_missing=True,
                  date_field="Date", date_from=date(2026, 6, 1))

    assert _rows(db, CATALOG)["b"]["is_deleted_or_empty"] is True


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


def test_recheck_batches_by_query_length_not_key_count(db, monkeypatch):
    """
    Пачка перепроверки набирается по ДЛИНЕ строки запроса, а не по числу ключей: ограничивает её
    веб-сервер перед 1С (IIS по умолчанию — 2048 байт на query string), и меряет он байты.

    Раньше здесь стояло фиксированное «20 ключей на запрос», и на составном ключе с длинным
    кириллическим значением двадцать ключей давали query string за 2048 байт: IIS отвечал 404.15
    «Query String Too Long», и перепроверка падала целиком.
    """
    from urllib.parse import quote
    from onecdc.replicator import RECHECK_MAX_QUERY_BYTES, RECHECK_QUERY_RESERVE_BYTES

    rep = _replicator(db)
    budget = RECHECK_MAX_QUERY_BYTES - RECHECK_QUERY_RESERVE_BYTES
    assert rep._recheck_query_budget == budget

    # Ключ регистра составной, и значение длинное — как имя типа регистратора в реальной базе.
    long_value = "Документ_РеализацияТоваровУслуг_ДлинноеИмя"
    candidates = [{"Period": f"2026-01-{i:02d}", "Sklad": long_value} for i in range(1, 41)]
    sent = []
    monkeypatch.setattr(DataReader, "read_object",
                        lambda self, name, extra_filter=None, **kw: (sent.append(extra_filter), 0)[1])

    rep._still_in_1c(REGISTER, candidates, DataReader("http://x", rep.metadata))

    assert len(sent) > 1, 'сорок таких ключей обязаны разойтись по нескольким запросам'
    for flt in sent:
        encoded = len(quote(flt, safe="':"))
        assert encoded <= budget or flt.count(' or ') == 0, \
            f'закодированная длина {encoded} превысила бюджет {budget}'
    # Ключи не потерялись и не продублировались.
    assert sum(flt.count('(') for flt in sent) == len(candidates)


def test_recheck_lowers_the_budget_when_the_server_refuses(db, monkeypatch):
    """
    Страховка: лимит веб-сервера оказался ниже умолчания. Получив 404.15 (так его отдаёт IIS) или
    414, перепроверка опускает бюджет и повторяет ТУ ЖЕ пачку, а не теряет её.
    """
    import requests
    from onecdc.replicator import RECHECK_BUDGET_DIVISOR

    rep = _replicator(db)
    before = rep._recheck_query_budget
    candidates = [{"Period": f"2026-01-{i:02d}", "Sklad": "S" * 200} for i in range(1, 11)]
    sent = []

    class _Resp:
        status_code = 404
        text = 'Ошибка IIS 10.0 — 404.15 — Query String Too Long'

    def fake_read_object(self, name, extra_filter=None, **kw):
        sent.append(extra_filter)
        # Отказываем только первой (самой длинной) пачке.
        if len(sent) == 1:
            raise requests.HTTPError('too long', response=_Resp())
        return 0

    monkeypatch.setattr(DataReader, "read_object", fake_read_object)
    rep._still_in_1c(REGISTER, candidates, DataReader("http://x", rep.metadata))

    assert rep._recheck_query_budget == before // RECHECK_BUDGET_DIVISOR, 'бюджет обязан опуститься'
    assert len(sent[1]) < len(sent[0]), 'та же пачка пересобрана короче'
    # Ни один ключ не потерян: суммарно отправлено ровно столько условий, сколько кандидатов.
    assert sum(flt.count('(') for flt in sent[1:]) == len(candidates)


def test_recheck_does_not_mistake_a_real_404_for_a_long_query(db, monkeypatch):
    """
    IIS отдаёт «слишком длинно» обычным 404, поэтому по одному коду его не отличить от опечатки в
    имени объекта. Без приметы в теле 404 обязан пробрасываться, а не запускать деление бюджета.
    """
    import requests

    rep = _replicator(db)
    before = rep._recheck_query_budget
    candidates = [{"Period": f"2026-01-{i:02d}", "Sklad": "S" * 200} for i in range(1, 11)]

    class _Resp:
        status_code = 404
        text = '<m:error><m:message>Ресурс не найден</m:message></m:error>'

    def fake_read_object(self, name, extra_filter=None, **kw):
        raise requests.HTTPError('not found', response=_Resp())

    monkeypatch.setattr(DataReader, "read_object", fake_read_object)
    with pytest.raises(requests.HTTPError):
        rep._still_in_1c(REGISTER, candidates, DataReader("http://x", rep.metadata))
    assert rep._recheck_query_budget == before, 'настоящий 404 бюджет трогать не должен'


def test_recheck_of_a_record_set_register_asks_by_recorder_only(db, monkeypatch):
    """
    У регистра, подчинённого регистратору, перепроверять пачками через $filter нельзя ничем:
    `LineNumber` лежит внутри RecordSet (400 «Сегмент пути LineNumber не найден!»), `Recorder eq
    guid'…'` даёт 500 «Нельзя сравнивать поля неограниченной длины», а `Recorder eq '…'` строкой
    отвечает 200 и НОЛЬ строк — то есть молча врёт, и перепроверка пометила бы удалённым весь
    регистр.

    Поэтому спрашиваем про НАБОР прямым адресом, по одному запросу на регистратора: набор и есть
    та единица, которая либо существует, либо нет. Один регистратор — один запрос, сколько бы его
    строк ни было в кандидатах, а имя типа в адресе пишется с префиксом StandardODATA.
    """
    from onecdc.metadata_reader import MetadataObject

    rep = _replicator(db)
    name = "AccumulationRegister_R"
    rep.metadata[name] = MetadataObject(
        name, {"Recorder": "String", "Recorder_Type": "String", "LineNumber": "Int64"},
        {"Recorder": "String", "LineNumber": "Int64", "Recorder_Type": "String"},
        object_key=["Recorder", "Recorder_Type"])

    # Два регистратора, у каждого по три строки.
    candidates = [{"Recorder": rec, "Recorder_Type": "Document_X", "LineNumber": n}
                  for rec in ("r-1", "r-2") for n in (1, 2, 3)]
    asked = []
    monkeypatch.setattr(DataReader, "read_by_key",
                        lambda self, name, key_values: (asked.append(key_values), 0)[1])
    # $filter-путь для такого объекта не должен использоваться вовсе.
    monkeypatch.setattr(DataReader, "read_object",
                        lambda *a, **k: pytest.fail("перепроверка набора обязана идти по ключу"))

    rep._still_in_1c(name, candidates, DataReader("http://x", rep.metadata))

    assert len(asked) == 2, 'каждый регистратор спрашивается ровно один раз'
    assert [k["Recorder"] for k in asked] == ["r-1", "r-2"]
    assert all(k["Recorder_Type"] == "StandardODATA.Document_X" for k in asked), \
        'в адресе имя типа обязано быть с префиксом'
    assert all("LineNumber" not in k for k in asked), 'по LineNumber спрашивать нельзя'


def test_recheck_keeps_the_namespace_of_an_unavailable_recorder_type(db, monkeypatch):
    """
    Регистратором может быть документ, НЕ опубликованный в этом интерфейсе OData: его тип приходит
    со своим пространством имён — `UnavailableEntities.UnavailableEntity_<guid>`. Обычному типу в
    адресе надо вернуть снятый `StandardODATA.`, а этому — нельзя: с ним 1С отвечает 400
    «Недопустимое значение … для свойства составного типа». Отличаем по точке: в имени объекта 1С
    её нет.
    """
    from onecdc.metadata_reader import MetadataObject

    rep = _replicator(db)
    name = "AccumulationRegister_R"
    rep.metadata[name] = MetadataObject(
        name, {"Recorder": "String", "Recorder_Type": "String", "LineNumber": "Int64"},
        {"Recorder": "String", "LineNumber": "Int64", "Recorder_Type": "String"},
        object_key=["Recorder", "Recorder_Type"])

    unavailable = "UnavailableEntities.UnavailableEntity_767d0764-f1b6-4eb3-a430-24b3ea7175ef"
    candidates = [{"Recorder": "r-1", "Recorder_Type": "Document_X", "LineNumber": 1},
                  {"Recorder": "r-2", "Recorder_Type": unavailable, "LineNumber": 1}]
    asked = []
    monkeypatch.setattr(DataReader, "read_by_key",
                        lambda self, name, key_values: (asked.append(key_values), 0)[1])

    rep._still_in_1c(name, candidates, DataReader("http://x", rep.metadata))

    assert asked[0]["Recorder_Type"] == "StandardODATA.Document_X", 'обычному типу префикс нужен'
    assert asked[1]["Recorder_Type"] == unavailable, 'у недоступной сущности своё пространство имён'
