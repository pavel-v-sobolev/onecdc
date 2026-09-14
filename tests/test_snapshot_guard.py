"""
Снимок полной выгрузки не должен затирать подтверждённое состояние, а исчезнувший владелец
обязан уводить за собой свои табличные части.

Две находки одного корня — в полной выгрузке табличная часть второго сорта.

CDC-06. Guard снимка спрашивает «строку переписывали после моей отметки?», а нужен другой факт —
«строку ПОДТВЕРЖДАЛИ после моей отметки?». Разные они потому, что merged_on двигают только
изменившиеся значения: exchange_message_no и версия данных лежат в skip_compare_fields. Очередь 1С
хранит ссылки на объекты, а не значения, поэтому правка «туда и обратно» приезжает одним пакетом
с исходным значением — для БД это no-op, следа нет, и снимок, прочитавший страницу между двумя
правками, запишет устаревшее поверх актуального.

CDC-12. Ключи прогона собираются только по самому объекту: табличные части приезжают вложенно и
заменяются группой при приходе владельца. Владельца удалили физически — его entry не приходит,
заменять нечего, и строки его ТЧ живут дальше со своими суммами.
"""

import time
from pathlib import Path

from sqlalchemy import MetaData, Table, inspect, select, text

import fake_1c
from onecdc import DataObject, NameMapper, Replicator
from onecdc.db_writer import DBWriter
from onecdc.metadata_reader import MetadataObject

OBJ = "Catalog_X"
META = MetadataObject(OBJ, {"Ref_Key": "String", "Val": "String", "DataVersion": "String"},
                      {"Ref_Key": "String"})


def _writer(db) -> DBWriter:
    return DBWriter(db.engine, NameMapper(), schema=db.schema)


def _save(writer, val, emn, version, started=None, always_touch=False):
    record = {"Ref_Key": "R1", "Val": val, "DataVersion": version,
              "is_deleted_or_empty": False, "exchange_message_no": emn}
    return writer.save(OBJ, DataObject(META, [record]),
                       full_load_started_at=started, always_touch=always_touch)


def _row(db) -> dict:
    table = Table(OBJ, MetaData(), schema=db.schema, autoload_with=db.engine)
    with db.engine.connect() as conn:
        return dict(conn.execute(select(table)).mappings().one())


# --- CDC-06: no-op пакет под снимком обязан оставить след ---

def test_a_noop_packet_leaves_no_trace_when_no_snapshot_is_running(db):
    # Штатное поведение, которое менять нельзя: 1С регистрирует объект на любую перезапись, и
    # будить обработчиков на таких пакетах незачем.
    w = _writer(db)
    _save(w, "A", 1, "v1")
    before = _row(db)['merged_on']

    result = _save(w, "A", 2, "v2")

    assert result.updated_row_count == 0
    assert _row(db)['merged_on'] == before


def test_a_noop_packet_under_a_snapshot_moves_merged_on(db):
    w = _writer(db)
    _save(w, "A", 1, "v1")
    before = _row(db)['merged_on']

    result = _save(w, "A", 2, "v2", always_touch=True)

    assert result.updated_row_count == 1, 'без следа снимок затрёт эту строку устаревшей'
    assert _row(db)['merged_on'] > before


def test_a_stale_snapshot_no_longer_overwrites_a_confirmed_value(db):
    """
    Сценарий целиком, по шагам:

      t0  в БД A, в 1С A
      t1  в 1С стало B (изменение ещё в очереди)
      t2  страница снимка прочитала B; отметка страницы взята перед чтением
      t3  в 1С снова A
      t4  приходит пакет: текущее значение A, для БД это no-op, пакет подтверждён
      t5  снимок сохраняет прочитанное B
    """
    w = _writer(db)
    _save(w, "A", 1, "v1")                      # t0
    page_started_at = w.db_now()                # t2: отметка страницы
    time.sleep(0.05)
    _save(w, "A", 2, "v2", always_touch=True)   # t4: пакет с A — под снимком след остаётся

    _save(w, "B", None, "v0", started=page_started_at)   # t5: снимок пишет устаревшее B

    assert _row(db)['Val'] == "A", 'снимок записал устаревшее значение поверх подтверждённого'


def test_a_snapshot_still_repairs_a_row_nobody_confirmed(db):
    # Обратная сторона: выгрузка обязана оставаться способом выровнять данные. Строку, которую
    # после отметки страницы никто не трогал, снимок перезаписывает — в этом его смысл.
    w = _writer(db)
    _save(w, "A", 1, "v1")
    time.sleep(0.05)
    page_started_at = w.db_now()

    _save(w, "B", None, "v0", started=page_started_at)

    assert _row(db)['Val'] == "B"


# --- CDC-06: табличная часть наследует захват владельца ---

def test_a_table_part_inherits_the_claim_of_its_owner(db):
    with fake_1c.running_server(Path(__file__).parent / "responses" / "trade_demo_8.5") as (
            url, fake):
        repl = Replicator(odata_url=url, odata_auth=None, exchange_name="ДляODATA",
                          queue_guid=fake.queue_guid, engine=db.engine, db_schema=db.schema)
        repl.metadata.get_metadata()

        owner = 'Document_ЗаказКлиента'
        part = 'Document_ЗаказКлиента_Товары'
        assert repl.metadata.owner_of(part) == owner
        assert repl.metadata.owner_of(owner) is None

        assert repl._under_full_load(part, {owner}), \
            'у ТЧ своего захвата нет, а пишется она страницей владельца'
        assert not repl._under_full_load(part, set())
        assert repl._under_full_load(owner, {owner})


def test_a_dead_claim_does_not_keep_the_object_in_that_mode_forever(db):
    # Иначе выгрузка, упавшая посреди прогона, оставила бы объект переписываться на каждый шумный
    # пакет навсегда. Отсекается тем же TTL, которым claim() перехватывает брошенный захват.
    from onecdc.full_load_claim import CLAIM_HEARTBEAT_TTL, HEARTBEAT_FIELD

    with fake_1c.running_server(Path(__file__).parent / "responses" / "trade_demo_8.5") as (
            url, fake):
        repl = Replicator(odata_url=url, odata_auth=None, exchange_name="ДляODATA",
                          queue_guid=fake.queue_guid, engine=db.engine, db_schema=db.schema)
        repl.metadata.get_metadata()
        claim = repl._full_load_claim
        assert claim.claim('Catalog_Номенклатура')
        assert 'Catalog_Номенклатура' in claim.live_claims()

        claim.close()
        with db.engine.begin() as conn:
            conn.execute(claim.table.update().values(
                **{HEARTBEAT_FIELD: text(f"now() - interval '{CLAIM_HEARTBEAT_TTL + 60} seconds'")}))

        assert claim.live_claims() == set(), 'брошенный захват держит объект в режиме снимка вечно'


# --- CDC-12: исчезнувший владелец уводит свои табличные части ---

EMPTY_FEED = ('<?xml version="1.0" encoding="utf-8"?>'
              '<feed xmlns="http://www.w3.org/2005/Atom"><title>gone</title></feed>')


class _EmptyFeed:
    ok = True
    status_code = 200
    reason = 'OK'
    url = 'http://fake'
    text = EMPTY_FEED
    content = EMPTY_FEED.encode()


def _counts(db, table: str) -> tuple[int, int]:
    with db.engine.connect() as conn:
        return conn.execute(text(
            f'select count(*), count(*) filter (where is_deleted_or_empty) '
            f'from "{db.schema}"."{table}"')).one()


def test_table_parts_of_a_deleted_owner_are_marked(db, monkeypatch):
    from onecdc import data_reader

    with fake_1c.running_server(Path(__file__).parent / "responses" / "trade_demo_8.5") as (
            url, fake):
        repl = Replicator(odata_url=url, odata_auth=None, exchange_name="ДляODATA",
                          queue_guid=fake.queue_guid, engine=db.engine, db_schema=db.schema)
        for _ in range(6):
            repl.run_once()

        parts = [t for t in inspect(db.engine).get_table_names(schema=db.schema)
                 if t.startswith('Document_ZakazKlienta_')]
        alive_before = {t: _counts(db, t) for t in parts}
        assert any(total > marked for total, marked in alive_before.values()), \
            'предпосылка: у документа есть живые строки ТЧ'

        # Документ физически удалён в 1С: его entry не приходит вовсе.
        real_get = data_reader.requests.get
        monkeypatch.setattr(data_reader.requests, 'get',
                            lambda u, *a, **kw: (_EmptyFeed() if 'ЗаказКлиента?' in u
                                                 else real_get(u, *a, **kw)))
        repl.full_load('Document_ЗаказКлиента')

        total, marked = _counts(db, 'Document_ZakazKlienta')
        assert total == marked, 'предпосылка: сам документ помечен'
        for part in parts:
            total, marked = _counts(db, part)
            assert total == marked, f'{part}: строки удалённого документа остались живыми'


def test_marking_table_parts_is_idempotent(db, monkeypatch):
    # Повторный прогон не должен ни поднимать merged_on заново, ни будить обработчиков впустую.
    from onecdc import data_reader

    with fake_1c.running_server(Path(__file__).parent / "responses" / "trade_demo_8.5") as (
            url, fake):
        repl = Replicator(odata_url=url, odata_auth=None, exchange_name="ДляODATA",
                          queue_guid=fake.queue_guid, engine=db.engine, db_schema=db.schema)
        for _ in range(6):
            repl.run_once()
        real_get = data_reader.requests.get
        monkeypatch.setattr(data_reader.requests, 'get',
                            lambda u, *a, **kw: (_EmptyFeed() if 'ЗаказКлиента?' in u
                                                 else real_get(u, *a, **kw)))

        assert repl.full_load('Document_ЗаказКлиента') > 0
        assert repl.full_load('Document_ЗаказКлиента') == 0


# --- CDC-08: владение вшито в саму запись снимка ---

def test_a_snapshot_write_is_fenced_by_the_lease(db):
    """
    Страница выгрузки пишется минутами, и проверка «до записи» к моменту записи успевает
    устареть: мы могли замолчать на весь TTL уже после неё. Поэтому условие «объект всё ещё наш»
    вшивается в сам оператор — проверяет его СУБД в момент записи, а не мы заранее.
    """
    from sqlalchemy import literal

    def guard(value):
        return DBWriter(db.engine, NameMapper(), schema=db.schema,
                        lease_guard=lambda: literal(value))

    _save(guard(True), "A", 1, "v1")
    page_started_at = guard(True).db_now()
    time.sleep(0.05)

    # Аренда ушла — запись снимка обязана стать пустой.
    lost = guard(False).save(
        OBJ, DataObject(META, [{"Ref_Key": "R1", "Val": "B", "DataVersion": "v0",
                                "is_deleted_or_empty": False, "exchange_message_no": None}]),
        full_load_started_at=page_started_at)
    assert _row(db)['Val'] == "A", 'снимок записал, потеряв аренду объекта'
    assert lost.updated_row_count == 0

    # Аренда наша — та же запись проходит.
    _save(guard(True), "B", None, "v0", started=page_started_at)
    assert _row(db)['Val'] == "B"


def test_without_a_lease_the_guard_is_absent(db):
    # Прямой вызов save и тесты идут без аренды вовсе — условия тогда быть не должно, иначе
    # выгрузка на пустой базе не записала бы ничего.
    w = _writer(db)
    assert w.lease_guard is None
    assert w._still_ours() is None
