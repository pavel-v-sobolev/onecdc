"""
Оффлайн-тесты межпроцессного захвата объекта под полную выгрузку (full_load_claim).

Два процесса изображаются двумя экземплярами Replicator над одной базой: множества занятых
объектов в памяти у них разные, поэтому развести их может только отметка в onecdc_metadata_objects.
"""

import os
from datetime import timedelta

from sqlalchemy import func, select, update

from onecdc.full_load_claim import (CLAIM_HEARTBEAT_PERIOD, CLAIM_HEARTBEAT_RETRY_PERIOD,
                                    CLAIM_HEARTBEAT_TTL, HEARTBEAT_FIELD, OWNER_FIELD)
from onecdc.metadata_reader import MetadataObject
from onecdc.replicator import Replicator
from conftest import TEST_QUEUE_GUID

OBJECT = "Catalog_X"


def _replicator(db):
    rep = Replicator(odata_url="http://x", odata_auth=None, exchange_name="E",
                     queue_guid=TEST_QUEUE_GUID, engine=db.engine, db_schema=db.schema)
    rep.metadata.is_loaded = True
    rep.metadata[OBJECT] = MetadataObject(OBJECT, {"Ref_Key": "Guid"}, {"Ref_Key": "Guid"})
    # Реестр объектов заводит синхронизация метаданных — в тестах зовём её напрямую, без сети.
    rep.metadata._sync_objects([OBJECT])
    return rep


def _owner(rep):
    table = rep.metadata.objects_table
    with rep.engine.connect() as conn:
        return conn.execute(select(table.c[OWNER_FIELD])
                            .where(table.c.object_full_name == OBJECT)).scalar()


def _age_the_claim(rep, seconds):
    """Отодвигает отметку живости в прошлое — так выглядит захват умершего процесса."""
    table = rep.metadata.objects_table
    with rep.engine.begin() as conn:
        now = conn.scalar(select(func.now()))
        conn.execute(update(table).where(table.c.object_full_name == OBJECT)
                     .values(**{HEARTBEAT_FIELD: now - timedelta(seconds=seconds)}))


def test_owner_is_unique_per_replicator(db):
    # Владельцы разных экземпляров не совпадают, иначе release одного снимал бы захват другого.
    # Имя плана обмена и pid внутри — чтобы по строке в БД было видно, кто держит объект.
    first, second = _replicator(db), _replicator(db)
    assert first._full_load_claim.owner != second._full_load_claim.owner
    assert str(os.getpid()) in first._full_load_claim.owner


def test_another_process_is_refused(db):
    first, second = _replicator(db), _replicator(db)
    with first.claim_full_load(OBJECT) as claimed_first:
        assert claimed_first
        assert _owner(first) == first._full_load_claim.owner
        with second.claim_full_load(OBJECT) as claimed_second:
            assert not claimed_second, 'объект уже выгружает другой процесс'
    # Захват снят по выходу из блока, и объект снова свободен.
    assert _owner(first) is None
    with second.claim_full_load(OBJECT) as claimed_second:
        assert claimed_second


def test_dead_process_does_not_block_forever(db):
    # Отметку живости обновляет поток владельца. Перестала обновляться — процесс умер, и через
    # CLAIM_HEARTBEAT_TTL объект достаётся следующему.
    first, second = _replicator(db), _replicator(db)
    with first.claim_full_load(OBJECT) as claimed:
        assert claimed
        _age_the_claim(first, CLAIM_HEARTBEAT_TTL + 60)
        with second.claim_full_load(OBJECT) as taken_over:
            assert taken_over, 'захват брошен — перехватываем'
        assert _owner(first) == second._full_load_claim.owner or _owner(first) is None


def test_release_touches_only_own_claim(db):
    # Свой захват мог быть перехвачен по TTL: снимая его, нельзя сбить чужой.
    first, second = _replicator(db), _replicator(db)
    first._full_load_claim.claim(OBJECT)
    _age_the_claim(first, CLAIM_HEARTBEAT_TTL + 60)
    assert second._full_load_claim.claim(OBJECT)

    first._full_load_claim.release(OBJECT)
    assert _owner(first) == second._full_load_claim.owner, 'чужой захват остался на месте'


def test_heartbeat_keeps_the_claim_alive(db):
    first, second = _replicator(db), _replicator(db)
    first._full_load_claim.claim(OBJECT)
    _age_the_claim(first, CLAIM_HEARTBEAT_TTL + 60)
    first._full_load_claim.heartbeat()

    assert not second._full_load_claim.claim(OBJECT), 'владелец жив — объект занят'


def test_claim_is_a_no_op_without_the_registry(db):
    # Прямой вызов full_load должен работать и на пустой базе, где реестра ещё нет.
    rep = _replicator(db)
    rep.metadata.objects_table = None
    assert rep._full_load_claim.claim(OBJECT) is True
    rep._full_load_claim.release(OBJECT)


def test_failed_heartbeat_is_retried_sooner(db, monkeypatch):
    """
    Неудачная попытка продлить захват повторяется через укороченную паузу.

    Неудача — это почти всегда нехватка соединений в пуле: все заняты страницами выгрузки, а
    отметка живости приходит за своим. С обычным периодом на весь TTL пришлось бы четыре попытки,
    и разовая давка на пул стоила бы захвата: чужое расписание сочло бы живой процесс мёртвым и
    занялось бы тем же объектом (воспроизведено на живой связке с pool_size=2).
    """
    rep = _replicator(db)
    claim = rep._full_load_claim
    # Цикл прогоняется здесь вручную, поэтому фоновый поток claim() поднимать не должен: он крутил
    # бы тот же _heartbeat_loop параллельно, писал бы в те же delays и взводил бы _closed. Успеет
    # он дойти до wait раньше подмены или позже — вопрос скорости машины, и на медленном раннере
    # тест падал с [20.0, 5.0] вместо [5.0]. Взведённый флаг заставляет _start_heartbeat выйти сразу.
    claim._closed.set()
    claim.claim(OBJECT)
    claim._closed.clear()
    assert claim._heartbeat_thread is None, 'фоновый поток испортит замер: цикл крутится вручную'
    delays = []
    monkeypatch.setattr(claim, 'heartbeat', lambda: (_ for _ in ()).throw(RuntimeError('no connection')))
    monkeypatch.setattr(claim._closed, 'wait', lambda delay: delays.append(delay) or claim._closed.set())

    claim._heartbeat_loop()

    assert delays == [CLAIM_HEARTBEAT_RETRY_PERIOD]

    # Удачная попытка — обычный период.
    claim._closed.clear()
    delays.clear()
    monkeypatch.setattr(claim, 'heartbeat', lambda: None)
    claim._heartbeat_loop()

    assert delays == [CLAIM_HEARTBEAT_PERIOD]
