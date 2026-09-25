"""
Оффлайн-тесты межпроцессного захвата объекта под полную выгрузку (full_load_claim).

Два процесса изображаются двумя экземплярами Replicator над одной базой: множества занятых
объектов в памяти у них разные, поэтому развести их может только отметка в onecdc_metadata_objects.
"""

import os
from datetime import timedelta

import pytest
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


def test_without_the_registry_the_claim_says_so_instead_of_lying(db):
    """
    Раньше здесь возвращалось True: заслон отвечал «занял», не заняв ничего, и два процесса,
    вызвавшие захват до загрузки метаданных, заходили в блок вместе (CDC-33).

    Ответ «не занял» при этом НЕ значит «занято кем-то»: реестра нет, значит его нет и у
    остальных, и захватить объект не мог никто. Эти два случая различает is_ready — по нему
    full_load решает, пропускать работу или выполнять её без заслона.
    """
    rep = _replicator(db)
    rep.metadata.objects_table = None

    assert rep._full_load_claim.is_ready is False
    assert rep._full_load_claim.claim(OBJECT) is False


def test_an_object_missing_from_the_registry_is_named_as_such(db, caplog):
    """«Не занял» из-за отсутствия строки неотличимо от «занято» — поэтому говорим об этом прямо."""
    import logging

    rep = _replicator(db)

    with caplog.at_level(logging.ERROR, logger='onecdc.full_load_claim'):
        assert rep._full_load_claim.claim('Catalog_НетТакого') is False

    assert 'not in the object registry' in '\n'.join(r.getMessage() for r in caplog.records)


def test_a_nested_claim_in_the_same_thread_goes_through(db):
    """
    Захват теперь берёт сам full_load, а снаружи его может держать claim_full_load (так делает
    чужое расписание). Без повторного входа вложенный вызов не смог бы занять объект У САМОГО
    СЕБЯ — условие «свободен или владелец молчит» ложно, владелец мы же — и молча пропустил бы
    работу.
    """
    rep = _replicator(db)
    claim = rep._full_load_claim

    with claim.hold(OBJECT) as outer:
        assert outer
        with claim.hold(OBJECT) as inner:
            assert inner, 'вложенный захват в том же потоке обязан проходить'
        # Выход из вложенного блока захват НЕ снимает — он не его.
        assert _owner(rep) == claim.owner
    assert _owner(rep) is None, 'внешний блок захват отпустил'


def test_another_thread_of_the_same_process_is_still_refused(db):
    """
    Повторный вход считается по потоку, а не по процессу: пока один поток держит объект, другой
    поток того же процесса обязан получить отказ — как и чужой процесс. Иначе фоновый воркер и
    пользовательский вызов выгружали бы один объект одновременно.
    """
    import threading

    rep = _replicator(db)
    claim = rep._full_load_claim
    result = []

    with claim.hold(OBJECT) as outer:
        assert outer
        thread = threading.Thread(target=lambda: result.append(claim.claim(OBJECT)))
        thread.start()
        thread.join()

    assert result == [False]


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


# --- full_load занимает объект сам (CDC-33) -----------------------------------------------------

def _stub_reading(rep, loaded):
    """Подменяет чтение под full_load: проверяем заслон, а не постраничный разбор."""
    rep._load_object = lambda name, **kw: loaded.append(name) or 7


def test_full_load_does_not_duplicate_work_of_another_process(db, caplog):
    """
    Пользователю не нужно ничего знать про захват: он зовёт full_load. Если тот же объект прямо
    сейчас выгружает кто-то другой, работа не дублируется — самая дорогая операция для 1С.
    """
    import logging

    first, second = _replicator(db), _replicator(db)
    loaded = []
    _stub_reading(second, loaded)

    with first.claim_full_load(OBJECT) as claimed:
        assert claimed
        with caplog.at_level(logging.WARNING, logger='onecdc.replicator'):
            assert second.full_load(OBJECT) == 0, 'изменить мы ничего не изменили'

    assert loaded == [], 'выгрузка не должна была пойти'
    assert 'already running elsewhere' in '\n'.join(r.getMessage() for r in caplog.records)


def test_the_object_is_released_after_the_load(db):
    """Захват живёт ровно на время выгрузки: следующий вызов должен пройти."""
    rep = _replicator(db)
    loaded = []
    _stub_reading(rep, loaded)

    assert rep.full_load(OBJECT) == 7
    assert _owner(rep) is None, 'захват не снят'
    assert rep.full_load(OBJECT) == 7, 'второй прогон упёрся в собственный захват'
    assert loaded == [OBJECT, OBJECT]


def test_those_who_keep_score_can_tell_busy_from_no_changes(db):
    """
    Фоновому воркеру и расписанию ответ 0 не годится: отметить объект выгруженным, не выгрузив
    его, значит снять заказ и больше к нему не вернуться. Для них — исключение.
    """
    from onecdc.replicator import FullLoadBusy

    first, second = _replicator(db), _replicator(db)
    _stub_reading(second, [])

    with first.claim_full_load(OBJECT):
        with pytest.raises(FullLoadBusy, match=OBJECT):
            second.full_load(OBJECT, skip_if_busy=False)


def test_the_heartbeat_does_not_compete_with_the_pages_for_connections(db):
    """
    Отметка живости идёт ОТДЕЛЬНЫМ пулом. Самая частая причина ложного истечения — не смерть
    процесса, а то, что потоку отметки не досталось соединения: страницы выгрузки разобрали пул
    целиком. Здесь цена этого выше всего — захват живого процесса достаётся чужому расписанию, и
    1С делает ту же работу дважды.

    Пул берём свой и крошечный: занимать общий тестовый значило бы подвесить соседние тесты.
    """
    from sqlalchemy import create_engine

    # Два соединения: одного не хватает даже на подготовку реестра (dbmerge держит своё под
    # промежуточную таблицу). Дальше занимаем ОБА — это и есть «страницы разобрали пул».
    engine = create_engine(db.engine.url.render_as_string(hide_password=False),
                           pool_size=2, max_overflow=0, pool_timeout=1)
    rep = Replicator(odata_url="http://x", odata_auth=None, engine=engine,
                     exchange_name="E", queue_guid=TEST_QUEUE_GUID, db_schema=db.schema)
    rep.metadata.is_loaded = True
    rep.metadata[OBJECT] = MetadataObject(OBJECT, {"Ref_Key": "Guid"}, {"Ref_Key": "Guid"})
    rep.metadata._sync_objects([OBJECT])
    try:
        assert rep._full_load_claim._heartbeat_engine is rep._lease_engine
        assert rep.writes._heartbeat_engine is rep._lease_engine
        assert rep._lease_engine is not engine, 'иначе изоляции нет'

        assert rep._full_load_claim.claim(OBJECT)
        with engine.connect(), engine.connect():    # пул занят целиком
            rep._full_load_claim.heartbeat()        # а отметка всё равно проходит
            rep.writes.heartbeat()
        assert _owner(rep) == rep._full_load_claim.owner
    finally:
        rep.close()
        engine.dispose()
