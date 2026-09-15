"""
Межпроцессная исключительность: узел обмена читает один процесс, витрину одного обработчика
считает один процесс.

Почему это не решалось транзакцией. Транзакция даёт атомарность и изоляцию от частичного
состояния, но НЕ даёт взаимного исключения: классическая потеря обновления помещается внутрь неё
целиком. Поэтому захват — один `UPDATE` с предикатом, а не `SELECT` с последующей записью;
арбитраж делает сама СУБД (см. lease).

Почему проигравший не ждёт освобождения. Процессы симметричны, роли ведущего нет: на каждой
итерации идёт честная гонка, и кто победит в следующий раз, заранее не определено. Не захвативший
пропускает итерацию и пробует снова — поднятый рядом второй контейнер благодаря этому безвреден.
"""

import threading
from pathlib import Path

import pytest
from sqlalchemy import MetaData, text

import fake_1c
from onecdc import Replicator
from onecdc.db_logs import (NODE_HEARTBEAT_FIELD, NODE_KEY_FIELD, NODE_OWNER_FIELD,
                            create_table_if_absent, exchange_nodes_table)
from onecdc.handlers import LEASE_HEARTBEAT_FIELD, LEASE_OWNER_FIELD, HandlerLoop
from onecdc.lease import LEASE_ROLE_TTL, Lease


class Spy:
    ON = ["Catalog_X"]
    name = "spy"

    def __init__(self):
        self.calls = []

    def handle(self, context):
        self.calls.append(context)


def _lease(db, owner: str) -> Lease:
    table = exchange_nodes_table(MetaData(), db.schema)
    create_table_if_absent(db.engine, table)
    lease = Lease(db.engine, lambda: table, owner, subject='test',
                  key_field=NODE_KEY_FIELD, owner_field=NODE_OWNER_FIELD,
                  heartbeat_field=NODE_HEARTBEAT_FIELD)
    lease.ensure_row('node')
    return lease


# --- Сам механизм аренды ---

def test_only_one_of_many_wins(db):
    # Восемь потоков одновременно — победитель обязан быть ровно один. Это то свойство, ради
    # которого захват сделан одним UPDATE: SELECT с последующей записью, даже в одной транзакции,
    # пропускает нескольких.
    leases = [_lease(db, f'p{i}') for i in range(8)]
    barrier = threading.Barrier(len(leases))
    winners = []

    def run(lease):
        barrier.wait()
        if lease.acquire('node'):
            winners.append(lease.owner)

    threads = [threading.Thread(target=run, args=(lease,)) for lease in leases]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    for lease in leases:
        lease.close()

    assert len(winners) == 1, f'предмет достался нескольким: {winners}'


def test_the_holder_can_reaffirm_its_own_lease(db):
    # Держатель переподтверждает аренду на каждой итерации: без этой ветки в предикате он терял бы
    # предмет сам у себя на втором же проходе.
    mine = _lease(db, 'mine')
    try:
        assert mine.acquire('node')
        assert mine.acquire('node')
        assert mine.acquire('node')
    finally:
        mine.close()


def test_a_dead_holder_releases_the_subject(db):
    first, second = _lease(db, 'first'), _lease(db, 'second')
    try:
        assert first.acquire('node')
        assert not second.acquire('node'), 'живого владельца перебивать нельзя'

        first.close()       # процесс исчез — отметку живости больше никто не продлевает
        with db.engine.begin() as conn:
            conn.execute(first.table.update().values(
                **{NODE_HEARTBEAT_FIELD: text(
                    f"now() - interval '{LEASE_ROLE_TTL + 60} seconds'")}))

        assert second.acquire('node'), 'брошенный предмет должен достаться следующему'
    finally:
        second.close()


def test_a_graceful_release_does_not_make_the_next_one_wait(db):
    first, second = _lease(db, 'first'), _lease(db, 'second')
    try:
        assert first.acquire('node')
        first.release('node')

        assert second.acquire('node'), 'после штатного ухода ждать TTL незачем'
    finally:
        first.close()
        second.close()


def test_still_mine_tells_a_takeover_apart(db):
    mine, other = _lease(db, 'mine'), _lease(db, 'other')
    try:
        assert mine.acquire('node')
        assert mine.still_mine('node')

        with db.engine.begin() as conn:
            conn.execute(mine.table.update().values(**{NODE_OWNER_FIELD: 'other'}))

        assert not mine.still_mine('node'), \
            'перехват обязан быть виден до необратимого действия, а не после'
    finally:
        mine.close()
        other.close()


# --- Узел обмена ---

def test_the_second_replicator_does_not_read_the_same_node(db):
    with fake_1c.running_server(Path(__file__).parent / "responses" / "trade_demo_8.5") as (
            url, fake):
        def make():
            return Replicator(odata_url=url, odata_auth=None, exchange_name="ДляODATA",
                              queue_guid=fake.queue_guid, engine=db.engine, db_schema=db.schema)

        first, second = make(), make()
        # Первый держит узел, пока идёт его цикл; второй в это время обязан пропустить свой.
        assert first._claim_node()
        try:
            assert not second._claim_node()
        finally:
            first._node_lease.release(fake.queue_guid)

        # Цикл первого закончился — узел свободен, и следующий цикл берёт тот, кто успел.
        assert second._claim_node()
        second._node_lease.release(fake.queue_guid)


def test_a_package_is_not_confirmed_after_a_takeover(db, monkeypatch):
    """
    Подтверждение необратимо: 1С удаляет по нему регистрации изменений, и вернуть их нечем.
    Поэтому перед ним аренда проверяется — процесс, которого сочли мёртвым, пока он считал пакет,
    подтверждать не вправе.
    """
    with fake_1c.running_server(Path(__file__).parent / "responses" / "trade_demo_8.5") as (
            url, fake):
        repl = Replicator(odata_url=url, odata_auth=None, exchange_name="ДляODATA",
                          queue_guid=fake.queue_guid, engine=db.engine, db_schema=db.schema)
        # Узел перехватили, пока пакет сохранялся.
        monkeypatch.setattr(repl._node_lease, 'still_mine', lambda *a, **kw: False)

        repl.run_once()

        assert fake.received_no == 0, 'пакет подтверждён процессом, потерявшим узел'


def test_a_package_is_confirmed_while_the_node_is_ours(db):
    with fake_1c.running_server(Path(__file__).parent / "responses" / "trade_demo_8.5") as (
            url, fake):
        repl = Replicator(odata_url=url, odata_auth=None, exchange_name="ДляODATA",
                          queue_guid=fake.queue_guid, engine=db.engine, db_schema=db.schema)

        repl.run_once()

        assert fake.received_no == 1


# --- Имя обработчика ---

def test_the_second_process_does_not_run_the_same_handler(db):
    first_spy, second_spy = Spy(), Spy()
    first = HandlerLoop(db.engine, db.schema, first_spy)
    second = HandlerLoop(db.engine, db.schema, second_spy)
    try:
        first.run_if_pending()
        second.run_if_pending()

        assert len(first_spy.calls) == 1
        assert second_spy.calls == [], 'витрину одного обработчика считают два процесса'
    finally:
        first.close()
        second.close()


def test_the_name_passes_to_the_next_process_after_a_graceful_stop(db):
    first_spy, second_spy = Spy(), Spy()
    first = HandlerLoop(db.engine, db.schema, first_spy)
    second = HandlerLoop(db.engine, db.schema, second_spy)
    try:
        first.run_if_pending()
        first.close()

        second.run_if_pending()

        assert second_spy.calls, 'сменщик не подхватил имя после штатного ухода'
    finally:
        second.close()


def test_the_name_passes_after_the_holder_stops_breathing(db):
    first_spy, second_spy = Spy(), Spy()
    first = HandlerLoop(db.engine, db.schema, first_spy)
    second = HandlerLoop(db.engine, db.schema, second_spy)
    try:
        first.run_if_pending()
        first._lease.close()
        with db.engine.begin() as conn:
            conn.execute(first.table.update().values(**{
                # Имя переписываем на постороннего владельца, а не просто состариваем отметку:
                # поток отметки живости предшественника продлевает строки условием
                # `owner = свой`, и, останови мы его хоть трижды, он мог бы оказаться уже внутри
                # запроса и освежить строку после нас. С чужим владельцем он не найдёт ничего.
                LEASE_OWNER_FIELD: 'ghost',
                LEASE_HEARTBEAT_FIELD: text(
                    f"now() - interval '{LEASE_ROLE_TTL + 60} seconds'"),
                # Отматываем и окно: предшественник записал last_run_at мгновение назад, и у
                # сменщика оно оказалось бы пустым — проверка зависела бы от микросекунд, а не от
                # передачи имени. В бою между этими событиями проходит не меньше TTL.
                'last_run_at': text("now() - interval '1 hour'"),
            }))

        second.run_if_pending()

        assert second_spy.calls, 'брошенное имя должно достаться следующему'
    finally:
        second.close()


@pytest.mark.parametrize("level", ["REPEATABLE_READ", "SERIALIZABLE"])
def test_a_stricter_isolation_level_does_not_break_the_claim(db, level):
    """
    На уровнях выше READ COMMITTED проигравший в гонке получает не rowcount=0, а ошибку
    сериализации. Уровень задаёт пользователь, создавая engine, поэтому такая ошибка обязана
    читаться как проигрыш, а не как авария.
    """
    from sqlalchemy import create_engine

    from conftest import TEST_DB_URL

    engine = create_engine(TEST_DB_URL, isolation_level=level)
    table = exchange_nodes_table(MetaData(), db.schema)
    create_table_if_absent(db.engine, table)
    leases = [Lease(engine, lambda: table, f'p{i}', subject='test',
                    key_field=NODE_KEY_FIELD, owner_field=NODE_OWNER_FIELD,
                    heartbeat_field=NODE_HEARTBEAT_FIELD) for i in range(4)]
    leases[0].ensure_row('node')
    barrier = threading.Barrier(len(leases))
    winners, errors = [], []

    def run(lease):
        barrier.wait()
        try:
            if lease.acquire('node'):
                winners.append(lease.owner)
        except Exception as exc:      # noqa: BLE001 — ровно это и проверяем
            errors.append(type(exc).__name__)

    threads = [threading.Thread(target=run, args=(lease,)) for lease in leases]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    for lease in leases:
        lease.close()
    engine.dispose()

    assert len(winners) == 1
    assert errors == [], 'сбой сериализации должен читаться как проигрыш, а не падать'


# --- CDC-08: истечение отметки живости доказывает молчание, а не смерть ---

def test_the_role_ttl_is_long_enough_to_need_real_silence(db):
    # Аренда роли уходит по молчанию. При 90 секундах хватает ЧЕТЫРЁХ несостоявшихся продлений
    # подряд, при 900 — нужно сорок пять. Молчать четыре такта и работать дальше — бывает;
    # молчать сорок пять — почти противоречие.
    from onecdc.full_load_claim import CLAIM_HEARTBEAT_TTL
    from onecdc.lease import LEASE_HEARTBEAT_PERIOD, LEASE_ROLE_TTL

    assert LEASE_ROLE_TTL / LEASE_HEARTBEAT_PERIOD >= 30
    # А у объекта под выгрузкой срок остаётся коротким: это единица работы, а не роль, и упавшая
    # выгрузка должна освобождать объект быстро, иначе он застрянет в очереди без причины.
    assert CLAIM_HEARTBEAT_TTL < LEASE_ROLE_TTL


def test_confirming_a_package_cannot_outlive_the_lease():
    # Общий таймаут рассчитан на чтение пакета (15 минут на ответ, пакет 1С формирует долго).
    # Для подтверждения это опасно: запрос, провисевший дольше аренды, применится уже тогда,
    # когда узлом владеет другой — причём БЕЗ всякого замирания, просто от медленного ответа.
    from onecdc.change_reader import NOTIFY_TIMEOUT
    from onecdc.lease import LEASE_ROLE_TTL
    from onecdc.metadata_reader import DEFAULT_REQUEST_TIMEOUT

    assert max(NOTIFY_TIMEOUT) < LEASE_ROLE_TTL, 'подтверждение может пережить аренду'
    assert max(NOTIFY_TIMEOUT) < max(DEFAULT_REQUEST_TIMEOUT), 'таймаут подтверждения не сужен'


def test_the_node_lease_is_held_across_cycles_inside_the_loop(db):
    # ВНУТРИ run_forever аренда живёт через все циклы: отпускать её каждую минуту значило бы
    # устраивать новую гонку на ровном месте, а читатель мигал бы между процессами.
    with fake_1c.running_server(Path(__file__).parent / "responses" / "trade_demo_8.5") as (
            url, fake):
        first = Replicator(odata_url=url, odata_auth=None, exchange_name="ДляODATA",
                           queue_guid=fake.queue_guid, engine=db.engine, db_schema=db.schema)
        second = Replicator(odata_url=url, odata_auth=None, exchange_name="ДляODATA",
                            queue_guid=fake.queue_guid, engine=db.engine, db_schema=db.schema)
        try:
            first._keep_node_lease = True       # так его выставляет run_forever
            first.run_once()

            assert first._node_lease.still_mine(fake.queue_guid), 'узел отпущен между циклами'
            assert not second._claim_node(), 'сосед перехватил узел между циклами'

            first.close()
            assert second._claim_node(), 'после штатной остановки ждать TTL незачем'
        finally:
            second.close()


def test_a_lost_lease_after_confirming_is_reported(db, monkeypatch, caplog):
    # Предотвратить уже поздно, но потеря перестаёт быть молчаливой. Автоматически при этом
    # ничего не перевыгружаем: потеря аренды НЕ означает потери данных — перехвативший почти
    # наверняка сохранил всё сам, и часы работы 1С по такому подозрению несоразмерны.
    import logging

    with fake_1c.running_server(Path(__file__).parent / "responses" / "trade_demo_8.5") as (
            url, fake):
        repl = Replicator(odata_url=url, odata_auth=None, exchange_name="ДляODATA",
                          queue_guid=fake.queue_guid, engine=db.engine, db_schema=db.schema)
        try:
            # Проверок три: после чтения пакета, перед подтверждением и после него. Аренда
            # уходит ровно в последнем зазоре — том, который предотвратить уже нельзя.
            answers = iter([True, True, False])
            monkeypatch.setattr(repl._node_lease, 'still_mine',
                                lambda *a, **kw: next(answers, False))
            with caplog.at_level(logging.ERROR):
                repl.run_once()

            assert fake.received_no == 1, 'подтверждение всё же прошло — иначе нечего обнаруживать'
            errors = [r.message for r in caplog.records
                      if r.levelno >= logging.ERROR and 'taken over' in r.message]
            assert errors, 'перехват во время подтверждения остался незамеченным'
        finally:
            repl.close()


def test_a_standalone_run_once_releases_the_node(db):
    """
    Одиночный run_once — самостоятельный режим (в том числе по расписанию снаружи), и процесс
    после него завершается. Оставь он узел захваченным, следующий запуск через минуту молча
    ничего бы не сделал, и так все 15 минут TTL.
    """
    with fake_1c.running_server(Path(__file__).parent / "responses" / "trade_demo_8.5") as (
            url, fake):
        def make():
            return Replicator(odata_url=url, odata_auth=None, exchange_name="ДляODATA",
                              queue_guid=fake.queue_guid, engine=db.engine, db_schema=db.schema)

        first = make()
        first.run_once()

        # Другой процесс (следующий запуск по расписанию) обязан взять узел сразу.
        second = make()
        try:
            assert second._claim_node(), 'узел остался захваченным завершившимся процессом'
        finally:
            second.close()
