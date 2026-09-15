"""
Аренда с отметкой живости: «этим сейчас занимается ровно один процесс».

Общий механизм на три разных предмета — объект под полной выгрузкой (full_load_claim), имя
обработчика и узел обмена. Предметы разные, а устройство одно: строка с владельцем и отметкой
живости, захват атомарным CAS-обновлением, освобождение по истечении отметки.

**Захват — ОДИН `UPDATE` с предикатом внутри, а не чтение с последующей записью.** Разница здесь
не стилистическая, она решает корректность. Два конкурирующих `UPDATE` одной строки СУБД не
выполняет параллельно: второй блокируется на строке, а после коммита первого перепроверяет свой
`WHERE` против НОВОЙ версии строки — владелец там уже стоит, условие ложно, `rowcount = 0`.
Арбитраж делает сама СУБД, и обойти его нечем.

Замерено на PostgreSQL, 8 потоков в 20 гонках:

    один UPDATE с предикатом            победитель ровно один, всегда
    SELECT + UPDATE в одной транзакции   пять победителей из восьми
    SELECT ... FOR UPDATE + UPDATE       победитель один

Средний вариант — распространённое заблуждение: транзакция даёт атомарность и изоляцию от
частичного состояния, но НЕ даёт взаимного исключения, и классическая потеря обновления помещается
внутрь неё целиком. Нижний корректен, но это два запроса и более долгая блокировка ради того же
результата.

**Симметрия процессов.** Роли ведущего нет: процессы одинаковы, на каждой итерации идёт честная
гонка, и кто её выиграет в следующий раз, заранее не определено. Не захвативший предмет не ждёт
освобождения в цикле, а пропускает итерацию и пробует снова — так поднятый рядом второй процесс
не дублирует работу, ничего не требует от пользователя и подхватывает дело, когда сосед исчезнет.
"""

import threading
from datetime import timedelta

from sqlalchemy import create_engine, func, insert, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError, IntegrityError

from onecdc.common_functions import DB_NOW_WITHOUT_TIMEZONE, HEARTBEAT_JOIN_TIMEOUT
from onecdc.logging_config import get_logger

logger = get_logger(__name__)

# Как часто владелец продлевает аренду.
LEASE_HEARTBEAT_PERIOD = 20.0
# Через сколько чужая аренда считается брошенной — у РОЛИ (узел обмена, имя обработчика).
#
# Пятнадцать минут, а не полторы. Аренда уходит по молчанию, а молчание не доказывает смерть:
# процесс мог остаться без соединения в пуле, замереть в паузе контейнера, попасть в своп. При
# 90 секундах хватает ЧЕТЫРЁХ несостоявшихся продлений подряд, при 900 — нужно сорок пять. Молчать
# четыре такта и продолжать работать — бывает; молчать сорок пять и продолжать — почти
# противоречие. Именно это и сужает окно, в котором потерявший аренду успевает сделать
# необратимое (см. Replicator.run_once, HandlerLoop.run_if_pending).
#
# Плата ровно одна: после НАСТОЯЩЕЙ смерти роль простаивает до 15 минут. Потери при этом нет —
# изменения копятся в 1С, витрина отстаёт, — только задержка. Отсюда же важность штатной
# остановки: по SIGTERM аренда отпускается сразу, и сменщик подхватывает мгновенно, а после
# kill -9 ждать придётся весь TTL.
#
# У ЕДИНИЦЫ РАБОТЫ (объект под полной выгрузкой) срок свой и короткий — см. full_load_claim:
# упавшая выгрузка должна освобождать объект быстро, иначе он застрянет в очереди без причины.
LEASE_ROLE_TTL = 900.0
# Пауза после НЕУДАЧНОЙ попытки продлить аренду. Короче обычной: неудача — это, как правило,
# нехватка соединений в пуле, и обычным периодом мы получили бы на весь TTL всего четыре попытки.
LEASE_HEARTBEAT_RETRY_PERIOD = 5.0

# Коды PostgreSQL: сбой сериализации и взаимоблокировка. На уровнях изоляции выше READ COMMITTED
# проигравший в гонке получает не rowcount=0, а исключение (проверено: на REPEATABLE READ и
# SERIALIZABLE победитель по-прежнему один, но остальные трое падают). Уровень изоляции задаёт
# пользователь, создавая engine, поэтому трактуем такую ошибку как проигрыш, а не как аварию.
_RACE_LOST_SQLSTATES = ('40001', '40P01')

# Пулы под аренды — по одному на адрес БД (см. lease_engine). Живут столько же, сколько процесс:
# аренды берутся и отпускаются постоянно, а соединения переоткрывать на каждую незачем.
_LEASE_ENGINES: dict[str, Engine] = {}
_LEASE_ENGINES_LOCK = threading.Lock()


def _is_race_lost(exc: BaseException) -> bool:
    code = getattr(getattr(exc, 'orig', None), 'pgcode', None)
    return code in _RACE_LOST_SQLSTATES


def lease_engine(engine: Engine) -> Engine:
    """
    Отдельный маленький пул под аренды, из того же адреса БД.

    Самая частая причина ложного истечения аренды — не смерть процесса, а то, что потоку отметки
    живости НЕ ДОСТАЛОСЬ СОЕДИНЕНИЯ: страницы полной выгрузки разобрали пул целиком. В проекте
    это уже случалось и задокументировано (см. full_load_claim, CLAIM_HEARTBEAT_RETRY_PERIOD):
    «при исчерпанном пуле захват живого процесса доставался чужому через 90 секунд, и объект
    грузили двое сразу».

    Отдельный пул делает это невозможным: рабочие потоки физически не могут обесточить аренды.
    Цена — одно дополнительное соединение на процесс, и это несопоставимо дешевле последствий.

    Пул ОДИН НА АДРЕС БД, а не на аренду: аренд в процессе несколько (узел обмена, каждый
    обработчик), и по своему пулу на каждую означало бы пригоршню лишних соединений вместо пары.
    Отметки живости короткие и редкие (раз в LEASE_HEARTBEAT_PERIOD), так что общий маленький пул
    их спокойно вмещает.

    Не получилось (адрес не разбирается, драйвер не тот) — работаем на общем engine: хуже, чем
    было, от этого не станет.
    """
    key = str(engine.url)
    with _LEASE_ENGINES_LOCK:
        cached = _LEASE_ENGINES.get(key)
        if cached is not None:
            return cached
        try:
            created = create_engine(engine.url, pool_size=2, max_overflow=2, pool_pre_ping=True)
        except Exception:
            logger.warning("Could not create a separate connection pool for leases, using the "
                           "shared one — a heartbeat may then lose its lease to pool exhaustion",
                           exc_info=True)
            return engine
        _LEASE_ENGINES[key] = created
        return created


class Lease:
    """
    Аренда предметов одного процесса: занять, продлевать, проверять, отпустить.

    Таблицу отдаёт не конструктор, а callable: у части предметов она создаётся позже, чем
    строится владелец аренды (реестр объектов появляется с первой синхронизацией метаданных).
    """

    def __init__(self, engine: Engine, table_provider, owner: str, *,
                 key_field: str, owner_field: str, heartbeat_field: str,
                 subject: str = 'lease',
                 period: float = LEASE_HEARTBEAT_PERIOD,
                 ttl: float = LEASE_ROLE_TTL,
                 retry_period: float = LEASE_HEARTBEAT_RETRY_PERIOD):
        self.engine = engine
        self._table_provider = table_provider
        self.owner = owner
        self.key_field = key_field
        self.owner_field = owner_field
        self.heartbeat_field = heartbeat_field
        self.subject = subject
        self.period = period
        self.ttl = ttl
        self.retry_period = retry_period
        self._lock = threading.Lock()
        self._held: set[str] = set()
        self._closed = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None

    @property
    def table(self):
        return self._table_provider()

    def ensure_row(self, key: str, **extra) -> None:
        """
        Заводит строку предмета, если её ещё нет. Идемпотентно: гонку разводит первичный ключ.

        Нужно потому, что захват — это `UPDATE`, а он по отсутствующей строке даёт `rowcount = 0`,
        то есть «занято», хотя на деле обновлять нечего.
        """
        table = self.table
        if table is None:
            return
        try:
            with self.engine.begin() as conn:
                conn.execute(insert(table).values(**{self.key_field: key}, **extra))
        except IntegrityError:
            pass        # завёл кто-то другой — это и требовалось

    def acquire(self, key: str) -> bool:
        """
        Занимает предмет: True — заняли, False — держит живой владелец.

        Одним `UPDATE`, без предварительного `SELECT` (см. модульную docstring). Условие «свободен
        либо владелец не подаёт признаков жизни» проверяет сама СУБД.
        """
        table = self.table
        if table is None:
            return False
        try:
            with self.engine.begin() as conn:
                now = conn.scalar(select(DB_NOW_WITHOUT_TIMEZONE))
                result = conn.execute(
                    update(table)
                    .where(table.c[self.key_field] == key,
                           # Свободен, наш собственный, либо владелец не подаёт признаков жизни.
                           # Своя аренда в условии обязательна: держатель переподтверждает её на
                           # каждой итерации, и без этой ветки он терял бы предмет сам у себя.
                           table.c[self.owner_field].is_(None)
                           | (table.c[self.owner_field] == self.owner)
                           | (table.c[self.heartbeat_field] < now - timedelta(seconds=self.ttl)))
                    .values(**{self.owner_field: self.owner, self.heartbeat_field: now}))
        except DBAPIError as exc:
            if not _is_race_lost(exc):
                raise
            return False
        if result.rowcount == 0:
            return False
        with self._lock:
            self._held.add(key)
            self._start_heartbeat()
        return True

    def still_mine(self, key: str, conn=None) -> bool:
        """
        Аренда всё ещё наша? Проверка И продление одним запросом.

        Зовётся перед необратимым действием — тем, которое нельзя отменить, узнав постфактум, что
        аренда ушла. У читателя изменений это подтверждение пакета (оно удаляет регистрации в 1С),
        и там же передаётся `conn`: проверка обязана лечь в ОДНУ транзакцию с самим действием,
        иначе между ними снова помещается перехват.

        Продление, а не только проверка: если мы дожили до этого места, мы живы — и незачем
        оставлять предмет на грани TTL ровно в тот момент, когда делаем самое важное.

        ВНИМАНИЕ к передаваемому conn: транзакция, в которой идёт эта проверка, обязана быть
        КОРОТКОЙ. Запрос берёт блокировку на строке аренды и держит её до коммита, а ту же строку
        обновляет поток отметки живости — на длинной транзакции он встанет на этой блокировке, и
        процесс лишит аренды сам себя, причём тем вернее, чем дольше транзакция. Передавать сюда
        соединение пакетного merge или чего-то подобного нельзя.
        """
        table = self.table
        if table is None:
            return False
        statement = (update(table)
                     .where(table.c[self.key_field] == key,
                            table.c[self.owner_field] == self.owner)
                     .values(**{self.heartbeat_field: func.now()}))
        if conn is not None:
            return conn.execute(statement).rowcount == 1
        with self.engine.begin() as own:
            return own.execute(statement).rowcount == 1

    def live_owners(self) -> dict[str, str]:
        """{предмет: владелец} по всем занятым сейчас — владелец есть и подаёт признаки жизни."""
        table = self.table
        if table is None:
            return {}
        with self.engine.connect() as conn:
            now = conn.scalar(select(DB_NOW_WITHOUT_TIMEZONE))
            rows = conn.execute(
                select(table.c[self.key_field], table.c[self.owner_field])
                .where(table.c[self.owner_field].is_not(None),
                       table.c[self.heartbeat_field] >= now - timedelta(seconds=self.ttl))).all()
        return {row[0]: row[1] for row in rows}

    def release(self, key: str) -> None:
        """Отпускает аренду — только свою: чужой мог перехватить предмет после нашего TTL."""
        with self._lock:
            self._held.discard(key)
        table = self.table
        if table is None:
            return
        with self.engine.begin() as conn:
            conn.execute(update(table)
                         .where(table.c[self.key_field] == key,
                                table.c[self.owner_field] == self.owner)
                         .values(**{self.owner_field: None, self.heartbeat_field: None}))

    def heartbeat(self) -> None:
        """Продлевает свои аренды. Зовётся своим же потоком, пока есть что держать."""
        table = self.table
        if table is None:
            return
        with self._lock:
            held = list(self._held)
        if not held:
            return
        with self.engine.begin() as conn:
            conn.execute(update(table)
                         .where(table.c[self.key_field].in_(held),
                                table.c[self.owner_field] == self.owner)
                         .values(**{self.heartbeat_field: func.now()}))

    def close(self) -> None:
        """
        Останавливает поток отметки живости и ДОЖИДАЕТСЯ его.

        Именно дожидается: одного флага мало. Поток мог уже пройти проверку флага и стоять внутри
        UPDATE — тогда close() возвращается, а запись ложится в таблицу ПОСЛЕ него, и строка,
        которую вызывающий считает отпущенной, оказывается свежей. Отсюда же «relation does not
        exist» в логе от потока, чью схему успели снести.

        Ожидание ограничено HEARTBEAT_JOIN_TIMEOUT: поток стоит на ожидании флага, который close()
        будит сразу, так что в худшем случае это один уже начатый запрос.
        """
        self._closed.set()
        thread = self._heartbeat_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=HEARTBEAT_JOIN_TIMEOUT)

    def _start_heartbeat(self) -> None:
        """Поднимает поток отметки живости, если его ещё нет. Зовётся под self._lock."""
        if self._heartbeat_thread is not None or self._closed.is_set():
            return
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, name=f'lease:{self.subject}:{self.owner}', daemon=True)
        self._heartbeat_thread.start()

    def _heartbeat_loop(self) -> None:
        """Продлевает аренды, пока они есть, и гаснет, когда их не осталось. Решение погаснуть
        принимается под тем же локом, под которым acquire() поток поднимает, — промежутка, в
        котором аренда уже взята, а поток уже вышел, не существует."""
        while not self._closed.is_set():
            try:
                self.heartbeat()
                delay = self.period
            except Exception:
                # Чаще всего это нехватка соединений в пуле — повторяем скорее, чтобы давка на
                # пул не стоила аренды (см. LEASE_HEARTBEAT_RETRY_PERIOD).
                logger.exception("%s lease heartbeat failed, retrying in %ss",
                                 self.subject, self.retry_period)
                delay = self.retry_period
            self._closed.wait(delay)
            with self._lock:
                if not self._held:
                    self._heartbeat_thread = None
                    return
