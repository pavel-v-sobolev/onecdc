"""
Аренда ролей через владельца и отметку активности в PostgreSQL.

Захват выполняется условным UPDATE: строка должна быть свободна, принадлежать тому
же владельцу или иметь просроченную отметку. Проверка и изменение одним оператором
не позволяют двум конкурирующим процессам одновременно получить свободную роль.

Lease обслуживает узлы обмена и имена обработчиков. В непрерывных циклах владелец
удерживает роль между итерациями; другой процесс пропускает работу до освобождения
или истечения TTL. Самостоятельный run_once освобождает узел в конце вызова.

Отметку продлевает отдельный поток. Истечение TTL не доказывает остановку процесса:
долгая пауза или потеря связи также могут привести к передаче роли. Поэтому вызывающий
проверяет владение перед критическими действиями; ограничения описаны в DESIGN.md.
"""

import threading
from datetime import timedelta

from sqlalchemy import create_engine, func, insert, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError, IntegrityError

from onecdc.common_functions import DB_NOW_WITH_TIMEZONE, HEARTBEAT_JOIN_TIMEOUT
from onecdc.logging_config import get_logger

logger = get_logger(__name__)

# Как часто владелец продлевает аренду.
LEASE_HEARTBEAT_PERIOD = 20.0
# Насколько должна протухнуть ЧУЖАЯ отметка живости, чтобы аренду разрешили перехватить. Это
# единственное применение срока: освобождения по таймеру нет, строка висит с владельцем, пока
# кто-то не заберёт её условием в claim() — см. предикат захвата ниже.
#
# Пятнадцать минут против полутора у захвата объекта и у реестра записей (CLAIM_HEARTBEAT_TTL,
# MERGE_HEARTBEAT_TTL). Разница не в аккуратности, а в том, что роль и единица работы ошибаются
# по-разному:
#
#   роль (узел обмена, имя обработчика) — не захватил, так и делать нечего, остаётся ждать.
#       Ложный перехват дорог: два читателя одного узла обмена. Простой безвреден: изменения
#       копятся в 1С, витрина отстаёт. Значит срок длинный;
#   единица работы (объект под выгрузкой) — объектов много, и застрявший зря держит очередь.
#       Значит срок короткий.
#
# Длинный срок нужен потому, что молчание не доказывает смерть: процесс мог остаться без
# соединения в пуле, замереть в паузе контейнера, попасть в своп. При продлении раз в
# LEASE_HEARTBEAT_PERIOD (20 с) полутора минут хватает четырёх несостоявшихся продлений подряд,
# а пятнадцати — сорока пяти. Молчать четыре такта и работать дальше — бывает; молчать сорок
# пять и работать — почти противоречие.
#
# Что именно это даёт. ШИРИНУ опасного окна — промежутка, в котором потерявший аренду ещё успеет
# сделать необратимое, — срок не меняет: окно закрывает следующая проверка владения (перед
# подтверждением пакета, условием lease_is_mine внутри оператора записи), а не истечение TTL.
# Длинный срок делает редким само ПОПАДАНИЕ в это окно.
#
# Плата ровно одна: после настоящей смерти роль простаивает до 15 минут — задержка, не потеря.
# Отсюда же важность штатной остановки: по SIGTERM аренда отпускается сразу и сменщик подхватывает
# мгновенно, а после kill -9 ждать придётся весь срок.
LEASE_ROLE_TTL = 900.0
# Пауза после неудачной попытки продлить аренду. Короче обычной: неудача — это, как правило,
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
    Получить общий для адреса БД пул отметок активности с пределом 2 + 2 соединения.

    Отдельный пул не позволяет рабочим запросам занять все соединения, необходимые
    для продления аренды. Он не устраняет задержки самой БД или сети. Если создать пул
    не удалось, возвращается исходный engine и записывается предупреждение.
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
                now = conn.scalar(select(DB_NOW_WITH_TIMEZONE))
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
        Проверить владельца и продлить аренду одним UPDATE; вернуть результат проверки.

        При переданном conn используется его транзакция, иначе открывается собственная.
        UPDATE держит блокировку строки до завершения транзакции, поэтому передаваемая
        транзакция должна быть короткой: длительная блокировка задержит поток продления.
        Проверка не делает внешнее действие атомарным с транзакцией PostgreSQL.
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
            now = conn.scalar(select(DB_NOW_WITH_TIMEZONE))
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
        Остановить поток отметки активности и ждать не более HEARTBEAT_JOIN_TIMEOUT.

        Установленный флаг прерывает ожидание между запросами. Уже выполняющийся запрос
        не отменяется и при задержке может завершиться после возврата close().
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
