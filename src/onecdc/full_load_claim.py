"""
Захват объекта под полную выгрузку — межпроцессный, с отметкой живости.

Зачем. Полную выгрузку одного объекта могут начать сразу двое: фоновый воркер репликатора (по
full_load_is_required) и расписание (FullLoadCron). Данные от этого не портятся — у каждого снимка
свой full_load_started_at, и guard'ы DBWriter.save не дают устаревшему снимку затереть свежие
строки, — но 1С делает двойную работу, а она здесь самая дорогая. Множества в памяти процесса для
разведения мало: репликатор и расписание могут работать в РАЗНЫХ процессах (и контейнерах), а тогда
они друг о друге не знают ничего.

Где. Отдельной таблицы нет: захват живёт двумя колонками в onecdc_metadata_objects, где уже лежит всё
остальное состояние полной выгрузки объекта (full_load_is_required, last_full_load_dt, метрики).
Строка на объект там и так одна, поэтому захват — это один атомарный UPDATE вида
compare-and-swap: занять удаётся тому, чей UPDATE изменил строку, а разводит гонку сама СУБД.

Отметка живости. Захват переживает процесс — строка остаётся в БД, — поэтому нужен признак, что
владелец ещё жив: иначе упавшая выгрузка заблокировала бы объект навсегда. Отметку обновляет
отдельный поток, как у реестра незавершённых merge (см. write_tracker): сам прогон занят
страницами и продлевать захват между ними ему нечем.

Константы намеренно НЕ общие с write_tracker: там отметка живости у merge, здесь у захвата
выгрузки, и путать их в коде нельзя (MERGE_* против CLAIM_*).

Владелец уникален НА ЭКЗЕМПЛЯР репликатора — см. common_functions.instance_owner. Не на план
обмена: иначе репликатор и расписание, поднятые в разных контейнерах одного обмена, считались бы
одним владельцем и снимали бы захваты друг у друга, то есть ровно то, ради чего всё и делается.
Реестр незавершённых merge (write_tracker) когда-то был устроен именно так и терял на этом строки;
теперь оба механизма берут владельца из одного места.

Плата за уникальность: захват упавшего процесса снимает не он сам при рестарте, а истечение
CLAIM_HEARTBEAT_TTL — полторы минуты, после которых объект достаётся следующему желающему.
"""

import threading
from contextlib import contextmanager
from datetime import timedelta

from sqlalchemy import func, select, update
from sqlalchemy.engine import Engine

from onecdc.common_functions import DB_NOW_WITH_TIMEZONE, HEARTBEAT_JOIN_TIMEOUT
from onecdc.logging_config import get_logger

logger = get_logger(__name__)

# Как часто владелец продлевает свой захват и через сколько чужой захват считается брошенным.
# TTL с запасом больше периода: разовая задержка не должна выглядеть как смерть процесса.
CLAIM_HEARTBEAT_PERIOD = 20.0
CLAIM_HEARTBEAT_TTL = 90.0
# Пауза после НЕУДАЧНОЙ попытки продлить захват. Короче обычной: неудача — это, как правило,
# нехватка соединений в пуле (все заняты страницами выгрузки), и обычным периодом мы получили бы
# на весь TTL всего четыре попытки. Проверено: при исчерпанном пуле захват живого процесса
# доставался чужому через 90 секунд, и объект грузили двое сразу. Сплошное голодание пула дольше
# TTL это не лечит — там захват теряется честно, как и задумано.
CLAIM_HEARTBEAT_RETRY_PERIOD = 5.0

OWNER_FIELD = 'full_load_owner'
HEARTBEAT_FIELD = 'full_load_heartbeat_at'


class FullLoadClaim:
    """
    Захваты полной выгрузки одного процесса: занять, продлевать, отпустить.

    Таблицу отдаёт не конструктор, а callable: реестр создаётся первой синхронизацией метаданных
    (MetadataReader._sync_objects), то есть позже, чем строится репликатор.
    """

    def __init__(self, engine: Engine, table_provider, owner: str,
                 heartbeat_engine: Engine | None = None):
        self.engine = engine
        # Отметку живости шлём ОТДЕЛЬНЫМ пулом (см. lease.lease_engine). Самая частая причина
        # ложного истечения — не смерть процесса, а то, что потоку отметки не досталось
        # соединения: страницы выгрузки разобрали пул целиком. Это уже случалось, и здесь цена
        # выше всего — захват живого процесса достаётся чужому расписанию, и 1С делает ту же
        # работу дважды. Остальные запросы короткие и редкие, они остаются на общем пуле.
        self._heartbeat_engine = heartbeat_engine or engine
        self._table_provider = table_provider
        self.owner = owner
        self._lock = threading.Lock()
        self._held: set[str] = set()
        # Глубина вложенного захвата, своя у каждого потока (см. hold). Процессное множество
        # _held для этого не годится: по нему поток не отличить, а отличать надо.
        self._nesting = threading.local()
        self._closed = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None

    @property
    def table(self):
        return self._table_provider()

    def claim(self, object_full_name: str) -> bool:
        """
        Занимает объект: True — заняли, False — держит живой владелец.

        Одним UPDATE, без предварительного SELECT: между чтением и записью поместилась бы чужая
        попытка. Условие «свободен либо владелец не подаёт признаков жизни» проверяет сама СУБД,
        и изменить строку удастся ровно одному.
        """
        table = self.table
        if table is None:
            # Реестра нет — занимать негде, и честный ответ «не занял». Раньше возвращалось True:
            # заслон отвечал «занял», не заняв ничего, и два процесса заходили в блок вместе
            # (CDC-33). Отличать «нечем координироваться» от «занято» — дело вызывающего, см.
            # is_ready и Replicator.full_load.
            return False
        with self.engine.begin() as conn:
            now = conn.scalar(select(DB_NOW_WITH_TIMEZONE))
            result = conn.execute(
                update(table)
                .where(table.c.object_full_name == object_full_name,
                       (table.c[OWNER_FIELD].is_(None))
                       | (table.c[HEARTBEAT_FIELD]
                          < now - timedelta(seconds=CLAIM_HEARTBEAT_TTL)))
                .values(**{OWNER_FIELD: self.owner, HEARTBEAT_FIELD: now}))
        if result.rowcount == 0:
            with self.engine.connect() as conn:
                known = conn.scalar(select(table.c.object_full_name)
                                    .where(table.c.object_full_name == object_full_name))
            if known is None:
                # Не «занято»: объекта нет в реестре. Ошибка в имени сюда не доходит (его
                # разрешают раньше), значит реестр не синхронизирован с метаданными.
                logger.error("Cannot claim %s for full load: it is not in the object registry",
                             object_full_name)
            return False
        with self._lock:
            self._held.add(object_full_name)
            self._start_heartbeat()
        return True

    def live_claims(self) -> set[str]:
        """
        Объекты, которые СЕЙЧАС кто-то выгружает целиком: захват стоит и его владелец подаёт
        признаки жизни.

        Нужно пишущему изменения: пока по объекту идёт снимок, пакет обязан оставлять след даже
        когда значения не поменялись (см. Replicator._skip_compare_allowed). Иначе снимок,
        прочитавший страницу до пакета, запишет устаревшее значение поверх подтверждённого —
        guard проверяет merged_on, а его no-op пакет не двигает.

        Живость обязательна: без неё упавшая посреди прогона выгрузка оставила бы объект в этом
        режиме навсегда. Отсекается тем же CLAIM_HEARTBEAT_TTL, которым claim() перехватывает
        брошенный захват, — одно правило на оба случая.

        Один запрос на пакет к реестру объектов, а не на каждый объект пакета.
        """
        table = self.table
        if table is None:
            return set()
        with self.engine.connect() as conn:
            now = conn.scalar(select(DB_NOW_WITH_TIMEZONE))
            rows = conn.execute(
                select(table.c.object_full_name)
                .where(table.c[OWNER_FIELD].is_not(None),
                       table.c[HEARTBEAT_FIELD]
                       >= now - timedelta(seconds=CLAIM_HEARTBEAT_TTL))).scalars().all()
        return set(rows)

    @property
    def is_ready(self) -> bool:
        """
        Есть ли чем координироваться: реестр объектов существует.

        Нужно затем, что «не занял» бывает по двум разным причинам. Объект держит живой владелец —
        работу дублировать нельзя. Реестра нет вовсе — значит его нет и у остальных, захватить
        объект не мог никто, и отказываться от выгрузки не из-за чего.
        """
        return self.table is not None

    @contextmanager
    def hold(self, object_full_name: str):
        """
        Захват на время блока: `with claim.hold(name) as claimed:`. Отпускает сам, и только если
        сам же и занял.

        Повторный вход В ТОМ ЖЕ ПОТОКЕ проходит без нового UPDATE — иначе вложенный вызов не смог
        бы занять объект у самого себя (условие «свободен или владелец молчит» ложно, владелец —
        мы же) и молча пропустил бы работу. Считаем именно по потоку, а не по процессу: пока один
        поток держит объект, другой поток того же процесса обязан получить отказ, как и чужой
        процесс, — и получает его от БД, которая тут единственный арбитр.
        """
        depth = getattr(self._nesting, 'held', None)
        if depth is None:
            depth = self._nesting.held = {}
        if depth.get(object_full_name):
            depth[object_full_name] += 1
            try:
                yield True
            finally:
                depth[object_full_name] -= 1
            return

        claimed = self.claim(object_full_name)
        if claimed:
            depth[object_full_name] = 1
        try:
            yield claimed
        finally:
            if claimed:
                depth.pop(object_full_name, None)
                self.release(object_full_name)

    def held_objects(self) -> set[str]:
        """Объекты, которые этот процесс сейчас держит. Снимок — множество меняется из потоков."""
        with self._lock:
            return set(self._held)

    def release(self, object_full_name: str) -> None:
        """Отпускает захват — только свой: чужой мог перехватить объект после нашего TTL."""
        with self._lock:
            self._held.discard(object_full_name)
        table = self.table
        if table is None:
            return
        with self.engine.begin() as conn:
            conn.execute(update(table)
                         .where(table.c.object_full_name == object_full_name,
                                table.c[OWNER_FIELD] == self.owner)
                         .values(**{OWNER_FIELD: None, HEARTBEAT_FIELD: None}))

    def heartbeat(self) -> None:
        """Продлевает свои захваты. Зовётся своим же потоком, пока есть что держать."""
        table = self.table
        if table is None:
            return
        with self._heartbeat_engine.begin() as conn:
            conn.execute(update(table).where(table.c[OWNER_FIELD] == self.owner)
                         .values(**{HEARTBEAT_FIELD: func.now()}))

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
            target=self._heartbeat_loop, name=f'claim-heartbeat:{self.owner}', daemon=True)
        self._heartbeat_thread.start()

    def _heartbeat_loop(self) -> None:
        """Продлевает захваты, пока они есть, и гаснет, когда их не осталось. Решение погаснуть
        принимается под тем же локом, под которым claim() поток поднимает, — промежутка, в котором
        захват уже взят, а поток уже вышел, не существует."""
        while not self._closed.is_set():
            try:
                self.heartbeat()
                delay = CLAIM_HEARTBEAT_PERIOD
            except Exception:
                # Чаще всего это нехватка соединений в пуле — повторяем скорее, чтобы давка на
                # пул не стоила захвата (см. CLAIM_HEARTBEAT_RETRY_PERIOD).
                logger.exception("Full load claim heartbeat failed, retrying in %ss",
                                 CLAIM_HEARTBEAT_RETRY_PERIOD)
                delay = CLAIM_HEARTBEAT_RETRY_PERIOD
            self._closed.wait(delay)
            with self._lock:
                if not self._held:
                    self._heartbeat_thread = None
                    return
