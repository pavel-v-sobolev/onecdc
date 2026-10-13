"""
Межпроцессный захват объекта на время полной выгрузки.

Фоновая выгрузка и FullLoadCron могут одновременно выбрать один объект. Захват не даёт
им выполнять одну работу параллельно, в том числе в разных процессах и контейнерах.
Состояние хранится в full_load_owner и full_load_heartbeat_at таблицы
onecdc_metadata_objects. Занять свободный или просроченный захват позволяет атомарный UPDATE.

Владельца идентифицирует уникальная метка экземпляра Replicator из instance_owner.
Отдельный поток обновляет отметку живости каждые 20 секунд. Если отметка не обновлялась
90 секунд, другой экземпляр может занять объект. Это проверка по времени, а не доказательство
завершения прежнего процесса; при потере захвата прогон прекращается на ближайшей проверке.

Константы CLAIM_* относятся к захвату полной выгрузки. Реестр незавершённых записей
write_tracker использует собственные константы MERGE_* и независимое состояние.
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
# Пауза после неудачной попытки продлить захват. Короче обычной: неудача — это, как правило,
# нехватка соединений в пуле (все заняты страницами выгрузки), и обычным периодом мы получили бы
# на весь TTL всего четыре попытки. Проверено: при исчерпанном пуле захват живого процесса
# доставался чужому через 90 секунд, и объект грузили двое сразу. Сплошное голодание пула дольше
# TTL это не лечит — там захват теряется честно, как и задумано.
CLAIM_HEARTBEAT_RETRY_PERIOD = 5.0

OWNER_FIELD = 'full_load_owner'
HEARTBEAT_FIELD = 'full_load_heartbeat_at'


class FullLoadClaim:
    """
    Захваты полной выгрузки, принадлежащие одному экземпляру Replicator.

    Таблица передаётся через вызываемый объект table_provider, поскольку реестр создаётся
    при первой синхронизации метаданных — позже, чем сам Replicator.
    """

    def __init__(self, engine: Engine, table_provider, owner: str,
                 heartbeat_engine: Engine | None = None):
        self.engine = engine
        # Отметку живости шлём отдельным пулом (см. lease.lease_engine). Самая частая причина
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
        Объекты, которые сейчас кто-то выгружает целиком: захват стоит и его владелец подаёт
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
        Проверить, существует ли реестр объектов для координации выгрузок.

        Отсутствие реестра отличается от занятого объекта. В первом случае захватить объект
        ещё не мог ни один процесс; во втором работу уже выполняет другой владелец.
        """
        return self.table is not None

    @contextmanager
    def hold(self, object_full_name: str):
        """
        Удерживать объект на время блока with claim.hold(name) as claimed.

        При выходе освобождается только захват, полученный этим блоком. Повторный вход из того же
        потока не выполняет новый UPDATE: вложенный вызов использует уже полученный захват.
        Другой поток, даже в том же процессе, должен занять объект через БД и получит отказ,
        пока прежний захват действует.
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
            target=self._heartbeat_loop, name=f'claim-heartbeat:{self.owner}', daemon=True)
        self._heartbeat_thread.start()

    def _heartbeat_loop(self) -> None:
        """
        Продлевать захваты, пока они есть, и завершить поток после освобождения последнего.

        Решение о завершении принимается под той же блокировкой, которую claim() использует
        при запуске потока. Это исключает ситуацию, когда новый захват остаётся без продления
        из-за одновременного завершения потока.
        """
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
