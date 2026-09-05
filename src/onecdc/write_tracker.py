"""
Реестр незавершённых merge: общий на процессы, живёт в таблице БД.

Отдельным модулем, потому что пользуются им обе стороны и по разным поводам. Обработчик прижимает
к нему верхнюю границу своего окна, репликатор — отметку страницы полной выгрузки: merged_on
штампуется ВНУТРИ merge-транзакции, а коммитится позже, поэтому «сейчас» перешагнуло бы строки,
которые уже помечены прошедшим временем, но ещё не видны. Обеим сторонам нужен один и тот же
ответ на вопрос «до какого момента данные точно устоялись».
"""

import itertools
import threading
import time
from datetime import datetime, timedelta
from typing import Iterable

from sqlalchemy import (Column, DateTime, Engine, MetaData, String, Table, delete, func, insert,
                        or_, select, update)

from cdc_1c.common_functions import DB_NOW_WITHOUT_TIMEZONE
from cdc_1c.db_logs import _check_create_schema
from cdc_1c.logging_config import get_logger

logger = get_logger(__name__)

# Реестр идущих merge (WriteTracker). Репликатор обновляет отметку живости своих строк
# не реже MERGE_HEARTBEAT_PERIOD; строки, чья отметка старше MERGE_HEARTBEAT_TTL, считаются
# брошенными —
# процесс, который их завёл, умер, а вместе с ним откатились и его транзакции, так что держать по
# ним границу больше не нужно. TTL с запасом больше периода: разовая задержка не должна выглядеть
# как смерть процесса.
WRITES_TABLE = "writes_in_process_1c"
MERGE_HEARTBEAT_PERIOD = 20.0
MERGE_HEARTBEAT_TTL = 90.0
# Через сколько строка брошенного процесса не просто игнорируется при расчёте границы, а удаляется.
# Нужно потому, что процесс может не вернуться никогда: репликатор переименовали (сменился
# exchange_name) или выключили совсем — его строки иначе остались бы в таблице навсегда. С запасом
# больше MERGE_HEARTBEAT_TTL: удалять то, что ещё держит границу, нельзя ни при каких
# обстоятельствах.
MERGE_ABANDONED_TTL = 3600.0


class _TrackedWrite:
    """Один merge в реестре: снимается по выходу из блока, т.е. после коммита. Ключ — имя объекта
    строки в writes_in_process_1c."""

    def __init__(self, tracker, key: str, started_at: datetime):
        self._tracker = tracker
        self._key = key
        self.started_at = started_at

    def __enter__(self) -> "_TrackedWrite":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._tracker._remove(self._key)


def _writes_table(metadata: MetaData, schema_name: str | None) -> Table:
    return Table(
        WRITES_TABLE, metadata,
        Column("id", String(64), primary_key=True),
        Column("owner", String(255), nullable=False),
        Column("object_name", String(255), nullable=False),
        # Момент старта merge по часам БД — то, к чему прижимается граница окна обработчика.
        Column("started_at", DateTime, nullable=False),
        # Отметка живости: обновляется, пока процесс жив (см. MERGE_HEARTBEAT_TTL).
        Column("heartbeat_at", DateTime, nullable=False),
        schema=schema_name,
    )


class WriteTracker:
    """
    Реестр незавершённых merge в БАЗЕ: строка живёт ровно столько, сколько данные merge могут быть
    ещё не видны другим процессам.

    Нужен, потому что обработчик может работать отдельно от репликатора. Граница его окна обязана
    быть прижата к незакоммиченным merge: их merged_on уже в прошлом, а строки ещё не видны, и
    отметка, взятая как «сейчас», их бы перешагнула — потеря строк, молча. В памяти такой реестр
    чужому процессу не виден, поэтому он живёт в таблице writes_in_process_1c: строка появляется
    перед merge и исчезает после коммита.

    Брошенные строки (процесс умер между вставкой и удалением) отсекаются по отметке живости: раз
    процесса нет, его транзакции откатились, и держать по ним границу незачем. Поэтому падение
    репликатора обработчиков не морозит — максимум на MERGE_HEARTBEAT_TTL.

    Отметку живости обновляет сам реестр, своим потоком. Именно реестр, а не цикл репликации:
    строки появляются в любом сценарии, включая одиночный run_once и вызванный руками full_load,
    а цикла в этих сценариях нет. Без этого одна страница выгрузки, считающаяся дольше
    MERGE_HEARTBEAT_TTL, признавалась бы брошенной — и обработчик молча перешагнул бы её строки.
    Поток
    отдельный ещё и потому, что merge и сам может идти дольше MERGE_HEARTBEAT_TTL: обновлять отметку
    между merge поздно.
    """

    def __init__(self, engine: Engine, schema: str | None, owner: str):
        self.engine = engine
        self.schema_name = _check_create_schema(engine, schema)
        self.owner = owner
        self.table = _writes_table(MetaData(), self.schema_name)
        self.table.create(engine, checkfirst=True)
        self._counter = itertools.count()
        self._lock = threading.Lock()
        # Сколько своих merge сейчас в реестре и поток, который обновляет им отметку живости.
        # Поток поднимается с первым merge и гаснет, когда продлевать становится нечего: реестры
        # живут долго, и оставлять при каждом по спящему потоку незачем.
        self._active = 0
        self._heartbeat_thread: threading.Thread | None = None
        self._closed = threading.Event()
        self._cleanup()

    def close(self) -> None:
        """Останавливает поток отметки живости. Для процесса не обязательна (поток daemon), нужна
        тестам и тем, кто заводит реестры по ходу работы."""
        self._closed.set()

    def _ensure_heartbeat(self) -> None:
        """Поднимает поток отметки живости, если он ещё не поднят. Под локом: проверка «поток есть»
        и его создание обязаны быть неделимыми, иначе два merge, стартовавших одновременно, заведут
        по потоку каждый."""
        with self._lock:
            if self._heartbeat_thread is not None or self._closed.is_set():
                return
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop, name=f'heartbeat:{self.owner}', daemon=True)
            self._heartbeat_thread.start()

    def _heartbeat_loop(self) -> None:
        """
        Продлевает свои строки, пока они есть, и гаснет, когда их не осталось.

        Решение «гаснуть» принимается под тем же локом, под которым track() поднимает поток, и
        ПОСЛЕ инкремента _active. Поэтому промежутка, в котором merge уже стартовал, а поток уже
        решил выйти, не существует: либо выходящий поток видит _active > 0 и остаётся, либо он
        успел обнулить _heartbeat_thread, и track() заводит новый.
        """
        while not self._closed.is_set():
            try:
                self.heartbeat()
            except Exception:
                logger.exception("Merge heartbeat failed")
            self._closed.wait(MERGE_HEARTBEAT_PERIOD)
            with self._lock:
                if self._active == 0:
                    self._heartbeat_thread = None
                    return

    def _cleanup(self) -> None:
        """
        Уборка при старте — двух видов строк.

        Свои, от прошлого запуска: транзакции того процесса давно откатились, и ждать по ним
        MERGE_HEARTBEAT_TTL после каждого рестарта незачем.

        Чужие брошенные, старше MERGE_ABANDONED_TTL: при расчёте границы они и так игнорируются, но
        удалить их некому — процесс может не вернуться никогда (репликатор переименовали или
        выключили). Без этой уборки они копились бы в таблице вечно.
        """
        t = self.table
        with self.engine.begin() as conn:
            now = conn.scalar(select(func.now()))
            result = conn.execute(t.delete().where(or_(
                t.c.owner == self.owner,
                t.c.heartbeat_at < now - timedelta(seconds=MERGE_ABANDONED_TTL))))
        if result.rowcount:
            logger.info("Removed %s stale rows from %s", result.rowcount, WRITES_TABLE)

    def track(self, object_name: str) -> "_TrackedWrite":
        """
        Регистрирует начало merge и держит его до выхода из блока (то есть до коммита).

        Отметку старта и вставку строки делаем в ОДНОЙ транзакции: иначе между «спросил время» и
        «записался» помещается расчёт границы, который этого merge ещё не видит, а время берёт уже
        более позднее — и строки merge оказались бы левее границы, но невидимыми.
        """
        row_id = f'{self.owner}:{next(self._counter)}'
        with self._lock:
            self._active += 1
        self._ensure_heartbeat()
        with self.engine.begin() as conn:
            started_at = conn.scalar(select(DB_NOW_WITHOUT_TIMEZONE))
            conn.execute(insert(self.table).values(
                id=row_id, owner=self.owner, object_name=object_name,
                started_at=started_at, heartbeat_at=started_at))
        return _TrackedWrite(self, row_id, started_at)

    def _remove(self, row_id: str) -> None:
        try:
            with self.engine.begin() as conn:
                conn.execute(self.table.delete().where(self.table.c.id == row_id))
        finally:
            with self._lock:
                self._active -= 1

    def boundary(self, object_names: Iterable[str]) -> datetime:
        """
        Верхняя граница окна: минимум из «сейчас» и стартов живых незавершённых merge по этим
        таблицам, чьи бы они ни были.

        Одним запросом, а не двумя: границу спрашивают на КАЖДУЮ страницу полной выгрузки, а
        страница бывает и в одну entry (см. FULL_LOAD_MIN_BATCH) — лишний round-trip там заметен.
        Заодно «сейчас» для отсечки по живости и «сейчас» для самой границы гарантированно одно и
        то же значение.
        """
        t = self.table
        # started_at лежит в колонке без пояса, поэтому и читается уже offset-naive.
        earliest = (select(func.min(t.c.started_at))
                    .where(t.c.object_name.in_(list(object_names)),
                           t.c.heartbeat_at > DB_NOW_WITHOUT_TIMEZONE
                           - timedelta(seconds=MERGE_HEARTBEAT_TTL))
                    .scalar_subquery())
        with self.engine.connect() as conn:
            now, earliest = conn.execute(select(DB_NOW_WITHOUT_TIMEZONE, earliest)).one()
        return min(now, earliest) if earliest is not None else now

    def heartbeat(self) -> None:
        """Продлевает жизнь своим строкам. Вызывается своим же потоком (см. _heartbeat_loop), пока в
        реестре есть незавершённые merge этого процесса."""
        with self.engine.begin() as conn:
            conn.execute(update(self.table)
                         .where(self.table.c.owner == self.owner)
                         .values(heartbeat_at=func.now()))
