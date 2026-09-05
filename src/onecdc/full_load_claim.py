"""
Захват объекта под полную выгрузку — межпроцессный, с отметкой живости.

Зачем. Полную выгрузку одного объекта могут начать сразу двое: фоновый воркер репликатора (по
full_load_is_required) и расписание (FullLoadCron). Данные от этого не портятся — у каждого снимка
свой full_load_started_at, и guard'ы DBWriter1C.save не дают устаревшему снимку затереть свежие
строки, — но 1С делает двойную работу, а она здесь самая дорогая. Множества в памяти процесса для
разведения мало: репликатор и расписание могут работать в РАЗНЫХ процессах (и контейнерах), а тогда
они друг о друге не знают ничего.

Где. Отдельной таблицы нет: захват живёт двумя колонками в metadata_objects_1c, где уже лежит всё
остальное состояние полной выгрузки объекта (full_load_is_required, last_full_load_dt, метрики).
Строка на объект там и так одна, поэтому захват — это один атомарный UPDATE вида
compare-and-swap: занять удаётся тому, чей UPDATE изменил строку, а разводит гонку сама СУБД.

Отметка живости. Захват переживает процесс — строка остаётся в БД, — поэтому нужен признак, что
владелец ещё жив: иначе упавшая выгрузка заблокировала бы объект навсегда. Отметку обновляет
отдельный поток, как у реестра незавершённых merge (см. write_tracker): сам прогон занят чтением
страниц и фазой перепроверки кандидатов, где запросы идут в 1С пачками и не пишут ничего.

Константы намеренно НЕ общие с write_tracker: там отметка живости у merge, здесь у захвата
выгрузки, и путать их в коде нельзя (MERGE_* против CLAIM_*).

Владелец уникален НА ЭКЗЕМПЛЯР репликатора: имя плана обмена, хост, pid и случайный суффикс. Не на
план обмена, как у реестра merge, — иначе репликатор и расписание, поднятые в разных контейнерах
одного обмена, считались бы одним владельцем и снимали бы захваты друг у друга, то есть ровно то,
ради чего всё и делается. Хост и pid оставлены, чтобы по строке в БД было видно, кто держит объект.

Плата за уникальность: захват упавшего процесса снимает не он сам при рестарте, а истечение
CLAIM_HEARTBEAT_TTL — полторы минуты, после которых объект достаётся следующему желающему.
"""

import threading
from datetime import timedelta

from sqlalchemy import func, select, update
from sqlalchemy.engine import Engine

from cdc_1c.common_functions import DB_NOW_WITHOUT_TIMEZONE
from cdc_1c.logging_config import get_logger

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
    (MetadataReader1C._sync_objects), то есть позже, чем строится репликатор.
    """

    def __init__(self, engine: Engine, table_provider, owner: str):
        self.engine = engine
        self._table_provider = table_provider
        self.owner = owner
        self._lock = threading.Lock()
        self._held: set[str] = set()
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
            # Реестра ещё нет (метаданные не синхронизированы) — захватывать негде. Не отказываем:
            # прямой вызов full_load должен отрабатывать и на пустой базе.
            return True
        with self.engine.begin() as conn:
            # Часы БД, приведённые к наивному времени ею же: колонка отметки — timestamp без пояса,
            # а сырой now() драйвер отдаёт offset-aware (см. DB_NOW_WITHOUT_TIMEZONE). Значение
            # уходит обратно в БД, но живёт в Python, и наивным ему быть безопаснее.
            now = conn.scalar(select(DB_NOW_WITHOUT_TIMEZONE))
            result = conn.execute(
                update(table)
                .where(table.c.object_full_name == object_full_name,
                       (table.c[OWNER_FIELD].is_(None))
                       | (table.c[HEARTBEAT_FIELD]
                          < now - timedelta(seconds=CLAIM_HEARTBEAT_TTL)))
                .values(**{OWNER_FIELD: self.owner, HEARTBEAT_FIELD: now}))
        if result.rowcount == 0:
            return False
        with self._lock:
            self._held.add(object_full_name)
            self._start_heartbeat()
        return True

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
        with self.engine.begin() as conn:
            conn.execute(update(table).where(table.c[OWNER_FIELD] == self.owner)
                         .values(**{HEARTBEAT_FIELD: func.now()}))

    def close(self) -> None:
        self._closed.set()

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
