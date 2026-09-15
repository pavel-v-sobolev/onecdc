"""
Реестр незавершённых merge: общий на процессы, живёт в таблице БД.

Отдельным модулем, потому что пользуются им обе стороны и по разным поводам. Обработчик прижимает
к нему верхнюю границу своего окна, репликатор — отметку страницы полной выгрузки: merged_on
штампуется ВНУТРИ merge-транзакции, а коммитится позже, поэтому «сейчас» перешагнуло бы строки,
которые уже помечены прошедшим временем, но ещё не видны. Обеим сторонам нужен один и тот же
ответ на вопрос «до какого момента данные точно устоялись».
"""

import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Iterable

from sqlalchemy import (Column, DateTime, Engine, MetaData, String, Table, delete, func, insert,
                        select, update)

from onecdc.common_functions import (DB_NOW_WITHOUT_TIMEZONE,
                                     HEARTBEAT_JOIN_TIMEOUT, instance_owner)
from onecdc.db_logs import _check_create_schema, create_table_if_absent
from onecdc.logging_config import get_logger

logger = get_logger(__name__)

# Реестр идущих merge (WriteTracker). Репликатор обновляет отметку живости своих строк
# не реже MERGE_HEARTBEAT_PERIOD; строки, чья отметка старше MERGE_HEARTBEAT_TTL, считаются
# брошенными —
# процесс, который их завёл, умер, а вместе с ним откатились и его транзакции, так что держать по
# ним границу больше не нужно. TTL с запасом больше периода: разовая задержка не должна выглядеть
# как смерть процесса.
WRITES_TABLE = "onecdc_writes_in_process"
MERGE_HEARTBEAT_PERIOD = 20.0
MERGE_HEARTBEAT_TTL = 90.0
# Отдельного срока «пора удалять» больше нет: строку, чья отметка старше MERGE_HEARTBEAT_TTL,
# разбирает deliver_abandoned — сигналит её таблице и удаляет. Раньше такие строки просто
# копились до часа и удалялись молча, унося с собой единственный след недоставленного сигнала.


class _TrackedWrite:
    """
    Один merge в реестре. Строка живёт от начала merge до ДОСТАВЛЕННОГО сигнала обработчику, а не
    до коммита данных: пока она есть, обязанность сообщить об изменении не исполнена.

    Штатный выход: строка удаляется ОДНОЙ транзакцией вместе с сигналом (см. WriteTracker._finish).
    Выход с исключением: строка остаётся уликой — её merge мог закоммититься, а сигнал не уйти,
    и повтор пакета этого уже не восстановит (merge второй раз ничего не изменит, сигнала не
    будет, а пакет подтвердится). Продлевать её больше некому, поэтому она протухнет и достанется
    разбору (WriteTracker.deliver_abandoned).

    result — то, что вернул merge; по нему решают, нужен ли сигнал вообще. Проставляет вызывающий.
    """

    def __init__(self, tracker, key: str, started_at: datetime,
                 object_name: str, source: str | None):
        self._tracker = tracker
        self._key = key
        self.started_at = started_at
        self.object_name = object_name
        self.source = source
        self.result = None

    def __enter__(self) -> "_TrackedWrite":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            self._tracker._abandon(self._key)
        else:
            self._tracker._finish(self._key, self.object_name, self.source, self.result)


def _writes_table(metadata: MetaData, schema_name: str | None) -> Table:
    return Table(
        WRITES_TABLE, metadata,
        Column("id", String(64), primary_key=True),
        Column("owner", String(255), nullable=False),
        Column("object_name", String(255), nullable=False),
        # Момент старта merge по часам БД — то, к чему прижимается граница окна обработчика.
        Column("started_at", DateTime, nullable=False),
        # Отметка живости: обновляется, пока merge действительно идёт (см. MERGE_HEARTBEAT_TTL).
        Column("heartbeat_at", DateTime, nullable=False),
        # Источник изменения (changes / full_load). Нужен разбору брошенных строк: сигнал проходит
        # через фильтр on_full_load, и без источника отложенный сигнал разбудил бы обработчика,
        # который от бэкфилла отписался.
        Column("signal_source", String(16), nullable=True),
        schema=schema_name,
    )


class WriteTracker:
    """
    Реестр незавершённых merge в БАЗЕ: строка живёт ровно столько, сколько данные merge могут быть
    ещё не видны другим процессам.

    Нужен, потому что обработчик может работать отдельно от репликатора. Граница его окна обязана
    быть прижата к незакоммиченным merge: их merged_on уже в прошлом, а строки ещё не видны, и
    отметка, взятая как «сейчас», их бы перешагнула — потеря строк, молча. В памяти такой реестр
    чужому процессу не виден, поэтому он живёт в таблице onecdc_writes_in_process: строка появляется
    перед merge и исчезает после коммита.

    Брошенные строки (процесс умер между вставкой и удалением) отсекаются по отметке живости: раз
    процесса нет, его транзакции откатились, и держать по ним границу незачем. Поэтому падение
    репликатора обработчиков не морозит — максимум на MERGE_HEARTBEAT_TTL.

    Владелец строки — ЭКЗЕМПЛЯР процесса, а не план обмена (см. common_functions.instance_owner).
    Это не косметика: владельцем отбираются строки, которые процесс вправе удалить при старте и
    которым продлевает отметку живости. Пока владельцем было имя обмена, второй процесс того же
    обмена — а это штатная раскладка «репликатор и расписание в разных контейнерах» — при старте
    удалял ЖИВЫЕ строки первого (граница переставала их ждать, обработчик молча перешагивал ещё не
    закоммиченные merge), продлевал чужие брошенные строки вечно и мог столкнуться с ним на
    первичном ключе. Тот же урок раньше был усвоен в full_load_claim.

    Отметку живости обновляет сам реестр, своим потоком. Именно реестр, а не цикл репликации:
    строки появляются в любом сценарии, включая одиночный run_once и вызванный руками full_load,
    а цикла в этих сценариях нет. Без этого одна страница выгрузки, считающаяся дольше
    MERGE_HEARTBEAT_TTL, признавалась бы брошенной — и обработчик молча перешагнул бы её строки.
    Поток
    отдельный ещё и потому, что merge и сам может идти дольше MERGE_HEARTBEAT_TTL: обновлять отметку
    между merge поздно.
    """

    def __init__(self, engine: Engine, schema: str | None, owner: str,
                 deliver_signal=None):
        self.engine = engine
        self.schema_name = _check_create_schema(engine, schema)
        # Уникализируем ВНУТРИ, а не доверяем вызывающему: владелец здесь — не «чей это обмен», а
        # «какой процесс держит строку», и цена ошибки — молча удалённая чужая живая строка.
        # Переданное имя остаётся префиксом, чтобы в таблице было видно, кто это.
        self.owner = instance_owner(owner)
        self.table = _writes_table(MetaData(), self.schema_name)
        # Состав колонок этой таблицы менялся (signal_source добавлен позже); create_table_if_absent
        # доводит действующую таблицу до текущего состава сам.
        create_table_if_absent(engine, self.table)
        self._lock = threading.Lock()
        # Доставка сигнала обработчикам: вызывается с открытым соединением, чтобы удаление строки
        # и сигнал легли в ОДНУ транзакцию. Сам реестр про обработчиков не знает ничего —
        # подставляет функцию тот, кто его завёл (см. Replicator).
        self._deliver_signal = deliver_signal
        # Строки СВОИХ merge, которые сейчас реально идут, и поток, который продлевает им отметку
        # живости. Именно множество идущих, а не «все строки этого владельца»: строку брошенного
        # merge живой процесс иначе освежал бы вечно, она никогда бы не протухла, и граница окна
        # замёрзла бы навсегда. Поток поднимается с первым merge и гаснет, когда продлевать
        # становится нечего: реестры живут долго, и оставлять при каждом по спящему потоку незачем.
        self._in_flight: set[str] = set()
        self._heartbeat_thread: threading.Thread | None = None
        self._closed = threading.Event()

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
        ПОСЛЕ добавления merge в _in_flight. Поэтому промежутка, в котором merge уже стартовал, а
        поток уже решил выйти, не существует: либо выходящий поток видит непустое множество и
        остаётся, либо он успел обнулить _heartbeat_thread, и track() заводит новый.
        """
        while not self._closed.is_set():
            try:
                self.heartbeat()
            except Exception:
                logger.exception("Merge heartbeat failed")
            self._closed.wait(MERGE_HEARTBEAT_PERIOD)
            with self._lock:
                if not self._in_flight:
                    self._heartbeat_thread = None
                    return

    def deliver_abandoned(self) -> int:
        """
        Разбирает брошенные строки: сигналит их таблицам и удаляет. Возвращает число разобранных.

        Брошенная строка — улика: в таблицу шла запись, которая не дошла до сигнала. Данные при
        этом могли закоммититься, а повтор пакета сигнала уже не вернёт — второй merge ничего не
        изменит, `rows_modified` будет ноль, и пакет молча подтвердится. Поэтому сигналим, не
        глядя на то, что вернул merge.

        Откатился merge или закоммитился, по строке не видно, и знать это не нужно: сигнал
        идемпотентен. Лишний сигнал — это один холостой проход обработчика, чей запрос по окну
        вернёт ноль строк.

        Каждая строка разбирается СВОЕЙ транзакцией вместе со своим сигналом: если сигнал не
        пройдёт, строка останется и достанется следующему разбору.

        Это и заменило прежнюю уборку по MERGE_ABANDONED_TTL — она удаляла улики молча. Строки
        своего прошлого запуска здесь тоже разбираются: владелец уникален на экземпляр (см.
        __init__), и для нового процесса они такие же чужие, как любые другие.
        """
        t = self.table
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(t.c.id, t.c.object_name, t.c.signal_source)
                .where(t.c.heartbeat_at < DB_NOW_WITHOUT_TIMEZONE
                       - timedelta(seconds=MERGE_HEARTBEAT_TTL))).all()
        delivered = 0
        for row in rows:
            with self.engine.begin() as conn:
                if not conn.execute(t.delete().where(t.c.id == row.id)).rowcount:
                    continue        # разобрал кто-то другой
                if self._deliver_signal is not None:
                    self._deliver_signal(conn, row.object_name, row.signal_source, None, True)
            delivered += 1
        if delivered:
            logger.warning("Recovered %s interrupted write(s) from %s: signalled %s",
                           delivered, WRITES_TABLE,
                           ', '.join(sorted({row.object_name for row in rows})))
        return delivered

    def track(self, object_name: str, source: str | None = None) -> "_TrackedWrite":
        """
        Регистрирует начало merge и держит его до выхода из блока (то есть до коммита).

        Отметку старта и вставку строки делаем в ОДНОЙ транзакции: иначе между «спросил время» и
        «записался» помещается расчёт границы, который этого merge ещё не видит, а время берёт уже
        более позднее — и строки merge оказались бы левее границы, но невидимыми.
        """
        # Идентификатор строки не выводим из owner: в колонке 64 символа, а уникальный owner —
        # это имя обмена плюс хост, pid и суффикс, и в контейнерной раскладке он туда не влезет.
        # uuid4 и короче, и уникален сам по себе, без счётчика.
        row_id = uuid.uuid4().hex
        with self._lock:
            self._in_flight.add(row_id)
        self._ensure_heartbeat()
        with self.engine.begin() as conn:
            started_at = conn.scalar(select(DB_NOW_WITHOUT_TIMEZONE))
            conn.execute(insert(self.table).values(
                id=row_id, owner=self.owner, object_name=object_name,
                started_at=started_at, heartbeat_at=started_at, signal_source=source))
        return _TrackedWrite(self, row_id, started_at, object_name, source)

    def _finish(self, row_id: str, object_name: str, source: str | None, result) -> None:
        """
        Штатное завершение: удаление строки и сигнал обработчикам — ОДНОЙ транзакцией.

        Именно одной. Порознь между ними помещается сбой, и получается ровно то, от чего строка
        и заведена: данные закоммичены, сигнала нет, а повтор пакета его не восстановит. А если
        сигнал поставить, не убрав строку, то обработчик, проснувшийся в этот промежуток, прижмёт
        границу окна к НАШЕМУ merge, свежих строк не увидит и израсходует отметку впустую.
        """
        with self._lock:
            self._in_flight.discard(row_id)
        with self.engine.begin() as conn:
            conn.execute(self.table.delete().where(self.table.c.id == row_id))
            if self._deliver_signal is not None:
                self._deliver_signal(conn, object_name, source, result, False)

    def _abandon(self, row_id: str) -> None:
        """
        Выход с исключением: строку ОСТАВЛЯЕМ, из множества идущих убираем.

        Дальше она не продлевается — её merge больше не идёт, — поэтому протухнет сама и станет
        уликой для разбора. Никаких дополнительных пометок для этого не нужно.
        """
        with self._lock:
            self._in_flight.discard(row_id)

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
        """
        Продлевает строки merge, которые СЕЙЧАС идут. Вызывается своим же потоком
        (см. _heartbeat_loop).

        По списку идущих, а не по владельцу: строка брошенного merge принадлежит тому же
        владельцу, и продление по владельцу освежало бы её, пока процесс жив, — она не протухла
        бы никогда, а граница окна обработчика замёрзла бы вместе с ней.
        """
        with self._lock:
            in_flight = list(self._in_flight)
        if not in_flight:
            return
        with self.engine.begin() as conn:
            conn.execute(update(self.table)
                         .where(self.table.c.id.in_(in_flight))
                         .values(heartbeat_at=func.now()))
