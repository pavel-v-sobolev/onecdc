"""
Реестр незавершённых записей: общий на процессы, живёт таблицей в БД.

Решает одну задачу. `merged_on` штампуется ВНУТРИ merge-транзакции, а коммитится позже — значит
существует промежуток, в котором строка уже помечена прошедшим временем, но другим соединениям
ещё не видна. Обработчик, взявший верхнюю границу окна как «сейчас», такие строки перешагнул бы
и больше к ним не вернулся: его окно двигается только вперёд. Потеря молчаливая — ни ошибки, ни
расхождения в счётчиках.

Поэтому репликатор заводит строку реестра ПЕРЕД записью, а `boundary` отдаёт обеим сторонам один
ответ на вопрос «до какого момента данные точно устоялись»: обработчику — верхнюю границу окна,
полной выгрузке — отметку, с которой она сравнивает свежесть строк в БД. Реестр в БД, а не в
памяти, потому что процессы разные: обработчик штатно живёт отдельно от репликатора.

Строка ПЕРЕЖИВАЕТ коммит данных. Снимается она позже — когда записана метка сигнала
обработчикам. Пока строка есть, обязанность сообщить об изменении считается неисполненной: сбой
сразу после коммита иначе терял бы уведомление насовсем, и повтор пакета не спас бы — второй
merge отличий не найдёт, `rows_modified` будет ноль, метки не будет. Оставшуюся строку подбирает
`deliver_abandoned`.

Обработчик в этом не участвует. «Сигнал» — это метка `update_requested_at` в `onecdc_handlers`,
которую ставит сам репликатор; подтверждения от обработчика нет, реестр он не трогает, и
работающего обработчика может не быть вовсе — метка просто лежит, пока её кто-нибудь не заметит.
"""

import threading
import uuid
from datetime import datetime, timedelta
from typing import Iterable

from sqlalchemy import (Column, DateTime, Engine, MetaData, String, Table, delete, func, insert,
                        select, update)

from onecdc.common_functions import (DB_NOW_WITH_TIMEZONE,
                                     HEARTBEAT_JOIN_TIMEOUT, instance_owner)
from onecdc.db_logs import _check_create_schema, create_table_if_absent
from onecdc.logging_config import get_logger

logger = get_logger(__name__)

WRITES_TABLE = "onecdc_writes_in_process"

# Как часто владелец продлевает отметку своих строк.
MERGE_HEARTBEAT_PERIOD = 20.0
# Насколько должна протухнуть отметка, чтобы boundary перестала держать границу по этой строке.
# Иначе прерванная запись морозила бы окно обработчика навсегда.
#
# Это решение ПО ТАЙМАУТУ, и оно не доказывает ни смерть процесса, ни откат его транзакции:
# обновление могло просто задержаться, а данные могли закоммититься и не доехать только сигналом.
# Отсюда два следствия: TTL с запасом больше периода (разовая задержка не должна выглядеть как
# смерть), и истечение TTL строку НЕ удаляет — её разбирает deliver_abandoned, сигналя её таблице
# и удаляя одной транзакцией. Отдельной молчаливой уборки по возрасту здесь нет намеренно: она
# уносила бы единственный след недоставленного сигнала.
MERGE_HEARTBEAT_TTL = 90.0


class _TrackedWrite:
    """
    Контекст одной зарегистрированной операции записи.

    При штатном выходе _finish вызывает функцию доставки сигнала и удаляет строку реестра
    одной транзакцией. Коммит данных к этому моменту должен быть уже завершён вызывающим кодом.
    result заполняет вызывающий: по результату merge функция доставки решает, нужно ли
    уведомлять обработчиков. Если данные не изменились, строка удаляется без уведомления.

    При исключении в блоке или при доставке сигнала строка остаётся в БД, но её heartbeat_at
    больше не продлевается. После истечения TTL её сможет обработать deliver_abandoned.
    Это важно, если данные уже сохранены: повторный merge может не найти изменений и потому
    не отправить новое уведомление.
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
        # Начало операции по часам БД; ограничивает окно обработчика и отметку страницы выгрузки.
        Column("started_at", DateTime(timezone=True), nullable=False),
        # Обновляется, пока операция находится в _in_flight, в том числе после коммита данных.
        Column("heartbeat_at", DateTime(timezone=True), nullable=False),
        # Источник изменения (changes / full_load). Нужен разбору брошенных строк: сигнал проходит
        # через фильтр on_full_load, и без источника отложенный сигнал разбудил бы обработчика,
        # который от полной выгрузки отписался.
        Column("signal_source", String(16), nullable=True),
        schema=schema_name,
    )


class WriteTracker:
    """
    Реестр незавершённых записей в БД: строка живёт ровно столько, сколько её данные могут быть
    ещё не видны другим процессам (зачем это нужно — см. docstring модуля).

    `track` заводит строку ДО записи данных. При штатном выходе из блока `deliver_signal`
    вызывается в той же транзакции, которая строку удаляет: порознь между ними помещается тот
    самый сбой, из-за которого сигнал теряется навсегда. Функцию подставляет репликатор — сам
    реестр про обработчиков не знает ничего; она получает соединение, имя таблицы, источник
    изменения, результат записи и признак принудительного уведомления. Без `deliver_signal`
    реестр только ведёт строки и удаляет завершённые.

    **Ничего не отбирается по владельцу.** Продлеваются строки по `id` из `_in_flight`,
    удаляются — по `id`; колонка `owner` не участвует ни в одном `WHERE` и нужна только для
    диагностики (поэтому `instance_owner` и делает её уникальной на экземпляр процесса). Урок
    дорогой: пока владельцем было имя плана обмена и отбор шёл по нему, второй процесс того же
    обмена — а «репликатор и расписание в разных контейнерах» это штатная раскладка — продлевал
    чужие брошенные строки вечно и трогал ЖИВЫЕ строки первого, из-за чего обработчик молча
    перешагивал ещё не закоммиченные merge (CDC-02).

    Отметку живости ведёт сам реестр отдельным потоком, а не цикл репликации. Не цикл — потому
    что строки появляются и в одиночном `run_once`, и в вызванном руками `full_load`, где цикла
    нет вовсе. Отдельным потоком — потому что один merge может идти дольше
    MERGE_HEARTBEAT_TTL: обновлять отметку между merge уже поздно, и такая страница выгрузки
    считалась бы брошенной, а обработчик перешагнул бы её строки.
    """

    def __init__(self, engine: Engine, schema: str | None, owner: str,
                 deliver_signal=None, heartbeat_engine: Engine | None = None):
        self.engine = engine
        # Репликатор передаёт для heartbeat отдельный пул (см. lease.lease_engine), чтобы
        # занятые записью данных соединения не задерживали обновление отметок. Иначе активная
        # операция может превысить TTL и перестать ограничивать boundary. Без отдельного
        # heartbeat_engine используется общий engine, через который идут остальные запросы.
        self._heartbeat_engine = heartbeat_engine or engine
        self.schema_name = _check_create_schema(engine, schema)
        # Различаем экземпляры реестра даже при одинаковом имени обмена. Префикс сохраняется
        # для диагностики, instance_owner добавляет хост, PID и случайный суффикс. Сами строки
        # выбираются для продления по id из _in_flight, а не по owner.
        self.owner = instance_owner(owner)
        self.table = _writes_table(MetaData(), self.schema_name)
        # Состав колонок этой таблицы менялся (signal_source добавлен позже); create_table_if_absent
        # доводит действующую таблицу до текущего состава сам.
        create_table_if_absent(engine, self.table)
        self._lock = threading.Lock()
        # Доставка сигнала обработчикам: вызывается с открытым соединением, чтобы удаление строки
        # и сигнал легли в одну транзакцию. Сам реестр про обработчиков не знает ничего —
        # подставляет функцию тот, кто его завёл (см. Replicator).
        self._deliver_signal = deliver_signal
        # Операции, которым этот экземпляр продолжает обновлять heartbeat_at. Завершение или
        # исключение убирает id из множества; оставшаяся после ошибки строка БД больше не
        # продлевается. Если обновлять все строки owner, живой процесс никогда не даст своим
        # прерванным операциям достичь TTL. Поток запускается при первой операции и завершается,
        # когда множество опустеет.
        self._in_flight: set[str] = set()
        self._heartbeat_thread: threading.Thread | None = None
        self._closed = threading.Event()

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
        после добавления merge в _in_flight. Поэтому промежутка, в котором merge уже стартовал, а
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
        Обрабатывает строки с истёкшим TTL и возвращает число удалённых строк.

        Отбирает строки по heartbeat_at независимо от owner: в том числе операции других
        процессов и прежних запусков. Результат записи неизвестен — данные могли как
        сохраниться, так и откатиться. Поэтому функция доставки вызывается с result=None
        и forced=True. Репликатор при этом отправляет сигнал без проверки rows_modified,
        сохраняя фильтрацию подписчиков по источнику changes/full_load.

        Повторная установка метки сигнала допустима. Она может вызвать лишний запуск
        обработчика, но не требует повторять запись данных. Это восстанавливает уведомление
        и тогда, когда повторный merge уже не обнаружил бы изменений.

        Каждая строка удаляется в одной транзакции с вызовом deliver_signal, если он задан.
        Если другой процесс уже удалил строку, она пропускается. Ошибка доставки откатывает
        удаление текущей строки и прерывает метод; ранее завершённые транзакции сохраняются.
        """
        t = self.table
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(t.c.id, t.c.object_name, t.c.signal_source)
                .where(t.c.heartbeat_at < DB_NOW_WITH_TIMEZONE
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
        Регистрирует операцию записи и возвращает контекст для блока with.

        object_name — имя целевой таблицы БД, source — источник будущего сигнала
        (changes/full_load). Время начала берётся из БД, и строка реестра фиксируется
        до возврата контекста: к началу записи данных её уже могут видеть другие процессы.

        Вызывающий код выполняет и завершает запись внутри блока, сохраняя результат
        в tracked.result. Сам WriteTracker не управляет транзакцией данных. При штатном
        выходе из блока _finish отдельно завершает уведомление и удаляет строку реестра.
        """
        # Идентификатор строки не выводим из owner: в колонке 64 символа, а уникальный owner —
        # это имя обмена плюс хост, pid и суффикс, и в контейнерной раскладке он туда не влезет.
        # uuid4 и короче, и уникален сам по себе, без счётчика.
        row_id = uuid.uuid4().hex
        with self._lock:
            self._in_flight.add(row_id)
        self._ensure_heartbeat()
        with self.engine.begin() as conn:
            started_at = conn.scalar(select(DB_NOW_WITH_TIMEZONE))
            conn.execute(insert(self.table).values(
                id=row_id, owner=self.owner, object_name=object_name,
                started_at=started_at, heartbeat_at=started_at, signal_source=source))
        return _TrackedWrite(self, row_id, started_at, object_name, source)

    def _finish(self, row_id: str, object_name: str, source: str | None, result) -> None:
        """
        Завершает операцию: удаляет строку реестра и вызывает доставку сигнала одной транзакцией.

        Если удалить строку раньше отдельной отправки сигнала, сбой между этими действиями
        потеряет уведомление вместе со сведениями, по которым его можно восстановить.
        Общая транзакция также делает сигнал видимым одновременно со снятием ограничения
        boundary, которое удерживала эта операция.

        Из _in_flight операция удаляется до обращения к БД. Поэтому при ошибке доставки
        строка останется в реестре без продления heartbeat_at и позже попадёт в deliver_abandoned.
        """
        with self._lock:
            self._in_flight.discard(row_id)
        with self.engine.begin() as conn:
            conn.execute(self.table.delete().where(self.table.c.id == row_id))
            if self._deliver_signal is not None:
                self._deliver_signal(conn, object_name, source, result, False)

    def _abandon(self, row_id: str) -> None:
        """
        Прекращает продление операции, завершившейся исключением внутри блока.

        Строка остаётся в БД: по исключению нельзя определить, успели ли сохраниться данные.
        После истечения TTL её сможет обработать deliver_abandoned. Дополнительная пометка
        ошибки в реестре для этого не нужна.
        """
        with self._lock:
            self._in_flight.discard(row_id)

    def boundary(self, object_names: Iterable[str]) -> datetime:
        """
        Возвращает границу времени для указанных таблиц БД, учитывая операции всех владельцев.

        Это минимум из текущего времени БД и started_at строк со свежим heartbeat_at.
        Если подходящих строк нет, возвращается текущее время. Строка может ограничивать
        границу и после коммита данных, пока завершение уведомления ещё не удалило её из реестра.

        Обработчик использует эту отметку как верхнюю границу окна изменений, а полная
        выгрузка — для защиты свежих данных при записи страницы. Результат — datetime с поясом.

        Время и минимум берутся ОДНИМ запросом, а не двумя. Границу спрашивают на каждую страницу
        полной выгрузки, а страница бывает и в одну entry (см. FULL_LOAD_MIN_BATCH) — лишний
        round-trip там заметен. И «сейчас» для отсечки по TTL и «сейчас» для самой границы
        оказываются гарантированно одним значением, а не двумя близкими.
        """
        t = self.table
        # started_at имеет тип timestamptz, как и результат DB_NOW_WITH_TIMEZONE:
        # оба значения приходят с часовым поясом и сравниваются без преобразования.
        earliest = (select(func.min(t.c.started_at))
                    .where(t.c.object_name.in_(list(object_names)),
                           t.c.heartbeat_at > DB_NOW_WITH_TIMEZONE
                           - timedelta(seconds=MERGE_HEARTBEAT_TTL))
                    .scalar_subquery())
        with self.engine.connect() as conn:
            now, earliest = conn.execute(select(DB_NOW_WITH_TIMEZONE, earliest)).one()
        return min(now, earliest) if earliest is not None else now

    def heartbeat(self) -> None:
        """
        Обновляет heartbeat_at у операций из _in_flight по времени БД.

        Вызывается фоновым потоком _heartbeat_loop. Список id копируется под блокировкой,
        а запрос выполняется после её освобождения, чтобы не задерживать track и завершение
        операций. Строки, оставшиеся после ошибки и уже исключённые из _in_flight, в новые
        списки не попадают, даже если принадлежат этому же owner.
        """
        with self._lock:
            in_flight = list(self._in_flight)
        if not in_flight:
            return
        with self._heartbeat_engine.begin() as conn:
            conn.execute(update(self.table)
                         .where(self.table.c.id.in_(in_flight))
                         .values(heartbeat_at=func.now()))
