"""
Полная выгрузка одного объекта по crontab-расписанию.

FullLoadCron вызывает Replicator.full_load и использует общий механизм остановки
StopSignal. Расписание задаётся выражением croniter и вычисляется в локальном
времени процесса. Результат выгрузки — число изменённых строк; это диагностический
показатель, а не самостоятельная проверка полноты CDC.
"""

import threading
import time
from datetime import date, datetime, timedelta

from croniter import croniter

from onecdc.logging_config import get_logger
from onecdc.replicator import FullLoadBusy
from onecdc.stop_signal import StopSignal, install_signal_handlers

logger = get_logger(__name__)

# Тип границы периода: то, что понимает full_load, плюс timedelta (скользящее окно).
DateBound = date | datetime | str | timedelta | None


class FullLoadCron:
    """
    Запускать полную выгрузку одного объекта по расписанию.

    replicator выполняет загрузку. table_name и date_field принимают как имена 1С,
    так и закреплённые имена БД. Имена разрешаются по метаданным при первом запуске.

    date_from/date_to ограничивают период включительно и требуют date_field.
    Граница timedelta вычисляется заново перед каждым запуском: целые сутки
    отсчитываются от сегодняшней даты, остальные смещения — от текущего момента.
    Для локального расписания в контейнере настройте TZ.

    При full_history_on_first_run=True расписание с границами сначала читает всю
    историю, если в реестре нет last_full_load_dt. После успеха обновляет флаг, дату
    и метрики полной выгрузки. Расписание без границ и обычные последующие прогоны
    эти поля не изменяют.

    Занятый объект пропускается до следующего срабатывания. Пропущенные срабатывания
    не накапливаются; для скользящего периода следующая выгрузка получит новые границы.
    mark_missing всегда включён. Ограничения чтения меняющегося источника через $skip
    описаны в DESIGN.md.
    """

    def __init__(self, replicator, table_name: str, *, cron: str,
                 date_field: str | None = None,
                 date_from: DateBound = None, date_to: DateBound = None,
                 batch_size: int = 1000, full_history_on_first_run: bool = True):
        # Перехват SIGTERM/SIGINT — по той же причине, что у Replicator и HandlerLoop: run_forever
        # уходит в пул потоков, а поставить перехват можно только из главного (см. stop_signal).
        install_signal_handlers(quiet=False)

        # Параметры проверяем здесь, а не в 3 часа ночи в чужом потоке.
        if not croniter.is_valid(cron):
            raise ValueError(f"cron={cron!r} is not a valid crontab expression "
                             "(e.g. '0 3 * * *' — every day at 03:00)")
        if (date_from is not None or date_to is not None) and not date_field:
            raise ValueError("date_from/date_to require date_field "
                             "(Date for documents, Period for registers)")

        self.replicator = replicator
        self.table_name = table_name
        self.cron = cron
        self.date_field = date_field
        self.date_from = date_from
        self.date_to = date_to
        self.batch_size = batch_size
        # Первый прогон — без границ периода (см. класс). Отключать стоит только там, где история
        # заведомо не нужна и её чтение неприемлемо дорого.
        self.full_history_on_first_run = full_history_on_first_run

        # Разрешённые имена 1С (объект и поле даты): требуют $metadata, то есть сетевого запроса,
        # поэтому разрешаются при первом прогоне; этот конструктор к 1С не обращается.
        self._object_name: str | None = None
        self._date_field_1c: str | None = None
        self._resolve_lock = threading.Lock()
        # Действующий StopSignal текущего run_forever — через него цикл останавливают снаружи.
        self._stop_signal: "StopSignal | None" = None

    def __repr__(self) -> str:
        return f'<FullLoadCron {self.table_name} {self.cron!r}>'

    def run_forever(self, max_runs: int = 0) -> None:
        """
        Выполнять блокирующий цикл расписания до остановки или max_runs запусков.

        max_runs=0 не ограничивает число запусков. Ошибка одного прогона записывается
        в журнал, затем цикл ждёт следующего времени по расписанию.
        request_stop завершает цикл после текущей выгрузки. SIGTERM/SIGINT дополнительно
        устанавливают процессный флаг: full_load проверяет его между страницами.
        """
        stop = StopSignal()
        self._stop_signal = stop
        logger.info("Schedule %s started (%s)", self.table_name, self.cron)
        runs = 0
        while not stop.requested:
            # Итератор считаем от «сейчас» перед каждым ожиданием: если прогон шёл дольше периода,
            # пропущенные срабатывания пропускаются, а не выстреливают пачкой подряд.
            next_run = croniter(self.cron, datetime.now()).get_next(datetime)
            logger.info("Next full load of %s at %s", self.table_name, next_run)
            stop.wait((next_run - datetime.now()).total_seconds())
            if stop.requested:
                break
            try:
                self.run_once()
            except Exception:
                # Расписание переживает неудачный прогон: следующий состоится. Иначе поток тихо
                # умрёт, а исключение пролежит в future до .result() точки входа.
                logger.exception("Scheduled full load of %s failed", self.table_name)
            runs += 1
            if max_runs > 0 and runs >= max_runs:
                logger.info("Reached max_runs (%s), stopping", max_runs)
                break
        logger.info("Schedule %s stopped", self.table_name)

    def request_stop(self) -> None:
        """Запросить остановку цикла расписания после текущего прогона."""
        if self._stop_signal is not None:
            self._stop_signal.requested = True

    def run_once(self) -> int:
        """
        Выполнить выгрузку немедленно с периодом, вычисленным на момент вызова.

        Вернуть число изменённых строк. Ноль также возвращается при пропуске занятого
        объекта; такой пропуск сопровождается предупреждением. Прочие ошибки передаются
        вызывающему.
        """
        object_name, date_field = self._resolve()
        # Объект занимает сам full_load — здесь только разбираем его отказ: нам нужно отличить
        # «уже выгружает кто-то другой» от «выгрузили, расхождений нет», иначе первый прогон
        # отметился бы выполненным, ничего не выгрузив.
        try:
            date_from = _bound(self.date_from)
            date_to = _bound(self.date_to)
            # При отсутствии отметки загрузки читаем всю историю. Проверка предшествует
            # захвату в full_load: параллельное завершение другого прогона может привести
            # к дополнительному чтению истории, но не к одновременной выгрузке объекта.
            first_run = (self.full_history_on_first_run
                         and (date_from is not None or date_to is not None)
                         and not self.replicator.metadata.was_fully_loaded(object_name))
            if first_run:
                logger.info("Full load of %s has never run: reading the whole object once, "
                            "ignoring the configured period", self.table_name)
                date_from = date_to = None
            started = time.monotonic()
            rows_modified = self.replicator.full_load(
                object_name, batch_size=self.batch_size, date_field=date_field,
                date_from=date_from, date_to=date_to, skip_if_busy=False)
            if first_run:
                # Прогон прочитал объект целиком — это и есть полная выгрузка, и отметить её надо
                # ровно так же, как её отмечает фоновый поток репликатора. Без этого
                # was_fully_loaded осталась бы ложной, и каждое следующее срабатывание снова читало
                # бы всю историю. Обычный прогон за период реестр не трогает: объект целиком он не
                # читал, и объявлять его выгруженным нельзя — это отменило бы фоновую выгрузку,
                # которая ещё должна собрать базу.
                self.replicator.metadata.mark_full_loaded(
                    object_name, rows_modified=rows_modified,
                    minutes=round((time.monotonic() - started) / 60, 3))
        except FullLoadBusy:
            logger.warning("Full load of %s is already running, skipping this run",
                           self.table_name)
            return 0
        logger.info("Scheduled full load of %s (%s..%s) modified %s rows",
                    self.table_name, date_from or '', date_to or '', rows_modified)
        return rows_modified

    def _resolve(self) -> tuple[str, str | None]:
        """
        Разрешить и закэшировать имена объекта и поля даты через MetadataReader.

        Если метаданные ещё не загружены, сначала прочитать их из 1С. Учитываются как
        исходные имена, так и идентификаторы, закреплённые NameMapper в БД.
        """
        with self._resolve_lock:
            if self._object_name is not None:
                return self._object_name, self._date_field_1c

            metadata = self.replicator.metadata
            if not metadata.is_loaded:
                metadata.get_metadata()

            # Разбор имён — общий с full_load (MetadataReader.resolve_*): обе формы, имя 1С и
            # имя таблицы/колонки в БД. Раньше это же правило жило здесь своей копией.
            object_name = metadata.resolve_object_name(self.table_name)
            date_field = (metadata.resolve_field_name(object_name, self.date_field)
                          if self.date_field else None)

            logger.info("Schedule %s resolved to 1C object %s (date_field=%s)",
                        self.table_name, object_name, date_field)
            self._object_name, self._date_field_1c = object_name, date_field
            return object_name, date_field


def _bound(bound: DateBound):
    """
    Вычислить границу периода: timedelta — смещение назад, остальные типы без изменений.

    Целые сутки отсчитываются от date.today(), смещения с часами или меньшими единицами —
    от datetime.now(). У результата datetime доли секунды отбрасываются.
    """
    if isinstance(bound, timedelta):
        if bound % timedelta(days=1):
            return (datetime.now() - bound).replace(microsecond=0)
        return date.today() - bound
    return bound
