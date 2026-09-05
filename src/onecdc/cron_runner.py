"""
Полная выгрузка объекта по расписанию: третий вечный цикл проекта, рядом с Replicator1C.run_forever
(изменения) и HandlerLoop.run_forever (витрины).

Цикл изменений догоняет данные событиями, а полная выгрузка — сверяет: она читает объект из 1С
целиком и возвращает число РЕАЛЬНО изменённых строк, то есть при исправном CDC отвечает нулём.
Ежедневная сверка свежего хвоста — самый дешёвый способ увидеть, что изменение где-то не доехало,
и тут же его выровнять.

Расписание — обычная crontab-строка (croniter). Планировщика со своим жизненным циклом
(APScheduler и подобные) сознательно нет: тогда в процессе появился бы второй способ остановки
рядом с общим SIGTERM. Здесь всё как у остальных циклов — блокирующий run_forever, StopSignal,
request_stop().
"""

import threading
import time
from datetime import date, datetime, timedelta

from croniter import croniter

from cdc_1c.logging_config import get_logger
from cdc_1c.stop_signal import StopSignal, install_signal_handlers

logger = get_logger(__name__)

# Тип границы периода: то, что понимает full_load, плюс timedelta (скользящее окно).
DateBound = date | datetime | str | timedelta | None


class FullLoadCron:
    """
    Полная выгрузка ОДНОГО объекта 1С по crontab-расписанию.

    Один объект на цикл, как один обработчик на HandlerLoop: у каждого своё расписание, свой поток
    и свой прогон, и тяжёлая ночная выгрузка не сдвигает чужую.

    Имена — те, что видны в БД (латиница): table_name="Document_ZakazKlienta",
    date_field="DataOtgruzki". По той же причине, что и у обработчиков: настраивая выгрузку,
    смотрят в базу и в реестр metadata_objects_1c (колонки object_full_name_en / fields_en), а не
    в конфигуратор. Оригинальные имена 1С тоже принимаются — разбирается само, см. _resolve.

    Период необязателен и задаётся с обеих сторон независимо: нет date_from — с начала, нет
    date_to — до конца, нет обеих — объект целиком. Граница timedelta — это смещение НАЗАД от
    текущей даты, вычисляемое в момент срабатывания: date_from=timedelta(days=3) — скользящий хвост
    за последние трое суток. Фиксированные date/datetime/str уходят в full_load как есть.

    Гонки. С потоком изменений — уже решено в full_load: снимок берёт отметку на каждую страницу
    и не трогает строки, переписанные после неё (см. DBWriter1C.save, full_load_started_at). Со
    второй полной выгрузкой ТОГО ЖЕ объекта (фоновая выгрузка репликатора или второе расписание)
    прогон разводится claim'ом: занятый объект — срабатывание пропускается, ждём следующего по
    расписанию. Догонять пропущенное незачем: следующий прогон прочитает тот же период заново.

    Пометка пропавших строк включена ВСЕГДА и параметром не управляется. Физическое удаление в
    обмен не приходит вовсе, а у независимого регистра нет и регистратора, по которому можно было
    бы удалить набор, — поэтому расписание остаётся единственным местом, где удаление вообще
    видно. Выключить его значило бы тихо сломать всё, что построено поверх: витрина замечает
    изменения по merged_on, а у неудалённой строки он не двигается. Помечается только то, что
    прогон читал: при заданном периоде — строки этого периода, и каждая перед пометкой
    перепроверяется в 1С (из окна строка могла уехать, а не исчезнуть).

    full_history_on_first_run — первый прогон идёт БЕЗ границ периода, то есть читает объект
    целиком. Иначе объекта, которого нет в плане обмена, не появится вовсе: полную выгрузку по
    флагу full_load_is_required репликатор заводит только тем, кто пришёл в пакете изменений
    (Replicator1C._save_changes → require_full_load_if_new), а такой объект в пакете не появляется
    никогда. Скользящее окно при этом собирало бы только свежий хвост, и история не приехала бы.
    Признак «уже выгружался» — last_full_load_dt в metadata_objects_1c: он в БД, поэтому
    перезапуск процесса историю заново не перечитывает.

    Время локальное — в контейнере задавайте TZ, иначе "0 3 * * *" сработает по UTC.
    """

    def __init__(self, replicator, table_name: str, *, cron: str,
                 date_field: str | None = None,
                 date_from: DateBound = None, date_to: DateBound = None,
                 batch_size: int = 1000, full_history_on_first_run: bool = True):
        # Перехват SIGTERM/SIGINT — по той же причине, что у Replicator1C и HandlerLoop: run_forever
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
        # поэтому считаются при первом прогоне — конструкторы в проекте в сеть не ходят.
        self._object_name: str | None = None
        self._date_field_1c: str | None = None
        self._resolve_lock = threading.Lock()
        # Действующий StopSignal текущего run_forever — через него цикл останавливают снаружи.
        self._stop_signal: "StopSignal | None" = None

    def __repr__(self) -> str:
        return f'<FullLoadCron {self.table_name} {self.cron!r}>'

    def run_forever(self, max_runs: int = 0) -> None:
        """
        Блокирующий цикл — та же форма, что у Replicator1C.run_forever и HandlerLoop.run_forever:
        где ему крутиться, решает точка входа, а не библиотека.

        Останавливается по SIGTERM/SIGINT (перехват процессный, см. stop_signal) либо точечно через
        request_stop(); идущая выгрузка при этом доработает — цикл прерывается между прогонами.

        max_runs>0 ограничивает число прогонов (нужно тестам).
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
        """Просит идущий run_forever завершиться после текущего прогона — то же, что SIGTERM, но
        программно. Нужна циклу в рабочем потоке: своего перехвата сигналов у него нет."""
        if self._stop_signal is not None:
            self._stop_signal.requested = True

    def run_once(self) -> int:
        """
        Один прогон: full_load объекта за вычисленный на этот момент период. Возвращает число
        реально изменённых строк (0 — изменения доезжали исправно), либо 0, если объект уже
        выгружается и срабатывание пропущено.

        Публичный, потому что цикл — не единственный способ его крутить: тем же вызовом проверяют
        настройку расписания, не дожидаясь трёх часов ночи.
        """
        object_name, date_field = self._resolve()
        with self.replicator.claim_full_load(object_name) as claimed:
            if not claimed:
                logger.warning("Full load of %s is already running, skipping this run",
                               self.table_name)
                return 0
            date_from = _bound(self.date_from)
            date_to = _bound(self.date_to)
            # Первый прогон — объект целиком: границы снимаем (см. класс). Проверяем это ВНУТРИ
            # claim, чтобы отметка о выгрузке не успела появиться между проверкой и прогоном.
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
                date_from=date_from, date_to=date_to)
            if first_run:
                # Прогон прочитал объект ЦЕЛИКОМ — это и есть полная выгрузка, и отметить её надо
                # ровно так же, как её отмечает фоновый воркер репликатора. Без этого
                # was_fully_loaded осталась бы ложной, и КАЖДОЕ следующее срабатывание снова читало
                # бы всю историю. Обычный прогон за период реестр не трогает: объект целиком он не
                # читал, и объявлять его выгруженным нельзя — это отменило бы фоновую выгрузку,
                # которая ещё должна собрать базу.
                self.replicator.metadata.mark_full_loaded(
                    object_name, rows_modified=rows_modified,
                    minutes=round((time.monotonic() - started) / 60, 3))
        logger.info("Scheduled full load of %s (%s..%s) modified %s rows",
                    self.table_name, date_from or '', date_to or '', rows_modified)
        return rows_modified

    def _resolve(self) -> tuple[str, str | None]:
        """
        Имена 1С для объекта и поля даты по тому, что задано в расписании.

        Транслитерация детерминирована (NameMapper1C), поэтому обратное соответствие ищется прямым
        перебором метаданных — отдельная таблица соответствий не нужна. Имя 1С, написанное как
        есть, тоже принимается: сначала пробуем его.

        Считается один раз (метаданные ради этого грузятся, если ещё не загружены) и запоминается:
        состав объектов между прогонами не меняется, а $metadata — не бесплатный запрос.
        """
        with self._resolve_lock:
            if self._object_name is not None:
                return self._object_name, self._date_field_1c

            metadata = self.replicator.metadata
            if not metadata.is_loaded:
                metadata.get_metadata()

            # Разбор имён — общий с full_load (MetadataReader1C.resolve_*): обе формы, имя 1С и
            # имя таблицы/колонки в БД. Раньше это же правило жило здесь своей копией.
            object_name = metadata.resolve_object_name(self.table_name)
            date_field = (metadata.resolve_field_name(object_name, self.date_field)
                          if self.date_field else None)

            logger.info("Schedule %s resolved to 1C object %s (date_field=%s)",
                        self.table_name, object_name, date_field)
            self._object_name, self._date_field_1c = object_name, date_field
            return object_name, date_field


def _bound(bound: DateBound):
    """Граница периода для full_load: timedelta — смещение назад от сегодняшней даты (считается
    в момент прогона, поэтому окно едет вместе с процессом), остальное — как передали."""
    if isinstance(bound, timedelta):
        return date.today() - bound
    return bound
