"""
Обработчики изменений загруженных таблиц и циклы их выполнения.

Обработчик объявляет ON — список имён таблиц БД — и метод handle(context). Можно
наследоваться от Handler, передать модуль с ON/handle или функцию с атрибутом ON.
Для каждого обработчика создаётся HandlerLoop; его run_forever запускается отдельным
потоком или процессом. Пример настройки — config/runner.py.

Репликатор сообщает об изменениях через onecdc_handlers. Передавать ему экземпляры
обработчиков не требуется: достаточно общей БД и схемы. Порядок выполнения разных
обработчиков не определён; зависимости витрин нужно учитывать в пользовательском коде.

В ON перечисляются все читаемые таблицы: подписка используется и для сигналов, и для
расчёта безопасной верхней границы окна. Данные не передаются в контексте; обработчик
сам выбирает их по merged_on > context.last_run_at. Окно учёта — (last_run_at, boundary].
Обе границы берутся по часам БД. Первый запуск получает EPOCH (1900-01-01 UTC).

boundary ограничивается стартами незавершённых записей из WriteTracker, чтобы курсор
не пропустил строки, получившие merged_on до коммита. Запрос может читать и правее
boundary: тогда видимые новые строки будут повторно обработаны в следующем окне.

После сбоя между работой обработчика и сохранением last_run_at окно повторяется.
Доставка имеет семантику at-least-once: пользовательский код должен быть идемпотентным,
в том числе при отправке во внешние системы. ON_FULL_LOAD=False отключает сигналы полной
выгрузки, но не исключает её строки из будущей выборки по окну.

Полная пересборка назначается через full_rebuild_is_required. Генератор rebuild может
делить её на блоки; после каждого yield цикл сохраняет метку и применяет накопившиеся
изменения в том же потоке. Подробности контракта — в Handler и README_API.md.
"""

import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import ModuleType
from typing import Any, Callable, Iterable, Iterator

from sqlalchemy import (ARRAY, Boolean, case, Column, ColumnElement, DateTime, Engine, exists,
                        Float, func, insert, MetaData, or_, select, String, Table,
                        text, update)

from onecdc.common_functions import DB_NOW_WITH_TIMEZONE, instance_owner
from onecdc.db_logs import (_check_create_schema, align_merge_timestamps,
                            create_table_if_absent)
from onecdc.lease import LEASE_ROLE_TTL, Lease, lease_engine
from onecdc.logging_config import LOAD_MODE_HANDLER, get_logger, load_mode
from onecdc.stop_signal import StopSignal, install_signal_handlers
# Реестр незавершённых merge переехал в свой модуль (им пользуется и репликатор). Имена
# реэкспортируются: HandlerLoop прижимает к нему границу окна, а внешний код и тесты импортируют
# WriteTracker отсюда с первых версий.
from onecdc.write_tracker import (MERGE_HEARTBEAT_PERIOD, MERGE_HEARTBEAT_TTL, WRITES_TABLE,
                                  WriteTracker, _writes_table)

logger = get_logger(__name__)

HANDLERS_TABLE = "onecdc_handlers"

# Метка сигнала об изменении подписанной таблицы (см. _handlers_table). Вынесена в
# константу: на неё смотрят и перенос со старого булева флага, и сам цикл.
UPDATE_REQUESTED_FIELD = "update_requested_at"
# Аренда имени обработчика — колонки вынесены, потому что по ним работает общий Lease.
LEASE_OWNER_FIELD = "lease_owner"
LEASE_HEARTBEAT_FIELD = "lease_heartbeat_at"

# Откуда пришло изменение: пакет изменений или страница полной выгрузки.
SOURCE_CHANGES = 'changes'
SOURCE_FULL_LOAD = 'full_load'
# Первый проход после старта процесса. Грязные отметки живут в памяти и перезапуск их не переживают,
# а работа могла остаться незавершённой (упали между обработкой и записью last_run_at, или процесс
# просто стоял, пока данные грузил кто-то другой). Поэтому на старте каждый обработчик считается
# грязным один раз: если делать нечего, его SELECT по окну просто вернёт пусто.
SOURCE_STARTUP = 'startup'
# Заказана пересборка витрины (full_rebuild_is_required). Отдельный повод к запуску: флаг ставят
# руками в таблице, и никаких изменений за ним не приходит — ждать сигнала можно вечно.
SOURCE_REBUILD = 'full_rebuild'
# Репликатор поставил update_requested_at: подписанная таблица изменилась. Так выглядит сигнал,
# пришедший через БД, — от репликатора в другом процессе или контейнере.
SOURCE_DB_SIGNAL = 'db_signal'

# Пауза холостого цикла потока обработчиков. Нужна не для опроса (о новых данных сообщает событие),
# а чтобы дождаться истечения MIN_INTERVAL и отпустившей границы окна, не занимая CPU.
IDLE_POLL = 1.0

# Пауза после падения обработчика. Окно при падении не сдвигается и отметка остаётся грязной,
# поэтому без паузы сломанный обработчик повторялся бы каждый холостой цикл.
RETRY_DELAY = 60.0

# Как часто репликатор перечитывает подписки обработчиков (update_on) из handlers. Меняются они
# только при перезапуске обработчика, а читать их на каждый сохранённый объект — лишние запросы:
# при полной выгрузке это запрос на страницу.
SUBSCRIPTIONS_TTL = 30.0


# Начало времён: то, что поступает в context.last_run_at, когда отметки прошлого прогона ещё нет
# (первый запуск или запрошена пересборка). Заведомо раньше любого merged_on и при этом валидная
# дата для всех поддерживаемых СУБД. В колонке onecdc_handlers.last_run_at на её месте NULL — там он
# значит «ни разу не отрабатывал», и по нему цикл выбирает пересборку; в контекст этот NULL не
# доходит, чтобы обработчику не приходилось разбирать None у себя в WHERE.
#
# С поясом, как и все служебные отметки: это значение сравнивается с ними и с merged_on. UTC здесь
# ничего не значит сверх «очень давно» — на таком расстоянии смещение роли не играет.
EPOCH = datetime(1900, 1, 1, tzinfo=timezone.utc)




@dataclass(frozen=True)
class HandlerContext:
    """
    Что обработчик получает на вход. Данных здесь нет намеренно: обработчик делает свой SELECT —
    так он не зависит от того, в каком виде их прочитал репликатор, и переживает перезапуск процесса
    (всё его состояние — в БД).
    """

    engine: Engine
    schema: str | None
    # Осталось от тех времён, когда dbmerge клал промежуточную таблицу в обычную схему. На
    # PostgreSQL он заводит её настоящей TEMPORARY и переданную схему обнуляет сам, поэтому
    # передавать её в свой dbmerge обработчику больше незачем. Поле сохранено, чтобы не ломать
    # написанные обработчики.
    temp_schema: str | None
    # Границы окна по часам БД: (last_run_at, boundary]. Обе — всегда datetime: первый прогон и
    # пересборка получают в last_run_at начало времён (EPOCH), а не None, поэтому обработчик пишет
    # `merged_on > context.last_run_at` одинаково во всех прогонах. Что это сборка с нуля, видно из
    # соседнего full_rebuild — гадать по None не нужно.
    last_run_at: datetime
    boundary: datetime
    # Витрину просят собрать заново: обработчик ещё ни разу не отрабатывал либо кто-то заказал
    # пересборку (handlers.full_rebuild_is_required). Окно при этом открыто с начала времён —
    # обработчику, который считает только по нему, ничего специально делать не надо. Флаг нужен
    # тем, у кого пересборка идёт иначе: например, не одним merge, а по частям.
    full_rebuild: bool
    # Метка последнего блока, который пересборка успела завершить до перезапуска процесса (см.
    # Handler.rebuild). None — начинаем с начала. Смысл метки известен только обработчику: он
    # сам нарезал блоки, он же и решает, какие из них пропустить.
    rebuild_from: str | None
    # Условие «имя этого обработчика всё ещё наше», годное прямо в SQL: подставьте его в
    # source_condition/delete_condition своего merge —
    #
    #     merge.exec(source_condition=and_(source_key.in_(groups), context.lease_is_mine))
    #
    # Проверка внутри SQL учитывает владение в снимке этого оператора. Она точнее проверки
    # перед долгим handle(), но не блокирует передачу аренды до коммита. Уже выполненные
    # операторы не откатываются автоматически при потере аренды; внешние действия обработчика
    # это условие также не защищает. Повторное выполнение должно быть идемпотентным.
    lease_is_mine: Any
    # Что привело к вызову. Только для логов: выбирать данные обязательно по окну.
    #
    # Точности тут немного, и это осознанно. Репликатор сообщает об изменении меткой времени в
    # handlers, а метка не несёт ни имени изменившейся таблицы, ни источника — иначе пришлось бы
    # городить в таблице очередь событий вместо одной колонки. Поэтому objects — это весь ON
    # обработчика,
    # а sources — db_signal (изменение), startup (первый проход процесса) или full_rebuild (заказ
    # пересборки). Отличить полную выгрузку от пакета изменений по sources нельзя.
    # Подписку на сигналы полной выгрузки отключают через ON_FULL_LOAD.
    objects: frozenset[str]
    sources: frozenset[str]
    logger: object


class Handler:
    """
    Базовый класс обработчика с помощниками для SQL-запросов.

    Объявите непустой список ON с именами таблиц БД и реализуйте handle(context).
    В HandlerLoop передаётся экземпляр класса. NAME по умолчанию совпадает с именем класса
    и служит ключом состояния в общей схеме; независимым экземплярам нужны разные имена.

    setup вызывается перед первой обработкой, rebuild задаёт необязательную пересборку
    по блокам. Состояние экземпляра живёт только в процессе; курсор окна и метка пересборки
    сохраняются в onecdc_handlers. Примеры — config/handlers/.
    """

    # Таблицы (транслит, как в БД), на изменения которых реагирует обработчик. Это же список
    # того, что он читает: по нему считается верхняя граница окна, поэтому перечислять надо все
    # читаемые таблицы, а не только те, ради которых обработчик написан.
    ON: list[str] = []
    # Вызывать ли на страницах полной выгрузки. False — для обработчиков без подписки на полную выгрузку (рассылка,
    # отправка в очередь): иначе первоначальная выгрузка выстрелит по каждой странице.
    #
    # Выключение отписывает и от того, что находит только полная выгрузка: исчезнувших строк. Их
    # пометка сигналит тем же источником, и отличать её отдельно мы не стали намеренно — иначе
    # обработчик, отказавшийся от полной выгрузки, на первой же выгрузке получил бы поток событий об
    # исчезновении сотен тысяч строк, то есть ровно то, от чего отписывался.
    #
    # Потерей это не становится: пометка поднимает строке merged_on, а у спящего обработчика
    # last_run_at не двигается — окно только растёт, и строки дождутся в нём. Сигнал здесь
    # будильник, а не носитель события. Меняется лишь момент, когда обработчик за ними придёт:
    # ближайшее живое изменение любой из его таблиц откроет окно, и они поступят вместе со всем
    # остальным. Пометка на удаление в 1С при этом приходит обычным изменением (поле
    # DeletionMark), так что «объект удалили» такой обработчик узнаёт вовремя и без выгрузки.
    ON_FULL_LOAD: bool = True
    # Не чаще раза в N секунд. Ограничитель для тяжёлых витрин: во время полной выгрузки сигналы
    # идут постранично, и без паузы инкремент конкурировал бы с самой выгрузкой за БД.
    MIN_INTERVAL: float = 0
    # Имя (ключ в onecdc_handlers). None — имя класса. Переименование заводит новую строку состояния,
    # то есть первый прогон после переименования пойдёт с начала времён.
    NAME: str | None = None

    def __init__(self, name: str | None = None):
        """
        Задать NAME экземпляра; в наследнике вызывайте super().__init__(name).

        Разным обработчикам в одной схеме нужны разные имена. Одно имя используется
        несколькими процессами только для выполнения одной и той же роли.
        """
        if name:
            self.NAME = name

    @property
    def name(self) -> str:
        return self.NAME or type(self).__name__

    def setup(self, context: HandlerContext) -> None:
        """
        Подготовить таблицы, представления и индексы перед первой обработкой.

        После успешного вызова HandlerLoop запоминает результат на время жизни экземпляра.
        При ошибке подготовка будет повторена при следующей попытке.
        """

    def handle(self, context: HandlerContext) -> None:
        """Полезная работа за окно (context.last_run_at, context.boundary]."""
        raise NotImplementedError(f"{self.name}.handle(context) is not implemented")

    def rebuild(self, context: HandlerContext) -> Iterator[str]:
        """
        Пересобрать витрину по блокам, возвращая метку каждого завершённого блока.

        Перед yield обработчик должен зафиксировать результат блока в БД. Затем HandlerLoop
        сохраняет метку и применяет накопившиеся изменения. Так витрина продолжает обновляться
        во время длительной пересборки.

            def rebuild(self, context: HandlerContext) -> Iterator[str]:
                for year in range(2020, 2027):
                    if context.rebuild_from and str(year) <= context.rebuild_from:
                        continue                      # блок завершён до перезапуска
                    self.merge_year(context, year)
                    yield str(year)

        Обработчик сам выбирает разбиение: по периоду, ключам, складам или организациям. Библиотека
        сохраняет только метку в rebuild_cursor и передаёт её в context.rebuild_from после перезапуска.

        Пересборка и обработка изменений выполняются последовательно в одном потоке. Каждый блок
        должен читать актуальные данные на момент своего выполнения. Заранее сохранённый снимок
        всей витрины использовать нельзя: он может затереть изменения, применённые между блоками.
        Примеры в config/handlers соблюдают это условие, не ограничивая чтение верхней границей окна.

        Реализация по умолчанию вызывает handle один раз с окном от EPOCH.
        """
        self.handle(context)
        yield 'all'

    # --- то, что иначе копируется из обработчика в обработчик ----------------------------------

    def changed_since(self, context: HandlerContext, *columns: ColumnElement) -> ColumnElement:
        """
        Построить OR-условие column > context.last_run_at для переданных колонок.

        Строка витрины может объединять несколько источников, каждый из которых изменяется
        независимо. Условие выбирает строку, если обновился хотя бы один источник.

        Если колонки принадлежат разным таблицам JOIN, на больших объёмах PostgreSQL может
        применить OR только после соединения и не использовать индексы для предварительного отбора.
        В таком случае рассмотрите UNION ALL с отдельной ветвью для каждой таблицы: её условие
        можно применить до соединения. Примеры и замеры приведены в config/handlers. Для колонок
        одной таблицы PostgreSQL может объединить индексные сканирования через BitmapOr.

        Верхняя граница в это условие не включена. Безопасный сдвиг курсора обеспечивает
        context.boundary, ограниченная началами незавершённых merge. Уже видимая строка правее
        boundary будет обработана сейчас и повторно в следующем окне. Это позволяет раньше
        обновить витрину, но добавляет повторную работу.

        Если повторная обработка нежелательна, например при платной отправке во внешнюю систему,
        добавьте column <= context.boundary.
        """
        return or_(*[column > context.last_run_at for column in columns])

    def execute(self, context: HandlerContext, sql: str, **params) -> None:
        """
        Выполнить SQL в отдельной транзакции, передав именованные параметры из params.

        Запрос передаётся в sqlalchemy.text без подстановок. Значения задавайте через :name,
        идентификаторы формируйте и экранируйте при сборке текста запроса.
        """
        with context.engine.begin() as conn:
            conn.execute(text(sql), params)

    def query(self, context: HandlerContext, sql: str, **params) -> list:
        """
        Выполнить SQL с именованными параметрами и вернуть список строк.

        Соединение освобождается до возврата; результат не удерживает открытый курсор.
        """
        with context.engine.connect() as conn:
            return conn.execute(text(sql), params).all()


@dataclass(frozen=True)
class _ResolvedHandler:
    """
    Нормализованный обработчик: то, с чем работает HandlerLoop, независимо от того, чем он был
    объявлен (экземпляр Handler, модуль или функция). Собирается функцией as_handler.

    Имя с подчёркиванием, потому что снаружи этот класс не нужен: пользователь объявляет
    обработчика, а не собирает его нормализованный вид.
    """

    name: str
    on: frozenset[str]
    on_full_load: bool
    min_interval: float
    handle: Callable[[HandlerContext], None]
    setup: Callable[[HandlerContext], None] | None = None
    # Пересборка по блокам (см. Handler.rebuild). Не объявлена — подставляется обёртка вокруг
    # handle: один блок, прежнее поведение.
    rebuild: Callable[[HandlerContext], object] | None = None


def as_handler(obj) -> _ResolvedHandler:
    """
    Приводит объявленный пользователем обработчик к _ResolvedHandler.

    Принимается всё, у чего есть `handle` и непустой `ON`: экземпляр Handler, модуль
    (`from handlers import zakazy` → модуль целиком) или функция с теми же атрибутами. Нужны
    только имя, набор объектов и вызываемое — обязательного базового класса нет.
    """
    if isinstance(obj, _ResolvedHandler):
        return obj
    if isinstance(obj, type):
        # Класс вместо экземпляра — типичная опечатка (`ZakazyKlientov` вместо `ZakazyKlientov()`).
        # Инстанцировать за пользователя нельзя: конструктор может требовать аргументов.
        raise TypeError(f"Handler {obj.__name__} is passed as a class, not as an instance: "
                        f"use {obj.__name__}() in the handlers list")

    handle = getattr(obj, 'handle', None)
    if handle is None and callable(obj):
        handle = obj
    if not callable(handle):
        raise AttributeError(f"Handler {obj!r} has no callable handle(context)")

    declared_on = getattr(obj, 'ON', ()) or ()
    if isinstance(declared_on, str):
        # Строка — это коллекция символов, и frozenset("Catalog_X") дал бы множество букв. Такой
        # обработчик регистрировался без единого возражения и не вызывался никогда: ни ошибки, ни
        # предупреждения, витрина просто не обновлялась. Забыть скобки при одной таблице —
        # самая естественная опечатка из возможных, поэтому ловим её явно.
        raise TypeError(f"Handler {getattr(obj, 'NAME', obj)!r}: ON must be a list of table names, "
                        f"not a string — write ON = [{declared_on!r}]")
    on = frozenset(declared_on)
    if not on:
        raise AttributeError(f"Handler {_handler_name(obj)} has empty ON: it would never be called")
    # Подписка идёт по именам таблиц (транслит), а не по именам объектов 1С. Имя 1С в ON выглядит
    # правдоподобно, но не совпадёт ни с чем и обработчик просто никогда не вызовут — молча.
    # Ловим по не-ASCII символам и отправляем за точным именем в реестр: назвать его здесь нельзя,
    # имя таблицы закреплено в БД и при коллизии транслита отличается от него (см. name_mapper),
    # а базы в этой точке — регистрация обработчика — ещё нет.
    for name in sorted(on):
        if not name.isascii():
            raise ValueError(
                f"Handler {_handler_name(obj)}: ON must list TABLE names as they appear in the "
                f"database, not 1C object names. Look up {name!r} in "
                f"onecdc_metadata_objects.object_full_name_en")

    setup = getattr(obj, 'setup', None)
    rebuild = getattr(obj, 'rebuild', None)
    if not callable(rebuild):
        # Пересборка не разбита на блоки — один блок, весь handle() целиком.
        def rebuild(context, _handle=handle):
            _handle(context)
            yield 'all'
    return _ResolvedHandler(name=_handler_name(obj), on=on,
                            on_full_load=bool(getattr(obj, 'ON_FULL_LOAD', True)),
                            min_interval=float(getattr(obj, 'MIN_INTERVAL', 0)),
                            handle=handle, setup=setup if callable(setup) else None,
                            rebuild=rebuild)


def _handler_name(obj) -> str:
    """Имя обработчика: NAME → .name (Handler) → имя модуля/функции без пути пакета."""
    name = getattr(obj, 'NAME', None) or getattr(obj, 'name', None)
    if isinstance(name, str) and name:
        return name
    if isinstance(obj, ModuleType) or callable(obj):
        return obj.__name__.rsplit('.', 1)[-1]
    return type(obj).__name__


def _handlers_table(metadata: MetaData, schema_name: str | None) -> Table:
    return Table(
        HANDLERS_TABLE, metadata,
        Column("name", String(255), primary_key=True),
        Column("enabled", Boolean, nullable=False),
        Column("last_run_at", DateTime(timezone=True), nullable=True),
        Column("last_error", String, nullable=True),
        # На что обработчик подписан. Заполняет он сам при старте, читает — репликатор: так
        # репликатору не нужны ни объекты обработчиков, ни их код, и они могут жить в другом
        # процессе или вообще в другом контейнере.
        Column("update_on", ARRAY(String), nullable=True),
        # Вызывать ли на страницах полной выгрузки. Объявляет обработчик, читает репликатор: сам
        # обработчик по метке update_requested_at источник изменения не различает, поэтому решение
        # принимается на стороне того, кто флаг поднимает.
        Column("on_full_load", Boolean, nullable=False, server_default=text('true')),
        # Репликатор увидел изменение подписанной таблицы: пишет сюда момент сигнала по часам БД
        # (NULL — незакрытых сигналов нет). Единственный способ, которым он вызывает обработчика:
        # поставить метку, а дальше тот сам увидит её своим циклом.
        #
        # Метка, а не булев флаг, — из-за сигнала, пришедшего посередине прогона: поднять true над
        # true нельзя, следа не остаётся, и прогон снимал флаг вместе с непрочитанным сигналом.
        # Метка же сдвигается вправо от границы окна, и снять её прогон уже не вправе (см.
        # _save_success): обработчик будет вызван ещё раз.
        Column(UPDATE_REQUESTED_FIELD, DateTime(timezone=True), nullable=True),
        # Заказ полной пересборки витрины — руками или автоматически (см. request_full_rebuild) —
        # и метрики последней пересборки. Названы по образцу onecdc_metadata_objects, где так же
        # устроена полная выгрузка объекта.
        Column("full_rebuild_is_required", Boolean, nullable=False, server_default=text('false')),
        Column("last_full_rebuild_dt", DateTime(timezone=True), nullable=True),
        # Аренда имени: витрину этого обработчика считает ровно один процесс. Поднять второй с тем
        # же NAME ничто не мешает (rolling restart, дублированный контейнер), а CAS по last_run_at
        # ловит только отметку: к моменту проверки витрина уже переписана, а у отправщика во
        # внешнюю систему письма ушли. Поэтому владение берётся ДО вызова кода обработчика.
        Column(LEASE_OWNER_FIELD, String(255), nullable=True),
        Column(LEASE_HEARTBEAT_FIELD, DateTime(timezone=True), nullable=True),
        Column("last_full_rebuild_minutes", Float, nullable=True),
        # Метка последнего завершённого блока идущей пересборки (см. Handler.rebuild). Нужна,
        # чтобы пересборка на десятки минут переживала перезапуск процесса: генератор блоков живёт
        # в памяти и рестарта не переживает, а метка лежит в БД и возвращается обработчику в
        # context.rebuild_from. NULL — пересборка не идёт либо не сделала ни одного блока.
        Column("rebuild_cursor", String(255), nullable=True),
        schema=schema_name,
    )


def _create_table(engine: Engine, table: Table) -> None:
    """Создание с общим переездом колонок плюс перенос данных, нужный только этой таблице."""
    existing, added = create_table_if_absent(engine, table)
    if added:
        _carry_over_update_flag(engine, table, existing, added)


def _carry_over_update_flag(engine: Engine, table: Table, existing: set, added: set) -> None:
    """
    Переносит незакрытые сигналы из старого булева update_is_required в новую метку
    update_requested_at — один раз, в момент, когда колонка только что добавлена.

    Без этого обработчик, стоявший в очереди на момент обновления библиотеки, дождался бы только
    следующего изменения подписанной таблицы: его витрина всё это время висела бы неактуальной, и
    молча. Само значение метки — «сейчас»: когда именно пришёл потерянный сигнал, уже не узнать,
    а прогон от этого только перечитает чуть большее окно.

    Старую колонку не удаляем: она ничему не мешает (NOT NULL с дефолтом), а автоматически ронять
    колонку в чужой БД — не то, что библиотека вправе делать без спроса.
    """
    legacy = 'update_is_required'
    if UPDATE_REQUESTED_FIELD not in added or legacy not in existing:
        return
    with engine.begin() as conn:
        result = conn.execute(update(table)
                              .where(text(f'{legacy} = true'))
                              .values(update_requested_at=DB_NOW_WITH_TIMEZONE))
    if result.rowcount:
        logger.info("Carried over %s pending signal(s) from %s to %s",
                    result.rowcount, legacy, UPDATE_REQUESTED_FIELD)


class HandlerSignals:
    """
    Сторона репликатора: как он вызывает обработчиков, не зная о них ничего.

    Обработчик при старте объявляет себя в onecdc_handlers и перечисляет в update_on таблицы, на
    которые подписан. Репликатор перечитывает эту таблицу и, увидев изменение подписанной таблицы,
    просто ставит метку update_requested_at. Дальше обработчик сам заметит её своим циклом.

    Отсюда и берётся возможность разнести их как угодно: репликатору не нужны ни объекты
    обработчиков, ни их импорт, ни общий с ними процесс — только общая БД. Обработчик может жить
    в отдельном контейнере, его можно перезапускать и обновлять независимо.

    Подписки кэшируются и перечитываются не чаще SUBSCRIPTIONS_TTL: они меняются только при
    перезапуске обработчика, а читать их на каждый сохранённый объект — лишние запросы (при полной
    выгрузке это запрос на страницу).
    """

    def __init__(self, engine: Engine, schema: str | None):
        self.engine = engine
        self.schema_name = _check_create_schema(engine, schema)
        self.table = _handlers_table(MetaData(), self.schema_name)
        # Таблицу заводит и репликатор тоже: обработчиков в этом процессе может не быть, а
        # поднимать флаг всё равно надо — иначе первый же сигнал упал бы на отсутствующей таблице.
        _create_table(engine, self.table)
        self._lock = threading.Lock()
        self._subscriptions: dict[str, list[tuple[str, bool]]] = {}
        self._read_at = 0.0

    def subscribers(self, table_name: str, source: str = SOURCE_CHANGES) -> list[str]:
        """
        Вернуть имена обработчиков, подписанных на таблицу и данный источник изменения.

        Подписка определяется update_on и on_full_load. Источник проверяется при отправке сигнала:
        сам обработчик видит только update_requested_at и по этой метке не может отличить пакет
        изменений от полной выгрузки.
        """
        with self._lock:
            if time.monotonic() - self._read_at > SUBSCRIPTIONS_TTL:
                self._subscriptions = self._read_subscriptions()
                self._read_at = time.monotonic()
            subscribed = self._subscriptions.get(table_name, [])
        if source == SOURCE_FULL_LOAD:
            return [name for name, on_full_load in subscribed if on_full_load]
        return [name for name, _ in subscribed]

    def _read_subscriptions(self) -> dict[str, list[tuple[str, bool]]]:
        """
        Вернуть подписки в виде {таблица: [(имя обработчика, on_full_load)]}.

        Учитываются и отключённые обработчики. Их цикл проверяет enabled перед запуском, а сигналы
        продолжают накапливаться вместе с необработанным окном. Благодаря этому после включения
        обработчик сразу выполняет пропущенную работу, не ожидая нового изменения в источнике.
        """
        t = self.table
        subscriptions: dict[str, list[tuple[str, bool]]] = {}
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(t.c.name, t.c.update_on, t.c.on_full_load)).all()
        for name, update_on, on_full_load in rows:
            for table in update_on or ():
                subscriptions.setdefault(table, []).append((name, bool(on_full_load)))
        return subscriptions

    def signal(self, table_name: str, source: str, conn=None) -> None:
        """
        Ставит подписчикам таблицы метку сигнала. Подписчиков нет — ничего не делаем.

        Метка всегда перезаписывается текущим временем БД, а не ставится «если пусто»: обработчик
        снимает её, только если она не правее границы его окна, а значит хранить надо последний
        сигнал. С самым ранним сигнал, пришедший в середине прогона, не сдвинул бы значение — и
        был бы снят вместе с обработанными.

        conn — открытое соединение вызывающего: репликатор ставит сигнал одной транзакцией со
        снятием строки реестра идущих merge (см. WriteTracker._finish). Порознь между ними
        помещается сбой, после которого данные закоммичены, а сигнала нет, — и повтор пакета его
        уже не восстановит.
        """
        names = self.subscribers(table_name, source)
        if not names:
            return
        statement = (update(self.table)
                     .where(self.table.c.name.in_(names))
                     .values(update_requested_at=DB_NOW_WITH_TIMEZONE))
        if conn is not None:
            conn.execute(statement)
        else:
            with self.engine.begin() as own:
                own.execute(statement)
        logger.info("Changed %s (%s) → update requested for %s",
                    table_name, source, ', '.join(sorted(names)))

    def request_full_rebuild(self, table_name: str, reason: str,
                             source: str = SOURCE_CHANGES) -> None:
        """
        Установить full_rebuild_is_required у обработчиков, подписанных на таблицу.

        Подписчики выбираются с учётом source, как в signal(). При source=SOURCE_FULL_LOAD
        обработчики с ON_FULL_LOAD=False исключаются. reason используется в сообщении журнала.

        Метод вызывается явно. Добавление колонки сейчас приводит только к предупреждению
        в Replicator._request_rebuild_for_added_fields: пользователь сам обновляет код витрины
        и при необходимости назначает пересборку.
        """
        names = self.subscribers(table_name, source)
        if not names:
            return
        with self.engine.begin() as conn:
            conn.execute(update(self.table)
                         .where(self.table.c.name.in_(names))
                         .values(full_rebuild_is_required=True))
        logger.info("Changed %s (%s) → full rebuild requested for %s",
                    table_name, reason, ', '.join(sorted(names)))


class HandlerLoop:
    """
    Выполнять один обработчик по сигналам из onecdc_handlers.

    Цикл регистрирует подписку, захватывает имя обработчика, рассчитывает окно и после
    успеха сохраняет его границу. Несколько сигналов могут объединиться в один запуск.
    Обработчики с разными именами работают независимо и могут запускаться в разных
    процессах. Их код выполняется в потоке HandlerLoop, а не в потоке репликации.
    """

    def __init__(self, engine: Engine, schema: str | None, handler,
                 temp_schema: str | None = None,
                 write_tracker: "WriteTracker | None" = None):
        # Перехват SIGTERM/SIGINT — по той же причине, что у Replicator: run_forever уходит в
        # пул потоков, а поставить перехват можно только из главного (см. stop_signal).
        install_signal_handlers(quiet=False)
        # Объявление разбирается и проверяется ДО первого обращения к БД: кривое (нет handle,
        # пустой ON, класс вместо экземпляра) должно ронять старт, не оставив за собой ни схемы,
        # ни строки состояния.
        self.handler = as_handler(handler)
        self.name = self.handler.name

        self.engine = engine
        self.schema_name = _check_create_schema(engine, schema)
        self.schema = schema
        # Витрина — такая же таблица с merged_on, и её отметка сравнивается с нашим курсором.
        # Цикл обработчиков поднимают и отдельно от репликатора, поэтому миграция зовётся и здесь.
        align_merge_timestamps(engine, schema)
        # Значение сохраняется в контексте для совместимости с пользовательским кодом.
        # См. HandlerContext.temp_schema: параметр оставлен ради совместимости, dbmerge на
        # PostgreSQL его не использует.
        self.temp_schema = temp_schema
        # Реестр незавершённых merge. По умолчанию — общий, из таблицы onecdc_writes_in_process
        # (см. свойство writes); подменяется только в тестах.
        self._writes = write_tracker

        self.table = _handlers_table(MetaData(), self.schema_name)
        _create_table(engine, self.table)
        self._register_handler()

        self._lock = threading.Lock()
        # Грязные отметки: что изменилось и откуда пришло. Пустой набор объектов = ждать нечего.
        # Сразу непустой: отметки живут в памяти и перезапуск процесса не переживают, поэтому
        # после старта обработчик обязан отработать хотя бы раз.
        self._dirty_objects: set[str] = set(self.handler.on)
        self._dirty_sources: set[str] = {SOURCE_STARTUP}
        # Разовая подготовка (setup) уже сделана.
        self._prepared = False
        # Когда обработчику снова можно бежать (MIN_INTERVAL), по монотонным часам.
        self._next_allowed_at = 0.0
        # Момент, когда репликатор точно узнал о наших подписках, и повторная догонялка.
        #
        # Стартовый прогон (выше) закрывает только то, что было ДО него. А репликатор про нас ещё
        # не знает: подписки он держит в кэше до SUBSCRIPTIONS_TTL и читает их одним запросом на
        # все обработчики. Значит изменения, сохранённые сразу после нашего старта, сигнала не
        # получат — и подождут следующего изменения этой таблицы, которого может не быть неделями.
        # Ошибок при этом в логе нет, всё выглядит здоровым.
        #
        # Поэтому после истечения TTL смотрим окно ещё раз. Обычно оно пустое, и собственный
        # SELECT обработчика вернёт ноль строк, — это одна холостая проверка на запуск процесса.
        self._catch_up_at: float | None = time.monotonic() + SUBSCRIPTIONS_TTL + 1.0
        # Аренда имени: витрину этого обработчика считает ровно один процесс. Держится на всю
        # единицу работы — прогон целиком, а пересборку со всеми её блоками. Отпускать между
        # блоками нельзя: два процесса повели бы пересборку по очереди, и блок, досчитанный
        # первым позже, затёр бы группы, в которые второй уже внёс свежее. rebuild_cursor нужен
        # для того, чтобы пересборку подхватили после смерти процесса, а не чтобы её вели двое.
        # Для продления аренды используется отдельный пул: соединения должны оставаться
        # доступными, даже когда обработчик занял весь рабочий пул (см. lease_engine).
        self._lease = Lease(lease_engine(engine), lambda: self.table,
                            instance_owner(f'handler:{self.name}'),
                            subject='handler', key_field='name',
                            owner_field=LEASE_OWNER_FIELD,
                            heartbeat_field=LEASE_HEARTBEAT_FIELD)
        # Кого мы уже сообщили как владельца: лог только на переходах (см. Replicator._claim_node).
        self._lease_holder_logged: str | None = None
        # Действующий StopSignal текущего run_forever — через него цикл останавливают снаружи.
        self._stop_signal: "StopSignal | None" = None

    def __repr__(self) -> str:
        return f'<HandlerLoop {self.name}>'

    @property
    def writes(self):
        """
        Реестр незавершённых merge, по которому считается верхняя граница окна.

        По умолчанию — общий, из таблицы onecdc_writes_in_process: обработчик обязан видеть merge любого
        репликатора, в том числе работающего в другом процессе или контейнере. Его незакоммиченные
        строки имеют merged_on в прошлом, и граница, взятая как «сейчас», их бы перешагнула.
        Подменяется только в тестах.
        """
        if self._writes is None:
            self._writes = WriteTracker(self.engine, self.schema, f'handler:{self.name}')
        return self._writes

    def _register_handler(self) -> None:
        """
        Зарегистрировать обработчик и обновить его подписку в onecdc_handlers.

        Список update_on записывается при каждом запуске, чтобы соответствовать текущему Handler.ON.
        Репликатор читает его из БД, поэтому не зависит от Python-объекта обработчика и может работать
        в другом процессе. У нового обработчика last_run_at=NULL, что запускает обработку всей истории.
        """
        update_on = sorted(self.handler.on)
        with self.engine.begin() as conn:
            known = conn.scalar(select(self.table.c.name).where(self.table.c.name == self.name))
            if known is None:
                conn.execute(insert(self.table).values(
                    name=self.name, enabled=True, last_run_at=None, last_error=None,
                    update_on=update_on, on_full_load=self.handler.on_full_load))
                logger.info("Registered handler %s", self.name)
            else:
                conn.execute(update(self.table).where(self.table.c.name == self.name)
                             .values(update_on=update_on,
                                     on_full_load=self.handler.on_full_load))
        logger.info("Handler %s (update_on=%s, on_full_load=%s, min_interval=%ss)",
                    self.name, update_on, self.handler.on_full_load, self.handler.min_interval)

    # --- исполнение --------------------------------------------------------------------------

    def run_forever(self, poll_interval: float = IDLE_POLL) -> None:
        """
        Блокирующий цикл обработчика — та же форма, что у Replicator.run_forever: где ему
        крутиться, решает точка входа, а не библиотека.

        Опрос, а не ожидание события: сигнал приходит флагом в onecdc_handlers, поднять его может любой
        процесс, и подписаться на такое изменение нечем. Заодно опрос сам подхватывает всё
        остальное, что меняется в таблице снаружи, — enabled, заказ пересборки, обнулённую отметку.

        Останавливается по SIGTERM/SIGINT (перехват процессный, см. stop_signal) либо точечно через
        request_stop(). Текущий прогон при этом доработает: цикл прерывается между прогонами, а не
        посреди merge.
        """
        stop = StopSignal()
        self._stop_signal = stop
        with load_mode(LOAD_MODE_HANDLER):
            logger.info("Handler %s started", self.name)
            try:
                while not stop.requested:
                    try:
                        self.run_if_pending()
                    except Exception:
                        logger.exception("Handler %s dispatch failed", self.name)
                    stop.wait(poll_interval)
            finally:
                # Уходим аккуратно — отпускаем имя сразу, не заставляя сменщика ждать TTL.
                self.close()
            logger.info("Handler %s stopped", self.name)

    def close(self) -> None:
        """
        Освободить имя обработчика и запросить остановку потока отметки живости.

        Освобождение позволяет следующему экземпляру сразу занять имя, не ожидая TTL
        в 15 минут. Метод не останавливает активный handle: сначала завершите цикл.
        """
        self._lease.release(self.name)
        self._lease.close()

    def request_stop(self) -> None:
        """Просит идущий run_forever завершиться после текущего прогона — то же, что SIGTERM, но
        программно. Нужна циклу в рабочем потоке: своего перехвата сигналов у него нет."""
        if self._stop_signal is not None:
            self._stop_signal.requested = True

    def run_if_pending(self) -> None:
        """
        Один оборот цикла: прогон, если обработчик ждёт запуска, иначе ничего.

        Публичный, потому что цикл — не единственный способ его крутить: обработчика можно
        запускать и по расписанию снаружи (cron, вызов из своего кода), не поднимая run_forever.
        """
        if not self._claim_name():
            return

        if self._catch_up_at is not None and time.monotonic() >= self._catch_up_at:
            # Подписки репликатора к этому моменту точно перечитаны — если что-то прошло мимо
            # сигнала, пока он о нас не знал, это последний шанс увидеть его без нового изменения.
            self._catch_up_at = None
            self._mark_dirty(set(self.handler.on), {SOURCE_STARTUP})

        enabled, last_run_at, full_rebuild, update_required, cursor = self._read_state()
        if not enabled:
            # Выключен в таблице — грязные отметки не копим, иначе после включения он получит
            # окно, накопленное за всё время простоя, и посчитает его одним прогоном.
            self._take_dirty()
            return
        if update_required:
            # Репликатор увидел изменение подписанной таблицы и поднял флаг. Какая именно таблица —
            # не важно: выбирать данные всё равно по окну. Флаг снимется вместе с записью
            # last_run_at, и только при успехе (см. _save_success).
            self._mark_dirty(set(self.handler.on), {SOURCE_DB_SIGNAL})
        if full_rebuild:
            # Заказ пересборки сам ставит обработчик в очередь. Иначе он бы дожидался сигнала
            # об изменении подписанных объектов, а изменений может не быть неделями — флаг,
            # проставленный руками, так и лежал бы без дела.
            self._mark_dirty(set(self.handler.on), {SOURCE_REBUILD})
        if time.monotonic() < self._next_allowed_at:
            return
        # Пересборка идёт по блокам, и между ними тот же поток применяет накопившиеся изменения —
        # поэтому у неё свой драйвер, а не общий с инкрементом прогон. Непустой rebuild_cursor
        # означает незаконченную пересборку: её продолжаем, даже если флаг заказа уже снят.
        if full_rebuild or last_run_at is None or cursor is not None:
            self._run_rebuild(last_run_at, cursor)
        else:
            self._run(last_run_at, full_rebuild, update_required)

    def _check_lease_after_run(self) -> None:
        """
        Проверить владение именем после прогона и при потере аренды запросить пересборку.

        Если аренда перешла другому процессу во время работы, результаты двух прогонов могли
        перезаписать друг друга. Пересборка позволяет повторно согласовать витрину с загруженными
        данными, не обращаясь к 1С.

        Для обработчиков с on_full_load=False пересборка автоматически не назначается: отправщик
        во внешнюю систему мог бы повторно отправить всю историю. В этом случае записывается ERROR,
        а способ восстановления выбирает пользователь.
        """
        if self._lease.still_mine(self.name):
            return
        if not self.handler.on_full_load:
            logger.error(
                "Handler %s lost its lease while running: another process may have been building "
                "the same data mart, and results could have overwritten each other. A rebuild is "
                "NOT requested automatically because this handler opted out of backfill — decide "
                "and request it yourself if the data mart looks stale", self.name)
            return
        logger.error("Handler %s lost its lease while running — requesting a full rebuild, "
                     "its results may have overwritten those of another process", self.name)
        with self.engine.begin() as conn:
            conn.execute(update(self.table).where(self.table.c.name == self.name)
                         .values(full_rebuild_is_required=True))

    def _lease_condition(self):
        """Условие «имя всё ещё наше» для SQL обработчика (см. HandlerContext.lease_is_mine)."""
        t = self.table
        return exists().where(
            t.c.name == self.name,
            t.c[LEASE_OWNER_FIELD] == self._lease.owner,
            t.c[LEASE_HEARTBEAT_FIELD] >= DB_NOW_WITH_TIMEZONE
            - timedelta(seconds=LEASE_ROLE_TTL))

    def _claim_name(self) -> bool:
        """
        Захватывает имя обработчика. False — витрину считает другой процесс, проход пропускаем.

        Не ждём освобождения в цикле, а пропускаем проход и пробуем на следующем: процессы
        симметричны, роли ведущего нет, и кому повезёт в следующий раз — заранее не определено
        (см. lease). Поднятый рядом второй контейнер благодаря этому безвреден и ничего не требует
        от пользователя.
        """
        if self._lease.acquire(self.name):
            if self._lease_holder_logged is not None:
                logger.info("Handler %s is ours now", self.name)
                self._lease_holder_logged = None
            return True
        holder = self._lease.live_owners().get(self.name, '?')
        if self._lease_holder_logged != holder:
            logger.info("Handler %s is being run by %s, skipping this pass", self.name, holder)
            self._lease_holder_logged = holder
        return False

    def _read_state(self) -> tuple[bool, datetime | None, bool, bool, str | None]:
        """Состояние обработчика из БД: (включён, last_run_at, заказана ли пересборка, ждёт ли
        обновления, метка последнего блока пересборки). Читается на каждый проход — всё это
        меняется снаружи: руками, репликатором или чужим процессом."""
        t = self.table
        with self.engine.connect() as conn:
            row = conn.execute(select(t.c.enabled, t.c.last_run_at, t.c.full_rebuild_is_required,
                                      t.c.update_requested_at, t.c.rebuild_cursor)
                               .where(t.c.name == self.name)).first()
        if row is None:
            return False, None, False, False, None
        return (bool(row.enabled), row.last_run_at, bool(row.full_rebuild_is_required),
                row.update_requested_at is not None, row.rebuild_cursor)

    def _run(self, last_run_at: datetime, expected_rebuild: bool = False,
             update_requested: bool = False) -> None:
        """Обычный прогон за окно (last_run_at, boundary]. Пересборкой не занимается — у неё свой
        драйвер (_run_rebuild), который этот метод и вызывает между блоками.

        expected_rebuild — значение full_rebuild_is_required, прочитанное перед прогоном: во время
        пересборки флаг поднят, и охранное условие записи результата обязано этого ожидать."""
        # Грязные отметки забираем перед расчётом границы, а не после: сигнал приходит уже после
        # коммита своего merge, поэтому всё, что не попало в это окно, пришлёт сигнал позже и
        # взведёт обработчик заново. В обратном порядке такой сигнал мог бы быть съеден вместе со
        # снятыми отметками, и его строки ждали бы следующего, ничем не гарантированного изменения.
        objects, sources = self._take_dirty()
        if not objects:
            return

        started = time.monotonic()
        retry_delay = 0.0
        # Всё, что может упасть, — внутри try, включая расчёт границы. Иначе отметки, снятые выше,
        # пропадут вместе с исключением: обработчик перестанет вставать в очередь до следующего
        # изменения, а в onecdc_handlers не появится last_error, и со стороны БД он будет выглядеть
        # исправным. Так уже случалось на сравнении границы с last_run_at.
        window_start = last_run_at

        try:
            boundary = self.writes.boundary(self.handler.on)
            if boundary <= window_start:
                # Окно пустое или вывернутое: по читаемым объектам идёт merge, начавшийся раньше
                # прошлого прогона. Ничего не берём — отметки возвращаем, вернёмся к ним позже.
                logger.debug("Handler %s: boundary %s is not past last_run_at %s, waiting",
                             self.name, boundary, window_start)
                self._mark_dirty(objects, sources)
                return

            context = self._context(window_start, boundary, objects, sources,
                                    full_rebuild=False, rebuild_from=None)
            self._prepare(context)

            # Парная запись к «finished»: без неё по логу не отличить идущий прогон от
            # незапустившегося, а для долгой витрины это единственный признак, что она считает.
            # Границы окна пишем здесь же — по ним видно, какой отрезок обработчик забрал.
            logger.info("Handler %s: update started (objects=%s, window=%s … %s)",
                        self.name, sorted(objects), window_start, boundary)
            self.handler.handle(context)
        except Exception as error:
            logger.exception("Handler %s failed, retry in %ss", self.name, RETRY_DELAY)
            self._save_error(error)
            # Возвращаем отметки: окно не сдвинулось (last_run_at не записан), но без грязного
            # флага повтор случился бы только при следующем изменении — а его может и не быть.
            self._mark_dirty(objects, sources)
            retry_delay = RETRY_DELAY
        else:
            # last_run_at = граница, взятая ДО вызова: всё, что смёржилось за время работы
            # обработчика, окажется правее неё и попадёт в следующее окно, а не потеряется.
            elapsed = time.monotonic() - started
            self._check_lease_after_run()
            if self._save_success(boundary, last_run_at, expected_rebuild, update_requested):
                logger.info("Handler %s finished in %.1fs (objects=%s)",
                            self.name, elapsed, sorted(objects))
            else:
                # Пока обработчик работал, его состояние успели поменять снаружи — например,
                # заказали пересборку. Записать свой результат значило бы этот заказ молча
                # отменить, поэтому оставляем чужое значение и взводим обработчик заново.
                logger.info("Handler %s finished, but its state changed meanwhile — rerunning",
                            self.name)
                self._mark_dirty(objects, sources)
        finally:
            # Пауза до следующего прогона: обычно MIN_INTERVAL, после падения — RETRY_DELAY, чтобы
            # сломанный обработчик не повторялся каждую секунду и не заваливал лог трейсбеками.
            delay = max(self.handler.min_interval, retry_delay)
            if delay > 0:
                self._next_allowed_at = time.monotonic() + delay

    def _context(self, window_start: datetime, boundary: datetime, objects: Iterable[str],
                 sources: Iterable[str], full_rebuild: bool,
                 rebuild_from: str | None) -> HandlerContext:
        return HandlerContext(
            lease_is_mine=self._lease_condition(),
            engine=self.engine, schema=self.schema, temp_schema=self.temp_schema,
            last_run_at=window_start, boundary=boundary,
            objects=frozenset(objects), sources=frozenset(sources), full_rebuild=full_rebuild,
            rebuild_from=rebuild_from or None,
            logger=get_logger(f'onecdc.handler.{self.name}'))

    def _prepare(self, context: HandlerContext) -> None:
        """
        Один раз подготовить таблицы и представления перед первым handle.

        Повторять DDL при каждом сигнале полной выгрузки не требуется: сигналы поступают
        постранично, а CREATE OR REPLACE VIEW каждый раз устанавливал бы лишние блокировки.
        """
        if self.handler.setup is not None and not self._prepared:
            self.handler.setup(context)
            self._prepared = True

    def _stop_requested(self) -> bool:
        return self._stop_signal is not None and self._stop_signal.requested

    # --- пересборка по блокам -----------------------------------------------------------------

    def _run_rebuild(self, last_run_at: datetime | None, cursor: str | None) -> None:
        """
        Полная пересборка витрины блоками, которые нарезал сам обработчик (Handler.rebuild).
        Между блоками применяются накопившиеся изменения, поэтому витрина продолжает обновляться
        во время пересборки.

        Компромисс осознанный: это НЕ параллельность. Блок и инкремент выполняет один и тот же
        поток, поэтому одновременно они не работают никогда и затирать друг друга им нечем — а
        именно это и случилось бы, считай пересборка в своём потоке по снимку на её старте.
        Платим задержкой в один блок вместо задержки во всю пересборку.
        """
        objects, sources = self._take_dirty()
        started = time.monotonic()
        retry_delay = 0.0
        try:
            boundary = self.writes.boundary(self.handler.on)
            if cursor is None:
                cursor = ''
                # Заявляем пересборку начатой ДО первого блока: непустой курсор — это и есть
                # признак «идёт пересборка», по нему она продолжится после перезапуска. Заодно
                # снимаем флаг заказа, чтобы заказ, пришедший во время пересборки, поднял его
                # заново и не был проглочен её завершением.
                #
                # Первая в жизни пересборка ставит здесь же и отметку окна: иначе инкремент между
                # блоками получил бы окно «с начала времён», то есть вторую пересборку. Блоки
                # читают свои данные целиком, независимо от окна, а инкременту остаётся ровно то,
                # что изменилось уже во время пересборки.
                #
                # Обе записи — одной транзакцией. По отдельности между ними помещается падение
                # процесса, после которого отметка стоит, а курсора нет: обработчик выглядит
                # построенным, хотя не сделал ни одного блока.
                self._save_rebuild_started(boundary if last_run_at is None else None)
                if last_run_at is None:
                    last_run_at = boundary

            # Окно с начала времён — значением EPOCH, а не None: обработчик пишет
            # `merged_on > context.last_run_at` одним и тем же кодом и в пересборке, и в
            # инкременте, а что это пересборка, узнаёт из context.full_rebuild.
            context = self._context(EPOCH, boundary, objects or self.handler.on,
                                    sources or {SOURCE_REBUILD},
                                    full_rebuild=True, rebuild_from=cursor)
            self._prepare(context)

            logger.info("Handler %s: rebuild started (from block %s)", self.name, cursor or '—')
            blocks = self.handler.rebuild(context)
            if blocks is None:
                # rebuild оказался обычной функцией и всю работу сделал сам — это один блок.
                blocks = ()
            try:
                for label in blocks:
                    if not self._lease.still_mine(self.name):
                        # Пересборка идёт часами, и аренду за это время можно потерять молча.
                        # Между блоками — естественная точка разрыва: бросаем, а место остановки
                        # уже записано, и тот, кто перехватил имя, продолжит с него.
                        logger.error("Handler %s lost its lease during the rebuild — stopping at "
                                     "block %s, whoever holds the name now will continue",
                                     self.name, label)
                        return
                    self._save_rebuild_cursor(str(label))
                    logger.info("Handler %s: rebuild block %s done", self.name, label)
                    if self._stop_requested():
                        logger.info("Handler %s: rebuild paused at block %s, will resume on start",
                                    self.name, label)
                        return
                    self._run_increment_between_blocks()
            finally:
                # Прерванный генератор надо закрыть, иначе его finally/with (открытая транзакция,
                # временная таблица) отработают неизвестно когда — на сборке мусора.
                close = getattr(blocks, 'close', None)
                if close is not None:
                    close()
        except Exception as error:
            logger.exception("Handler %s rebuild failed, retry in %ss", self.name, RETRY_DELAY)
            self._save_error(error)
            self._mark_dirty(objects, sources)
            retry_delay = RETRY_DELAY
        else:
            elapsed = time.monotonic() - started
            self._save_rebuild_finished(elapsed)
            logger.info("Handler %s: rebuild finished in %.1fs", self.name, elapsed)
        finally:
            delay = max(self.handler.min_interval, retry_delay)
            if delay > 0:
                self._next_allowed_at = time.monotonic() + delay

    def _run_increment_between_blocks(self) -> None:
        """Применяет изменения, накопившиеся к этому моменту пересборки. Ничего не пришло —
        ничего и не делает; MIN_INTERVAL действует и здесь."""
        if time.monotonic() < self._next_allowed_at:
            return
        enabled, last_run_at, rebuild_requested, update_required, _ = self._read_state()
        if not enabled or last_run_at is None:
            return
        if update_required:
            self._mark_dirty(set(self.handler.on), {SOURCE_DB_SIGNAL})
        with self._lock:
            pending = bool(self._dirty_objects)
        if not pending:
            return
        self._run(last_run_at, rebuild_requested, update_required)

    def _take_dirty(self) -> tuple[set[str], set[str]]:
        """Забирает накопленные отметки, оставляя очередь пустой."""
        with self._lock:
            objects, sources = self._dirty_objects, self._dirty_sources
            self._dirty_objects, self._dirty_sources = set(), set()
        return objects, sources

    def _mark_dirty(self, objects: set[str], sources: set[str]) -> None:
        """Ставит обработчика в очередь, не затирая отметки, успевшие прилететь за это время.
        Используется и чтобы вернуть снятые отметки, когда прогон не состоялся или упал."""
        with self._lock:
            self._dirty_objects |= objects
            self._dirty_sources |= sources

    def _save_success(self, boundary: datetime, expected_last_run_at: datetime,
                      expected_rebuild: bool, update_requested: bool = False) -> bool:
        """
        Двигает отметку обработчика — но только если его состояние не поменяли снаружи, пока он
        работал (`expected_*` — значения, с которыми он стартовал). False, если не сдвинул.

        Сравнение, а не безусловная запись: пересборку могли заказать уже в середине прогона, и
        безусловная запись сняла бы этот заказ, не оставив следа. Условие собрано на равенстве, а
        не на IS DISTINCT FROM — тот есть не во всех СУБД; ветка IS NULL не нужна, потому что
        last_run_at=NULL сюда не приходит (см. ниже).

        expected_rebuild — что стояло в БД на старте прогона: по нему и проверяем, не заказали ли
        пересборку в середине.

        Пересборкой этот метод не занимается: её ведёт _run_rebuild — флаг заказа снимает
        _save_rebuild_started, метрики пишет _save_rebuild_finished. Сюда прогон приходит только
        инкрементом, в том числе тем, что идёт между блоками пересборки. Отсюда и datetime без
        None в expected_last_run_at: обработчика с пустой отметкой цикл отправляет в пересборку,
        а она к моменту первого инкремента отметку уже поставила (_save_rebuild_started).
        """
        t = self.table
        unchanged = t.c.last_run_at == expected_last_run_at
        values = {'last_run_at': boundary, 'last_error': None}
        if update_requested:
            # Метку снимаем только вместе с успешной отметкой: упавший прогон должен остаться
            # «ждущим обновления», иначе изменение, о котором сообщил репликатор, потерялось бы.
            #
            # И снимаем не всякую, а только ту, что не правее границы окна: всё, что правее, этот
            # прогон не читал. Сюда попадают и сигналы, пришедшие посередине прогона, и сигналы
            # о данных, которых в окне не было — граница прижата к незавершённым merge, а метку
            # ставит время сигнала. Оставшаяся метка заведёт обработчика ещё раз.
            values['update_requested_at'] = case(
                (t.c.update_requested_at <= boundary, None),
                else_=t.c.update_requested_at)
        with self.engine.begin() as conn:
            result = conn.execute(
                update(t)
                # Оптимистичного условия по метке здесь нет: её судьбу решает сравнение с границей
                # выше, и сигнал, пришедший за время прогона, оно сохраняет само.
                .where(t.c.name == self.name, unchanged,
                       t.c.full_rebuild_is_required == expected_rebuild)
                .values(**values))
        return result.rowcount > 0

    def _save_rebuild_started(self, first_watermark: datetime | None) -> None:
        """Отмечает пересборку начатой: пустой курсор (признак «идёт») и снятый флаг заказа. Для
        первой в жизни пересборки заодно ставит отметку окна — всё одной транзакцией, см.
        _run_rebuild."""
        values = {'rebuild_cursor': '', 'full_rebuild_is_required': False}
        if first_watermark is not None:
            values['last_run_at'] = first_watermark
        with self.engine.begin() as conn:
            conn.execute(update(self.table).where(self.table.c.name == self.name).values(**values))

    def _save_rebuild_cursor(self, cursor: str) -> None:
        """Запоминает, до какого блока дошла пересборка. Именно эта метка переживает перезапуск
        процесса: генератор блоков живёт в памяти, а продолжать надо с того же места."""
        with self.engine.begin() as conn:
            conn.execute(update(self.table).where(self.table.c.name == self.name)
                         .values(rebuild_cursor=cursor))

    def _save_rebuild_finished(self, elapsed: float) -> None:
        """
        Закрывает пересборку: снимает курсор и пишет метрики — как mark_full_loaded для полной
        выгрузки объекта. Время в минутах и дробное: пересборка витрины меряется десятками минут.

        last_run_at здесь НЕ трогается. Его уже двигали инкременты между блоками, и вернуть его на
        старт пересборки значило бы пересчитать всё это ещё раз без всякой пользы.

        full_rebuild_is_required тоже не трогается: флаг сняли ещё на старте, и если он снова
        поднят — значит пересборку заказали уже во время этой, и она должна пойти следующей.
        """
        with self.engine.begin() as conn:
            conn.execute(update(self.table).where(self.table.c.name == self.name)
                         .values(rebuild_cursor=None, last_error=None,
                                 last_full_rebuild_dt=func.now(),
                                 last_full_rebuild_minutes=round(elapsed / 60, 3)))

    def _save_error(self, error: BaseException) -> None:
        """
        В таблицу — тип и первая строка сообщения. Полный трейсбек остаётся в логе, он там уже
        есть: оба вызова стоят следом за logger.exception.

        Раньше сюда клали хвост трейсбека в 4000 символов, и это была утечка. Таблицу читает
        любой, у кого есть SELECT на схему, то есть и потребители витрины, — а в трейсбеке ошибки
        SQLAlchemy едут и сам запрос, и значения его параметров (`[SQL: …]`, `[parameters: …]`).
        Первая строка сообщения их не содержит: SQLAlchemy печатает и то, и другое со второй.
        """
        first_line = (str(error).strip().splitlines() or [''])[0]
        with self.engine.begin() as conn:
            conn.execute(update(self.table).where(self.table.c.name == self.name)
                         .values(last_error=f'{type(error).__name__}: {first_line}'[:500]))
