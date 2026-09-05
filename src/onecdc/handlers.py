"""
Пользовательские обработчики событий (handlers): код, который запускается, когда в целевой БД
реально изменились данные конкретных объектов 1С.

Зачем отдельный механизм. Инкрементальная витрина (или отправка изменений во внешнюю систему)
не может сама узнать, что данные приехали: она либо опрашивает БД вхолостую, либо ждёт, пока её
запустят руками. Репликатор эту информацию имеет — он и сохраняет данные, — поэтому он и
сообщает, что пора считать.

Обработчик — класс, унаследованный от Handler1C:

    # handlers/zakazy_klientov.py
    from cdc_1c import Handler1C, HandlerContext

    class ZakazyKlientov(Handler1C):
        ON = ["AccumulationRegister_ZakazyKlientov", "Catalog_Nomenklatura"]
        ON_FULL_LOAD = True     # вызывать ли на страницах полной выгрузки (по умолчанию да)
        MIN_INTERVAL = 0        # не чаще раза в N секунд (0 — без ограничения)

        def handle(self, context: HandlerContext) -> None: ...

Аннотацию `context: HandlerContext` писать стоит везде: библиотека её не требует, но с ней IDE
подсказывает поля контекста и их типы, а опечатка в имени поля видна сразу, а не в проде.

Наследование не обязательно: as_handler примет и модуль целиком, и функцию — нужны только те же
`ON` и `handle`. Базовый класс просто избавляет от копипасты (см. Handler1C).

Каждому обработчику — свой HandlerLoop, и запускается он блокирующим run_forever, как и
репликатор:

    from handlers import ZakazyKlientov, OtpravkaVOchered

    handler_zakazy  = HandlerLoop(engine=engine, schema='cdc_1c', handler=ZakazyKlientov())
    handler_ochered = HandlerLoop(engine=engine, schema='cdc_1c', handler=OtpravkaVOchered())

    with ThreadPoolExecutor(max_workers=2) as pool:
        pool.submit(handler_zakazy.run_forever)
        pool.submit(handler_ochered.run_forever)

Репликатору эти циклы не передаются: он о них не знает, а сообщает через таблицу handlers_1c (см.
HandlerSignals). Поэтому обработчики можно запускать где угодно — хоть в отдельном контейнере.

Цикл на обработчика, а не один на всех, — чтобы тяжёлая витрина не задерживала остальные. Порядок
между ними при этом не определён: на «витрина поверх витрины считается после базовой» полагаться
нельзя. Общий код обработчиков выносится в соседний модуль обычным `from handlers._common import
...`, а не особым соглашением.

В `ON` перечисляются имена ТАБЛИЦ, как они выглядят в целевой БД (транслит), а не имена объектов
1С: обработчик пишет SQL по таблицам, имя 1С он в глаза не видит. Имя 1С в `ON` не совпадёт ни с
чем — as_handler ловит это на старте и подсказывает нужное написание.

И это не только «на что реагировать», но и «что я читаю»: по тому же списку считается верхняя
граница окна (см. ниже), поэтому перечислять надо ВСЕ таблицы, из которых обработчик выбирает
данные, а не только те, ради которых он написан.

Данные обработчику не передаются — он делает свой SELECT. Ему передаётся окно времени
(`context.last_run_at`, `context.boundary`), по которому он выбирает изменившееся:

    WHERE merged_on > :last_run_at

`last_run_at` — отметка предыдущего УСПЕШНОГО запуска, и это ВСЕГДА datetime, а не None:
обработчик, который ещё ни разу не отрабатывал, получает начало времён (EPOCH). Поэтому WHERE у
него один и тот же во всех случаях — без ветки «а если первый раз». Собрать витрину заново можно,
заказав пересборку:

    UPDATE <схема>.handlers_1c SET full_rebuild_is_required = true WHERE name = '<имя обработчика>';

Тогда окно откроется с начала времён (`context.last_run_at` = EPOCH), а в `context.full_rebuild`
придёт True. После успеха требование снимается и в `last_full_rebuild_dt` записывается время.

Пересборку большой витрины стоит нарезать на блоки — объявив генератор `rebuild` (см. Handler1C):
между блоками тот же поток применяет накопившиеся изменения, и витрина не стоит холодной все те
десятки минут, что идёт пересборка. Одним потоком, а не двумя: блок и инкремент тогда никогда не
выполняются одновременно, и затирать друг друга им нечем.

Обе границы — по часам БД (тем же now(), которым dbmerge штампует merged_on), чтобы не ловить
расхождение часов между хостами.

Про верхнюю границу. Брать её как «сейчас» нельзя: merged_on пишется внутри merge-транзакции, а
коммитится минутами позже (толстая страница полной выгрузки), поэтому строка может иметь merged_on
заведомо в прошлом и при этом быть не видна SELECT-у обработчика. Отметка бы её перешагнула, и
строка потерялась бы навсегда. Поэтому граница — минимум из «сейчас» и стартов ещё не завершённых
merge по таблицам из `ON` (см. WriteTracker): всё, что обработчику не видно, записано незавершённым
merge, а его merged_on не может быть меньше старта этого merge — значит оно гарантированно окажется
правее границы и попадёт в следующее окно.

Доставка получается at-least-once: если процесс упадёт после работы обработчика, но до записи
last_run_at, окно повторится. Обработчик обязан быть идемпотентным (для витрины это выполняется
само, для отправки во внешнюю систему — забота потребителя).
"""

import itertools
import threading
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from types import ModuleType
from typing import Callable, Iterable, Iterator

from sqlalchemy import (ARRAY, Boolean, case, Column, ColumnElement, DateTime, Engine, Float,
                        func, insert, inspect, MetaData, or_, select, String, Table, text, update)

from cdc_1c.common_functions import DB_NOW_WITHOUT_TIMEZONE
from cdc_1c.db_logs import _check_create_schema
from cdc_1c.logging_config import LOAD_MODE_HANDLER, get_logger, load_mode
from cdc_1c.name_mapper import NameMapper1C
from cdc_1c.stop_signal import StopSignal, install_signal_handlers
# Реестр незавершённых merge переехал в свой модуль (им пользуется и репликатор). Имена
# реэкспортируются: HandlerLoop прижимает к нему границу окна, а внешний код и тесты импортируют
# WriteTracker отсюда с первых версий.
from cdc_1c.write_tracker import (MERGE_ABANDONED_TTL, MERGE_HEARTBEAT_PERIOD, MERGE_HEARTBEAT_TTL, WRITES_TABLE,
                                  WriteTracker, _writes_table)

logger = get_logger(__name__)

HANDLERS_TABLE = "handlers_1c"

# Метка сигнала об изменении подписанной таблицы (см. _handlers_table). Вынесена в
# константу: на неё смотрят и перенос со старого булева флага, и сам цикл.
UPDATE_REQUESTED_FIELD = "update_requested_at"

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

# Как часто репликатор перечитывает подписки обработчиков (update_on) из handlers_1c. Меняются они
# только при перезапуске обработчика, а читать их на каждый сохранённый объект — лишние запросы:
# при полной выгрузке это запрос на страницу.
SUBSCRIPTIONS_TTL = 30.0


# Начало времён: то, что приезжает в context.last_run_at, когда отметки прошлого прогона ещё нет
# (первый запуск или запрошена пересборка). Заведомо раньше любого merged_on и при этом валидная
# дата для всех поддерживаемых СУБД. В колонке handlers_1c.last_run_at на её месте NULL — там он
# значит «ни разу не отрабатывал», и по нему цикл выбирает пересборку; в контекст этот NULL не
# доходит, чтобы обработчику не приходилось разбирать None у себя в WHERE.
EPOCH = datetime(1900, 1, 1)




@dataclass(frozen=True)
class HandlerContext:
    """
    Что обработчик получает на вход. Данных здесь нет намеренно: обработчик делает свой SELECT —
    так он не зависит от того, в каком виде их прочитал репликатор, и переживает перезапуск процесса
    (всё его состояние — в БД).
    """

    engine: Engine
    schema: str | None
    # Схема промежуточных таблиц dbmerge, если обработчик пишет витрину через него: передайте её
    # в dbmerge(..., temp_schema=context.temp_schema). None — та же схема, что у данных.
    temp_schema: str | None
    # Границы окна по часам БД: (last_run_at, boundary]. Обе — всегда datetime: первый прогон и
    # пересборка получают в last_run_at начало времён (EPOCH), а не None, поэтому обработчик пишет
    # `merged_on > context.last_run_at` одинаково во всех прогонах. Что это сборка с нуля, видно из
    # соседнего full_rebuild — гадать по None не нужно.
    last_run_at: datetime
    boundary: datetime
    # Витрину просят собрать заново: обработчик ещё ни разу не отрабатывал либо кто-то заказал
    # пересборку (handlers_1c.full_rebuild_is_required). Окно при этом открыто с начала времён —
    # обработчику, который считает только по нему, ничего специально делать не надо. Флаг нужен
    # тем, у кого пересборка идёт иначе: например, не одним merge, а по частям.
    full_rebuild: bool
    # Метка последнего блока, который пересборка успела завершить до перезапуска процесса (см.
    # Handler1C.rebuild). None — начинаем с начала. Смысл метки известен только обработчику: он
    # сам нарезал блоки, он же и решает, какие из них пропустить.
    rebuild_from: str | None
    # Что привело к вызову. Только для логов: выбирать данные обязательно по окну.
    #
    # Точности тут немного, и это осознанно. Репликатор сообщает об изменении меткой времени в
    # handlers_1c, а метка не несёт ни имени изменившейся таблицы, ни источника — иначе пришлось бы
    # городить в таблице очередь событий вместо одной колонки. Поэтому objects — это весь ON
    # обработчика,
    # а sources — db_signal (изменение), startup (первый проход процесса) или full_rebuild (заказ
    # пересборки). Отличить бэкфилл полной выгрузки от живого изменения по sources нельзя; если
    # обработчику не нужен бэкфилл, он выключается целиком через ON_FULL_LOAD.
    objects: frozenset[str]
    sources: frozenset[str]
    logger: object


class Handler1C:
    """
    Базовый класс обработчика. Наследоваться не обязательно — HandlerLoop достаточно `name`, `ON` и
    `handle` (годится и модуль, и функция), — но здесь лежит то, что иначе копируется из
    обработчика в обработчик.

        class ZakazyKlientov(Handler1C):
            # имена ТАБЛИЦ в целевой БД (транслит), а не имена объектов 1С
            ON = ["AccumulationRegister_ZakazyKlientov", "Catalog_Nomenklatura"]

            def setup(self, context: HandlerContext) -> None:
                self.execute(context, DDL)

            def handle(self, context: HandlerContext) -> None:
                ...

    В HandlerLoop передаётся ЭКЗЕМПЛЯР, а не класс: так обработчик можно параметризовать
    конструктором (одна и та же логика на две схемы — два экземпляра). Но имя по умолчанию берётся от
    класса и служит ключом состояния в handlers_1c, поэтому у параметризованных экземпляров оно
    обязано различаться — иначе они молча поделили бы одну отметку last_run_at на двоих.

    Состояние экземпляра — только кэш в пределах процесса (флаг «setup сделан», скомпилированные
    выражения). Всё, что должно пережить перезапуск, живёт в handlers_1c.
    """

    # Таблицы (транслит, как в БД), на изменения которых реагирует обработчик. Это же список
    # того, что он ЧИТАЕТ: по нему считается верхняя граница окна, поэтому перечислять надо все
    # читаемые таблицы, а не только те, ради которых обработчик написан.
    ON: list[str] = []
    # Вызывать ли на страницах полной выгрузки. False — для тех, кому бэкфилл не нужен (рассылка,
    # отправка в очередь): иначе первоначальная выгрузка выстрелит по каждой странице.
    ON_FULL_LOAD: bool = True
    # Не чаще раза в N секунд. Ограничитель для тяжёлых витрин: во время полной выгрузки сигналы
    # идут постранично, и без паузы инкремент конкурировал бы с самой выгрузкой за БД.
    MIN_INTERVAL: float = 0
    # Имя (ключ в handlers_1c). None — имя класса. Переименование заводит новую строку состояния,
    # то есть первый прогон после переименования пойдёт с начала времён.
    NAME: str | None = None

    def __init__(self, name: str | None = None):
        """Имя можно задать экземпляру — это то, что делает параметризацию рабочей: два экземпляра
        одного класса обязаны иметь разные имена, иначе поделят одну отметку last_run_at.
        Свой __init__ в наследнике должен вызывать super().__init__(name)."""
        if name:
            self.NAME = name

    @property
    def name(self) -> str:
        return self.NAME or type(self).__name__

    def setup(self, context: HandlerContext) -> None:
        """
        Разовая подготовка: DDL вьюшек и целевых таблиц, создание индексов. Вызывается один раз за
        процесс, перед первым handle. Отдельно от handle именно поэтому: полная выгрузка сигналит
        постранично, и CREATE OR REPLACE VIEW на каждый вызов брал бы блокировки на пустом месте.
        """

    def handle(self, context: HandlerContext) -> None:
        """Полезная работа за окно (context.last_run_at, context.boundary]."""
        raise NotImplementedError(f"{self.name}.handle(context) is not implemented")

    def rebuild(self, context: HandlerContext) -> Iterator[str]:
        """
        Полная пересборка витрины ПО БЛОКАМ. Генератор: `yield <метка>` означает «блок закончен и
        закоммичен». В этих точках цикл вклинивается и применяет накопившиеся изменения, поэтому
        витрина не стоит холодной все те десятки минут, что идёт пересборка.

            def rebuild(self, context: HandlerContext) -> Iterator[str]:
                for year in range(2020, 2027):
                    if context.rebuild_from and str(year) <= context.rebuild_from:
                        continue                      # этот блок уже сделан до перезапуска
                    self.merge_year(context, year)
                    yield str(year)

        Чем нарезать — решает обработчик: период, диапазон ключей, склад, организация. Библиотека
        в блоки не заглядывает, метка ей нужна только чтобы записать её в rebuild_cursor и вернуть
        в context.rebuild_from после перезапуска.

        Почему это безопасно. Блок и инкремент идут в ОДНОМ потоке и потому никогда не выполняются
        одновременно — затирать друг друга им нечем. Ключевое условие: блок должен читать данные
        АКТУАЛЬНЫЕ на момент своего выполнения, а не снимок на старте пересборки. Обработчики так и
        написаны (верхней границы в WHERE у них нет), поэтому специально делать ничего не нужно —
        но и «прочитаю всё один раз в начало, разложу по блокам потом» делать нельзя: такой снимок
        затрёт изменения, применённые между блоками.

        По умолчанию пересборка не делится: один блок, и это в точности прежнее поведение — весь
        handle() с окном от начала времён.
        """
        self.handle(context)
        yield 'all'

    # --- то, что иначе копируется из обработчика в обработчик ----------------------------------

    def changed_since(self, context: HandlerContext, *columns: ColumnElement) -> ColumnElement:
        """
        Условие «хоть одна из отметок свежее прошлого прогона»: `column > context.last_run_at`
        через OR.

        Колонок несколько, потому что объекты 1С приезжают в обмене независимо: строка витрины
        собирается JOIN-ом нескольких источников, и свежим может стать любой из них.

        ВАЖНО, если колонки приходят из РАЗНЫХ таблиц соединения: на объёме такой OR перестаёт
        пользоваться индексами — ни одну таблицу нельзя отфильтровать до джойна, и условие
        вырождается в Join Filter поверх полного соединения. Тогда вместо одного WHERE берут
        UNION ALL из той же вьюшки, по ветке на отметку: в каждой ветке остаётся предикат по одной
        таблице, и он опускается в её скан (см. config/handlers/ — там это разобрано с
        замерами). Этот помощник хорош, когда отметки лежат на одной таблице: BitmapOr из индексных
        сканов Postgres в таком случае строит сам.

        Верхней границы здесь намеренно нет, хотя окно ею закрывается. От потери строк защищает не
        этот WHERE, а само значение context.boundary: оно прижато к старту незавершённого merge, чьи
        строки обработчику ещё не видны. Строку правее границы, которая уже видна, обработчик
        посчитает раньше срока — и посчитает её же ещё раз в следующем окне, потому что last_run_at
        станет boundary. Это лишняя работа, а не пропуск, и взамен витрина получается свежее.

        Обработчику, которому повтор дорог (отправка во внешнюю систему, где каждое сообщение
        стоит денег), верхнюю границу стоит добавить самому: `column <= context.boundary`.
        """
        return or_(*[column > context.last_run_at for column in columns])

    def execute(self, context: HandlerContext, sql: str, **params) -> None:
        """Выполняет SQL, подставляя в `{schema}` целевую схему (уже в кавычках)."""
        with context.engine.begin() as conn:
            conn.execute(text(sql.format(schema=self.schema_prefix(context))), params)

    def query(self, context: HandlerContext, sql: str, **params) -> list:
        """
        Строки SQL-запроса; `{schema}` подставляется так же, как в execute(). Пара к нему: тот
        выполняет и ничего не возвращает, этот читает.

        Результат материализуется списком, а не курсором: вызывать такое обычно надо ДО долгой работы
        (например, за списком блоков пересборки), и держать соединение открытым всё это время
        незачем — тем более что работа возьмёт из пула своё.
        """
        with context.engine.connect() as conn:
            return conn.execute(text(sql.format(schema=self.schema_prefix(context))), params).all()

    @staticmethod
    def schema_prefix(context: HandlerContext) -> str:
        """Имя схемы для подстановки в текстовый SQL; без схемы — public (схема по умолчанию)."""
        return f'"{context.schema}"' if context.schema else 'public'


@dataclass(frozen=True)
class Handler:
    """
    Нормализованный обработчик: то, с чем работает HandlerLoop, независимо от того, чем он был объявлен
    (экземпляр Handler1C, модуль или функция). Собирается функцией as_handler.
    """

    name: str
    on: frozenset[str]
    on_full_load: bool
    min_interval: float
    handle: Callable[[HandlerContext], None]
    setup: Callable[[HandlerContext], None] | None = None
    # Пересборка по блокам (см. Handler1C.rebuild). Не объявлена — подставляется обёртка вокруг
    # handle: один блок, прежнее поведение.
    rebuild: Callable[[HandlerContext], object] | None = None


def as_handler(obj) -> Handler:
    """
    Приводит объявленный пользователем обработчик к Handler.

    Принимается всё, у чего есть `handle` и непустой `ON`: экземпляр Handler1C, модуль
    (`from handlers import zakazy` → модуль целиком) или функция с теми же атрибутами. Нужны
    только имя, набор объектов и вызываемое — обязательного базового класса нет.
    """
    if isinstance(obj, Handler):
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

    on = frozenset(getattr(obj, 'ON', ()) or ())
    if not on:
        raise AttributeError(f"Handler {_handler_name(obj)} has empty ON: it would never be called")
    # Подписка идёт по именам ТАБЛИЦ (транслит), а не по именам объектов 1С. Имя 1С в ON выглядит
    # правдоподобно, но не совпадёт ни с чем и обработчик просто никогда не вызовут — молча.
    # Ловим по не-ASCII символам и сразу подсказываем, как это имя выглядит в БД.
    for name in sorted(on):
        if not name.isascii():
            raise ValueError(
                f"Handler {_handler_name(obj)}: ON must list TABLE names as they appear in the "
                f"database, not 1C object names. Replace {name!r} with "
                f"{NameMapper1C().map_object_name(name)!r}")

    setup = getattr(obj, 'setup', None)
    rebuild = getattr(obj, 'rebuild', None)
    if not callable(rebuild):
        # Пересборка не разбита на блоки — один блок, весь handle() целиком.
        def rebuild(context, _handle=handle):
            _handle(context)
            yield 'all'
    return Handler(name=_handler_name(obj), on=on,
                   on_full_load=bool(getattr(obj, 'ON_FULL_LOAD', True)),
                   min_interval=float(getattr(obj, 'MIN_INTERVAL', 0)),
                   handle=handle, setup=setup if callable(setup) else None,
                   rebuild=rebuild)


def _handler_name(obj) -> str:
    """Имя обработчика: NAME → .name (Handler1C) → имя модуля/функции без пути пакета."""
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
        Column("last_run_at", DateTime, nullable=True),
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
        # Метка, а не булев флаг, — из-за сигнала, пришедшего ПОСЕРЕДИНЕ прогона: поднять true над
        # true нельзя, следа не остаётся, и прогон снимал флаг вместе с непрочитанным сигналом.
        # Метка же сдвигается вправо от границы окна, и снять её прогон уже не вправе (см.
        # _save_success): обработчик будет вызван ещё раз.
        Column(UPDATE_REQUESTED_FIELD, DateTime, nullable=True),
        # Заказ полной пересборки витрины — руками или автоматически (см. request_full_rebuild) —
        # и метрики последней пересборки. Названы по образцу metadata_objects_1c, где так же
        # устроена полная выгрузка объекта.
        Column("full_rebuild_is_required", Boolean, nullable=False, server_default=text('false')),
        Column("last_full_rebuild_dt", DateTime, nullable=True),
        Column("last_full_rebuild_minutes", Float, nullable=True),
        # Метка последнего ЗАВЕРШЁННОГО блока идущей пересборки (см. Handler1C.rebuild). Нужна,
        # чтобы пересборка на десятки минут переживала перезапуск процесса: генератор блоков живёт
        # в памяти и рестарта не переживает, а метка лежит в БД и возвращается обработчику в
        # context.rebuild_from. NULL — пересборка не идёт либо не сделала ни одного блока.
        Column("rebuild_cursor", String(255), nullable=True),
        schema=schema_name,
    )


def _add_missing_columns(engine: Engine, table: Table) -> None:
    """
    Дописывает в существующую таблицу колонки, которых в ней ещё нет: create(checkfirst=True)
    заводит таблицу целиком, но давно созданную не трогает, и после обновления библиотеки строки
    состояния остались бы без новых колонок.
    """
    existing = {column['name'] for column in inspect(engine).get_columns(
        table.name, schema=table.schema)}
    missing = [column for column in table.columns if column.name not in existing]
    if not missing:
        return
    compiler = engine.dialect.ddl_compiler(engine.dialect, None)
    with engine.begin() as conn:
        for column in missing:
            conn.execute(text(f'ALTER TABLE {compiler.preparer.format_table(table)} '
                              f'ADD COLUMN {compiler.get_column_specification(column)}'))
    logger.info("Added columns to %s: %s", table.name, ', '.join(c.name for c in missing))
    _carry_over_update_flag(engine, table, existing, {c.name for c in missing})


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
                              .values(update_requested_at=DB_NOW_WITHOUT_TIMEZONE))
    if result.rowcount:
        logger.info("Carried over %s pending signal(s) from %s to %s",
                    result.rowcount, legacy, UPDATE_REQUESTED_FIELD)


class HandlerSignals:
    """
    Сторона РЕПЛИКАТОРА: как он вызывает обработчиков, не зная о них ничего.

    Обработчик при старте объявляет себя в handlers_1c и перечисляет в update_on таблицы, на
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
        # Таблицу заводит и репликатор тоже: обработчиков в этом процессе может не быть вовсе, а
        # поднимать флаг всё равно надо — иначе первый же сигнал упал бы на отсутствующей таблице.
        self.table.create(engine, checkfirst=True)
        _add_missing_columns(engine, self.table)
        self._lock = threading.Lock()
        self._subscriptions: dict[str, list[tuple[str, bool]]] = {}
        self._read_at = 0.0

    def subscribers(self, table_name: str, source: str = SOURCE_CHANGES) -> list[str]:
        """
        Имена обработчиков, которых это изменение касается: подписаны на таблицу (update_on) и
        согласны на такой источник (on_full_load).

        Источник учитывается здесь, а не у обработчика: тот видит только метку update_requested_at и
        по нему не может отличить бэкфилл от живого изменения. Поэтому обработчик объявляет своё
        on_full_load в таблице, а решение принимает тот, кто флаг поднимает.
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
        """update_on всех включённых обработчиков, развёрнутый в отображение
        таблица → [(имя, вызывать ли на полной выгрузке)]."""
        t = self.table
        subscriptions: dict[str, list[tuple[str, bool]]] = {}
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(t.c.name, t.c.update_on, t.c.on_full_load).where(t.c.enabled)).all()
        for name, update_on, on_full_load in rows:
            for table in update_on or ():
                subscriptions.setdefault(table, []).append((name, bool(on_full_load)))
        return subscriptions

    def signal(self, table_name: str, source: str) -> None:
        """
        Ставит подписчикам таблицы метку сигнала. Подписчиков нет — ничего не делаем.

        Метка ВСЕГДА перезаписывается текущим временем БД, а не ставится «если пусто»: обработчик
        снимает её, только если она не правее границы его окна, а значит хранить надо последний
        сигнал. С самым ранним сигнал, пришедший в середине прогона, не сдвинул бы значение — и
        был бы снят вместе с обработанными.
        """
        names = self.subscribers(table_name, source)
        if not names:
            return
        with self.engine.begin() as conn:
            conn.execute(update(self.table)
                         .where(self.table.c.name.in_(names))
                         .values(update_requested_at=DB_NOW_WITHOUT_TIMEZONE))
        logger.info("Changed %s (%s) → update requested for %s",
                    table_name, source, ', '.join(sorted(names)))

    def request_full_rebuild(self, table_name: str, reason: str,
                             source: str = SOURCE_CHANGES) -> None:
        """
        Просит подписчиков таблицы собрать витрину заново: ставит full_rebuild_is_required.

        Единственный автоматический повод — новая колонка в таблице объекта (added_fields из
        dbmerge). Инкремент её появления не видит в принципе: окно строится по merged_on, а
        merged_on двигается только у строк, у которых изменились ЗНАЧЕНИЯ; добавление колонки не
        меняет ни одного значения, и все уже лежащие строки остаются левее окна навсегда.

        Источник учитывается так же, как в signal(), и по той же причине. Колонка появляется и на
        странице ПОЛНОЙ ВЫГРУЗКИ — dbmerge заводит их по фактическим данным страницы, а поля со
        значением null 1С не присылает вовсе, поэтому реквизит, пустой на первой странице и
        заполненный на второй, добавляет колонку прямо посреди прогона. Вызывать на этом того, кто от
        бэкфилла отписался (ON_FULL_LOAD=False), нельзя: для отправщика во внешнюю систему
        пересборка — это повторная отправка всего с начала времён, ровно то, чего он и избегал.
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
    Цикл ОДНОГО обработчика: его состояние, его очередь и его прогоны.

    Один обработчик на цикл, а не список, — чтобы тяжёлая витрина не задерживала остальные:
    циклы независимы и крутятся каждый в своём потоке. Плата за это — порядок вызова между обработчиками не определён, и
    два обработчика, пишущие в одну целевую таблицу, теперь могут делать это одновременно. Если
    витрина строится поверх другой витрины, полагаться на «сначала базовая» нельзя.

    Сигнал — метка update_requested_at в handlers_1c, а не очередь событий. Пока обработчик
    считает, прилетевшие страницы поднимают тот же флаг ещё раз, а не выстраиваются в очередь из
    тысячи вызовов. Поэтому репликатор может сигналить на каждую страницу полной выгрузки: витрина
    начинает наполняться после первой же и догоняет выгрузку с задержкой в один свой прогон.

    Цикл отдельный (а не вызов прямо в цикле репликации) — чтобы обработчик не тормозил приём
    изменений: пакет не подтверждается в 1С до успешного save, копить отставание незачем.
    Пользовательский код при этом никогда не исполняется в потоках репликатора — те только
    поднимают флаг.

    Планов обмена может быть сколько угодно: все репликаторы поднимают тот же флаг и публикуют свои
    незавершённые merge в общую writes_in_process_1c, поэтому обработчику безразлично, кто его кормит.
    """

    def __init__(self, engine: Engine, schema: str | None, handler,
                 temp_schema: str | None = None,
                 write_tracker: "WriteTracker | None" = None):
        # Перехват SIGTERM/SIGINT — по той же причине, что у Replicator1C: run_forever уходит в
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
        # Схема промежуточных таблиц dbmerge — обработчику она нужна затем же, зачем репликатору
        # (см. Replicator1C): держать их в стороне от таблиц с данными. Сам цикл её не использует,
        # он лишь передаёт её обработчику в контексте.
        self.temp_schema = temp_schema
        # Реестр незавершённых merge. По умолчанию — общий, из таблицы writes_in_process_1c
        # (см. свойство writes); подменяется только в тестах.
        self._writes = write_tracker

        self.table = _handlers_table(MetaData(), self.schema_name)
        self.table.create(engine, checkfirst=True)
        _add_missing_columns(engine, self.table)
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
        # Действующий StopSignal текущего run_forever — через него цикл останавливают снаружи.
        self._stop_signal: "StopSignal | None" = None

    def __repr__(self) -> str:
        return f'<HandlerLoop {self.name}>'

    @property
    def writes(self):
        """
        Реестр незавершённых merge, по которому считается верхняя граница окна.

        По умолчанию — общий, из таблицы writes_in_process_1c: обработчик обязан видеть merge ЛЮБОГО
        репликатора, в том числе работающего в другом процессе или контейнере. Его незакоммиченные
        строки имеют merged_on в прошлом, и граница, взятая как «сейчас», их бы перешагнула.
        Подменяется только в тестах.
        """
        if self._writes is None:
            self._writes = WriteTracker(self.engine, self.schema, f'handler:{self.name}')
        return self._writes

    def _register_handler(self) -> None:
        """
        Объявляет себя в handlers_1c: заводит строку состояния и — главное — записывает в update_on
        список таблиц, на которые подписан.

        update_on пишется на КАЖДОМ старте, а не только при первой регистрации: подписка живёт в
        коде обработчика (Handler1C.ON), и после её правки таблица обязана догнать код. Читает
        update_on репликатор — так ему не нужны ни объекты обработчиков, ни их импорт, и обработчик
        может работать в другом процессе или контейнере.

        Новый обработчик появляется с last_run_at=NULL, т.е. первым прогоном обработает всё
        с начала времён.
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
        Блокирующий цикл обработчика — та же форма, что у Replicator1C.run_forever: где ему
        крутиться, решает точка входа, а не библиотека.

        Опрос, а не ожидание события: сигнал приходит флагом в handlers_1c, поднять его может любой
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
            while not stop.requested:
                try:
                    self.run_if_pending()
                except Exception:
                    logger.exception("Handler %s dispatch failed", self.name)
                stop.wait(poll_interval)
            logger.info("Handler %s stopped", self.name)

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
        # Грязные отметки забираем ПЕРЕД расчётом границы, а не после: сигнал приходит уже после
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
        # изменения, а в handlers_1c не появится last_error, и со стороны БД он будет выглядеть
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
        except Exception:
            logger.exception("Handler %s failed, retry in %ss", self.name, RETRY_DELAY)
            self._save_error()
            # Возвращаем отметки: окно не сдвинулось (last_run_at не записан), но без грязного
            # флага повтор случился бы только при следующем изменении — а его может и не быть.
            self._mark_dirty(objects, sources)
            retry_delay = RETRY_DELAY
        else:
            # last_run_at = граница, ВЗЯТАЯ ДО вызова: всё, что смёржилось за время работы
            # обработчика, окажется правее неё и попадёт в следующее окно, а не потеряется.
            elapsed = time.monotonic() - started
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
            engine=self.engine, schema=self.schema, temp_schema=self.temp_schema,
            last_run_at=window_start, boundary=boundary,
            objects=frozenset(objects), sources=frozenset(sources), full_rebuild=full_rebuild,
            rebuild_from=rebuild_from or None,
            logger=get_logger(f'cdc_1c.handler.{self.name}'))

    def _prepare(self, context: HandlerContext) -> None:
        """Разовая подготовка (DDL вьюшек и целевых таблиц) — до первого handle и только один раз
        за процесс. Отдельно от handle потому, что полная выгрузка сигналит постранично, и
        CREATE OR REPLACE VIEW на каждый вызов брал бы блокировки на пустом месте."""
        if self.handler.setup is not None and not self._prepared:
            self.handler.setup(context)
            self._prepared = True

    def _stop_requested(self) -> bool:
        return self._stop_signal is not None and self._stop_signal.requested

    # --- пересборка по блокам -----------------------------------------------------------------

    def _run_rebuild(self, last_run_at: datetime | None, cursor: str | None) -> None:
        """
        Полная пересборка витрины блоками, которые нарезал сам обработчик (Handler1C.rebuild).
        Между блоками применяются накопившиеся изменения — витрина не стоит холодной все те
        десятки минут, что идёт пересборка.

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
                # Обе записи — ОДНОЙ транзакцией. По отдельности между ними помещается падение
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
        except Exception:
            logger.exception("Handler %s rebuild failed, retry in %ss", self.name, RETRY_DELAY)
            self._save_error()
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

        Пересборкой этот метод не занимается вовсе: её ведёт _run_rebuild — флаг заказа снимает
        _save_rebuild_started, метрики пишет _save_rebuild_finished. Сюда прогон приходит только
        инкрементом, в том числе тем, что идёт МЕЖДУ блоками пересборки. Отсюда и datetime без
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
            # прогон не читал. Сюда попадают и сигналы, пришедшие ПОСЕРЕДИНЕ прогона, и сигналы
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
        выгрузки объекта. Время в МИНУТАХ и дробное: пересборка витрины меряется десятками минут.

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

    def _save_error(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(update(self.table).where(self.table.c.name == self.name)
                         .values(last_error=traceback.format_exc()[-4000:]))
