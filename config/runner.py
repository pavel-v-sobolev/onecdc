"""
Шаблон запуска репликатора, обработчиков витрин и полных выгрузок по расписанию.

Запуск: python runner.py. Каталог handlers/ должен находиться рядом: Python добавляет
каталог скрипта в sys.path. В Docker эти файлы монтируются в /config.

После настройки окружения шаблон запускает репликацию изменений. Чтобы подключить
примеры обработчиков и расписаний, раскомментируйте импорты и элементы HANDLERS и CRON_JOBS.
Число рабочих потоков и предел основного пула соединений рассчитываются по этим спискам.
Контракт обработчиков описан в README_API.md, внутренняя механика — в DESIGN.md.

Параметры Replicator берутся из окружения и передаются явно. При необходимости значение
можно задать прямо в конструкторе, например odata_auth=None для публикации без авторизации.

Каждый HandlerLoop работает в своём потоке. Порядок обработчиков не определён: если одна
витрина зависит от другой, приложение должно само согласовать их обновление. Репликатор
передаёт сигналы через таблицу onecdc_handlers, поэтому обработчики можно запускать
и в отдельных процессах.

Для второго плана обмена создайте ещё один Replicator и добавьте его run_forever в RUNNABLES.
Для запуска только полных выгрузок оставьте Replicator исполнителем FullLoadCron,
не добавляя цикл чтения изменений в RUNNABLES. Все циклы обрабатывают SIGTERM.

ONECDC_MODE этот файл НЕ читает: состав и режим работы задаются кодом runner.py.
Переменная действует на стандартную команду onecdc, которую образ запускает без runner.py.
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial
# Нужен расписаниям (FullLoadCron ниже), когда грузится не весь объект, а свежий хвост.
# from datetime import timedelta

from sqlalchemy import create_engine

from onecdc import FullLoadCron, HandlerLoop, Replicator

# from handlers import ZakazyKlientov, ZakazyKlientovGrouped

# --- Что считаем и что выгружаем по расписанию ---------------------------------------------------

# Обработчики витрин: пример, замените на свои. Считают витрины из уже загруженных таблиц, каждый
# своим циклом. Классы — в handlers/ рядом.
HANDLERS = [
    # ZakazyKlientov(),
    # ZakazyKlientovGrouped(),
]

# Полные выгрузки по расписанию: пример, замените на свои.
#
# Полная выгрузка повторно читает данные из 1С и возвращает число изменённых строк.
# Если источник не менялся, CDC уже сохранил все записи и удалений не было, ожидается ноль.
# Ненулевой результат может означать отставание CDC или физическое удаление; сам по себе он
# не доказывает потерю изменений. Выгрузка за последние дни помогает регулярно сверять данные.
#
# Имена — те, что видны в БД (латиница), как и у обработчиков: настраивая выгрузку, смотрят в базу
# и в реестр onecdc_metadata_objects (колонки object_full_name_en / fields_en). Оригинальные имена
# 1С ("Document_ЗаказКлиента") тоже принимаются. Расписание — обычная crontab-строка, время
# локальное: в контейнере задавайте TZ.
#
# Граница timedelta считается в момент срабатывания, поэтому окно едет вместе с процессом. Фильтр
# по периоду заодно делает выгрузку устойчивее: документы листаются через $skip, и выборка за пару
# прошедших суток, в отличие от всего объекта, почти не меняется под ногами.
CRON_JOBS = [
    # Каждую ночь в 03:00 — хвост за трое суток по документу, где важна свежесть.
    # dict(table_name="Document_ZakazKlienta", cron="0 3 * * *",
    #      date_field="Date", date_from=timedelta(days=3)),
    #
    # По воскресеньям в 02:00 — справочник целиком: он маленький, границы не нужны.
    # dict(table_name="Catalog_Nomenklatura", cron="0 2 * * 0"),
]

# --- Параметры -----------------------------------------------------------------------------------

# Основные переменные совпадают с настройками команды onecdc; различия — в README_ENV.md.
FULL_LOAD_WORKERS = int(os.environ.get("ONECDC_FULL_LOAD_WORKERS", "2"))
POLL_INTERVAL = float(os.environ.get("ONECDC_POLL_INTERVAL", "60"))
LOG_LEVEL = os.environ.get("ONECDC_LOG_LEVEL", "INFO").strip().upper() or "INFO"
# Сколько суток держать журнал загрузок (строка на объект на пакет — это миллионы строк в год).
# 0 — хранить всё, уборка тогда ваша.
LOG_RETENTION_DAYS = int(os.environ.get("ONECDC_LOG_RETENTION_DAYS", "30"))
# Объект, впервые пришедший в пакете изменений, репликатор сам ставит на полную выгрузку — иначе в
# БД попадёт только то, что менялось после подключения. Выключают, если первую загрузку вы
# инициируете сами: регистрацией всех изменений на стороне 1С (обработка onecdc.epf) или
# расписанием FullLoadCron. Помеченный объект фоновые потоки выгрузят в любом случае.
AUTOMATIC_FULL_LOAD = os.environ.get("ONECDC_AUTOMATIC_FULL_LOAD", "true").strip().lower() \
    not in ("0", "false", "no", "off")

# Начальный запас для цикла изменений и его служебных запросов.
REPLICATOR_CONNECTIONS = 3

# Верхний предел основного пула: три соединения плюс по одному на каждый параллельный
# поток полной выгрузки, обработчик и расписание. Это ориентир; пользовательский обработчик
# может потребовать несколько соединений. Подробнее — README_DB.md.
#
# pool_size задаёт число сохраняемых соединений, max_overflow — дополнительный предел
# при нагрузке. Пул общий: конкретные соединения не закреплены за ролью потока.
# pool_pre_ping проверяет соединение перед выдачей. Отметки живости используют отдельный пул.
engine = create_engine(
    os.environ["ONECDC_DB_URL"],
    pool_size=REPLICATOR_CONNECTIONS,
    max_overflow=FULL_LOAD_WORKERS + len(HANDLERS) + len(CRON_JOBS),
    pool_pre_ping=True,
)

DB_SCHEMA = os.environ.get("ONECDC_DB_SCHEMA")
# Схема таблиц ключей полной выгрузки; по умолчанию используется схема данных.
# Эти UNLOGGED-таблицы общие для соединений пула. Временные таблицы самого dbmerge
# создаются в сессионной схеме PostgreSQL и от DB_TEMP_SCHEMA не зависят.
DB_TEMP_SCHEMA = os.environ.get("ONECDC_DB_TEMP_SCHEMA")

# Пользователь не задан — работаем без авторизации, как и образ без runner.py. Требовать здесь обе
# переменные значило бы, что одна и та же 1С без авторизации доступна из одной раскладки образа и
# недоступна из другой.
ODATA_USER = os.environ.get("ONECDC_ODATA_USER")

replicator = Replicator(
    odata_url=os.environ["ONECDC_ODATA_URL"],
    odata_auth=((ODATA_USER, os.environ.get("ONECDC_ODATA_PASSWORD", "")) if ODATA_USER else None),
    exchange_name=os.environ["ONECDC_EXCHANGE_NAME"],
    # Не знаете GUID узла — оставьте пустым: в лог выведется список узлов плана обмена.
    queue_guid=os.environ.get("ONECDC_QUEUE_GUID", ""),
    engine=engine,
    db_schema=DB_SCHEMA,
    db_temp_schema=DB_TEMP_SCHEMA,
    full_load_workers=FULL_LOAD_WORKERS,
    automatic_full_load=AUTOMATIC_FULL_LOAD,
    log_retention_days=LOG_RETENTION_DAYS,
)

# Уровень — после конструктора: он вешает обработчик на логгер onecdc (по умолчанию INFO), а тут
# уровень переопределяется на заданный.
logging.getLogger("onecdc").setLevel(LOG_LEVEL)

# --- Запуск ---------------------------------------------------------------------------------------

# По потоку на каждый run_forever. Считаем от списка, а не помним число: единственный поток,
# занятый репликатором навсегда, оставил бы задания обработчиков в очереди пула — витрины не
# обновлялись бы, и ни одной ошибки при этом не появилось бы.
RUNNABLES = [partial(replicator.run_forever, interval=POLL_INTERVAL)]
RUNNABLES += [HandlerLoop(engine=engine, schema=DB_SCHEMA, handler=handler).run_forever
              for handler in HANDLERS]
RUNNABLES += [FullLoadCron(replicator, **job).run_forever for job in CRON_JOBS]

with ThreadPoolExecutor(max_workers=len(RUNNABLES), thread_name_prefix='cdc') as pool:
    tasks = [pool.submit(run) for run in RUNNABLES]
    for task in tasks:
        task.result()
