"""
Шаблон конфига: здесь собирается всё — подключения, параметры цикла, обработчики и расписания.

Запускается как обычный скрипт: `python runner.py`. Пакет handlers/ лежит рядом — Python кладёт
каталог скрипта в sys.path, поэтому импорт его находит. В контейнере этот каталог монтируется
в /config, и файл исполняется тем же способом, без единой правки путей.

Как есть файл уже работает: поднимается репликатор, читает изменения и пишет их в целевую БД.
Всё, что дальше является ПРИМЕРОМ — обработчики витрин и полные выгрузки по расписанию, —
закомментировано: раскомментируйте нужное и замените на своё. Обработчики в handlers/ рядом —
тоже примеры; пользовательская сторона обработчиков описана в README_API.md, механика —
в DESIGN.md.

Параметры присваиваются явно, по одному, чтобы на этом экране было видно, что именно передано в
репликатор. Значения взяты из окружения (удобно для контейнера), но любое можно заменить литералом:

    odata_url="http://server/base/odata/standard.odata",
    odata_auth=("odata", "secret"),   # None, если 1С опубликована без авторизации

На каждого обработчика — свой HandlerLoop со своим циклом, поэтому тяжёлая витрина не задерживает
остальные. Порядок между ними при этом не определён — на «витрина поверх витрины считается после
базовой» полагаться нельзя.

Репликатор обработчиков не запускает и о них не знает: сигналы идут через таблицу handlers.
Здесь они просто живут в одном процессе с ним — так проще. Отсюда и другие раскладки, каждая
правкой этого файла: разнести по контейнерам — убрать репликатор, останутся одни циклы обработчиков;
несколько планов обмена — завести второй Replicator и отправить его в тот же пул; только полные
выгрузки без чтения изменений — собрать Replicator (он исполнитель full_load: метаданные,
пагинация, запись, журнал) и просто не отправлять его run_forever в пул. В самих обработчиках при
этом не меняется ничего.

Запускаются все одинаково: у репликатора, у обработчика и у расписания блокирующий run_forever,
и все они уходят в пул потоков. Останавливает всех SIGTERM.
"""

import os
from concurrent.futures import ThreadPoolExecutor
# Нужен расписаниям (FullLoadCron ниже), когда грузится не весь объект, а свежий хвост.
# from datetime import timedelta

from sqlalchemy import create_engine

from onecdc import FullLoadCron, HandlerLoop, Replicator

# from handlers import ZakazyKlientov, ZakazyKlientovGrouped

FULL_LOAD_WORKERS = 2
POLL_INTERVAL = 60
# Объект, впервые пришедший в пакете изменений, репликатор сам ставит на полную выгрузку — иначе в
# БД попадёт только то, что менялось после подключения. Выключают, если первую загрузку вы
# инициируете сами: регистрацией всех изменений на стороне 1С (обработка onecdc.epf) или
# расписанием FullLoadCron. Помеченный объект фоновые воркеры выгрузят в любом случае.
AUTOMATIC_FULL_LOAD = True

# Пул соединений: цикл изменений, страницы полной выгрузки, ДВА потока отметки живости (реестр
# незавершённых merge и захват полной выгрузки) — и СВЕРХ ТОГО по соединению на каждый обработчик
# и каждое расписание, каждое из которых работает своим потоком. Сейчас их нет, отсюда
# FULL_LOAD_WORKERS + 3; раскомментируете два обработчика и два расписания ниже — станет
# FULL_LOAD_WORKERS + 7 (подробнее в README_DB.md, «Сколько нужно соединений к БД»).
engine = create_engine(os.environ["ONECDC_DB_URL"], pool_size=FULL_LOAD_WORKERS + 3)

DB_SCHEMA = os.environ.get("ONECDC_DB_SCHEMA")
# Схема промежуточных таблиц dbmerge — своя, отдельно от данных. Не обязательна (не задана — та же,
# что у данных), но так таблицы с данными и рабочие таблицы merge не перемешиваются: в этой схеме по
# определению нет ничего ценного, поэтому таблицу, оставшуюся после падения процесса, там видно и не
# жалко удалить. Схему создаёт сам dbmerge.
DB_TEMP_SCHEMA = os.environ.get("ONECDC_DB_TEMP_SCHEMA")

replicator = Replicator(
    odata_url=os.environ["ONECDC_ODATA_URL"],
    odata_auth=(os.environ["ONECDC_ODATA_USER"], os.environ["ONECDC_ODATA_PASSWORD"]),
    exchange_name=os.environ["ONECDC_EXCHANGE_NAME"],
    # Не знаете guid узла — оставьте пустым: в лог выведется список узлов плана обмена.
    queue_guid=os.environ.get("ONECDC_QUEUE_GUID", ""),
    engine=engine,
    db_schema=DB_SCHEMA,
    db_temp_schema=DB_TEMP_SCHEMA,
    full_load_workers=FULL_LOAD_WORKERS,
    automatic_full_load=AUTOMATIC_FULL_LOAD,
)

# --- Обработчики витрин: ПРИМЕР, замените на свои ------------------------------------------------
# Считают витрины из уже загруженных таблиц, каждый своим циклом. Классы — в handlers/ рядом.
#
# handler_zakazy_klientov = HandlerLoop(
#     engine=engine,
#     schema=DB_SCHEMA,
#     # Обработчик получит её в context.temp_schema и передаст в свой dbmerge.
#     temp_schema=DB_TEMP_SCHEMA,
#     handler=ZakazyKlientov(),
# )
#
# handler_zakazy_klientov_grouped = HandlerLoop(
#     engine=engine,
#     schema=DB_SCHEMA,
#     temp_schema=DB_TEMP_SCHEMA,
#     handler=ZakazyKlientovGrouped(),
# )

# --- Полные выгрузки по расписанию: ПРИМЕР, замените на свои --------------------------------------
# Зачем это нужно. Цикл изменений догоняет данные событиями, а полная выгрузка — сверяет: она читает
# объект из 1С и возвращает число РЕАЛЬНО изменённых строк, то есть при исправном CDC отвечает нулём.
# Ночная перегрузка свежего хвоста (последние сутки-двое) стоит недорого, а расхождение показывает
# сразу — и тут же его выравнивает.
#
# На объект приходится две строки: что грузим и по какому расписанию. Имена — те, что видны в БД
# (латиница), как и у обработчиков: настраивая выгрузку, смотрят в базу и в реестр
# onecdc_metadata_objects (колонки object_full_name_en / fields_en). Оригинальные имена 1С
# ("Document_ЗаказКлиента") тоже принимаются. Расписание — обычная crontab-строка, время локальное:
# в контейнере задавайте TZ.
#
# Каждую ночь в 03:00 — хвост за трое суток по документу, где важна свежесть. Граница timedelta
# считается в момент срабатывания, поэтому окно едет вместе с процессом. Фильтр по периоду заодно
# делает выгрузку устойчивее: документы листаются через $skip, и выборка за пару прошедших суток,
# в отличие от всего объекта, почти не меняется под ногами.
#
# zakazy_cron = FullLoadCron(replicator, "Document_ZakazKlienta", cron="0 3 * * *",
#                            date_field="Date", date_from=timedelta(days=3))
#
# По воскресеньям в 02:00 — справочник целиком: он маленький, границы не нужны вовсе.
#
# nomenklatura_cron = FullLoadCron(replicator, "Catalog_Nomenklatura", cron="0 2 * * 0")

# max_workers — по потоку на каждый run_forever ниже. Сейчас он один; с двумя обработчиками
# и двумя расписаниями будет max_workers=5.
with ThreadPoolExecutor(max_workers=1, thread_name_prefix='cdc') as pool:
    replicator_task = pool.submit(replicator.run_forever, interval=POLL_INTERVAL)
    # zakazy_klientov_task = pool.submit(handler_zakazy_klientov.run_forever)
    # zakazy_klientov_grouped_task = pool.submit(handler_zakazy_klientov_grouped.run_forever)
    # zakazy_cron_task = pool.submit(zakazy_cron.run_forever)
    # nomenklatura_cron_task = pool.submit(nomenklatura_cron.run_forever)

    replicator_task.result()
    # zakazy_klientov_task.result()
    # zakazy_klientov_grouped_task.result()
    # zakazy_cron_task.result()
    # nomenklatura_cron_task.result()
