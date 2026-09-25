# Запуск из окружения: переменные `ONECDC_*`

Режим, в котором onecdc работает **без единой строки вашего кода**: параметры задаются переменными
окружения, точка входа готова. Запускается командой `onecdc` либо как модуль:

```bash
onecdc
python -m onecdc     # то же самое
```

В контейнере это поведение по умолчанию: не смонтирован `/config` со своим `runner.py` — работает
именно этот режим (см. [README](README.md), «Запуск в docker»).

## Что этот режим умеет и чего не умеет

Умеет ровно то, что делает `Replicator`: читает изменения из 1С через OData и план обмена и
пишет их в целевую БД, подтверждая приём пакета только после успешного сохранения. В режиме
`loop` — плюс полные выгрузки объектов, которые встали в очередь (новый объект, заказанная
перевыгрузка): их раздаёт фоновым потокам сам цикл. В режиме `once` этого шага нет — одиночный
проход только помечает такие объекты, а выгрузит их следующий запуск в `loop` (или ваш вызов
`full_load`).

Не умеет ничего, что объявляется кодом:

- **обработчики** (витрины поверх загруженных таблиц) — их классы надо где-то написать;
- **полные выгрузки по расписанию** (`FullLoadCron`) — состав расписаний тоже задаётся кодом;
- **несколько планов обмена** в одном процессе.

Понадобилось что-то из этого — берите шаблон [config/runner.py](config/runner.py): там та же сборка,
только явным кодом, и всё перечисленное добавляется раскомментированием. Переменные окружения при
этом никуда не деваются — шаблон читает те же самые.

## Переменные

### Обязательные

| Переменная | Значение |
|---|---|
| `ONECDC_ODATA_URL` | адрес OData-интерфейса базы 1С, например `http://server/base/odata/standard.odata` (именно интерфейса, а не базы) |
| `ONECDC_EXCHANGE_NAME` | имя плана обмена в 1С — то, что видно в конфигураторе |
| `ONECDC_DB_URL` | строка подключения SQLAlchemy к целевой БД, например `postgresql+psycopg2://user:pass@host:5432/dwh` |
| `ONECDC_QUEUE_GUID` | `Ref_Key` узла обмена (очереди) |

`ONECDC_QUEUE_GUID` формально можно не задавать: чтение изменений тогда выведет в лог список узлов
плана обмена и остановится — guid берётся оттуда, без похода в 1С. Как единственный обязательный,
который разрешено оставить пустым, он и не проверяется на старте.

### Необязательные

| Переменная | По умолчанию | Значение |
|---|---|---|
| `ONECDC_ODATA_USER` | — | пользователь OData; не задан — запросы идут без авторизации |
| `ONECDC_ODATA_PASSWORD` | пусто | пароль этого пользователя. Кириллица допустима: заголовок авторизации отправляется в UTF-8. По `http://` пароль уходит открытым текстом — при старте об этом предупреждают в логе |
| `ONECDC_DB_SCHEMA` | схема БД по умолчанию (`public` у Postgres) | схема, в которой создаются таблицы данных |
| `ONECDC_DB_TEMP_SCHEMA` | схема данных | куда класть одноразовые таблицы ключей полной выгрузки |
| `ONECDC_FULL_LOAD_WORKERS` | `2` | число фоновых потоков полной выгрузки |
| `ONECDC_AUTOMATIC_FULL_LOAD` | `true` | ставить ли новый объект пакета на полную выгрузку самому; `false` — репликатор только читает изменения |
| `ONECDC_POLL_INTERVAL` | `60` | период опроса изменений, секунд (только в режиме `loop`) |
| `ONECDC_MODE` | `loop` | `loop` — вечный цикл; `once` — один цикл read → save → notify и выход |
| `ONECDC_LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` / `CRITICAL` |
| `ONECDC_LOG_RETENTION_DAYS` | `30` | сколько суток держать журнал загрузок (`onecdc_replicator_log`); `0` — хранить всё |
| `ONECDC_RUNNER` | `/config/runner.py` | **только в контейнере**: путь к своему runner'у, если он назван иначе |

Про `ONECDC_DB_TEMP_SCHEMA` стоит сказать отдельно: своя схема удобна тем, что в ней по определению
нет ничего ценного. Таблицу ключей, оставшуюся после падения процесса, там видно и не жалко удалить,
а таблицы с данными она не засоряет. Промежуточную таблицу самого merge эта схема не касается —
`dbmerge` заводит её настоящей `TEMPORARY` (подробнее в [README_DB.md](README_DB.md)).

`ONECDC_FULL_LOAD_WORKERS` влияет и на пул соединений: entrypoint заводит `create_engine` с
`pool_size = ONECDC_FULL_LOAD_WORKERS + 3` — соединение одновременно держат цикл изменений, два
потока отметки живости (незавершённых merge и захвата полной выгрузки) и страницы полной выгрузки
(подробнее — [README_DB.md](README_DB.md), раздел «Сколько нужно соединений к БД»).

Смонтированный `runner.py` читает те же `ONECDC_FULL_LOAD_WORKERS`, `ONECDC_POLL_INTERVAL`,
`ONECDC_LOG_LEVEL`, `ONECDC_LOG_RETENTION_DAYS` и `ONECDC_AUTOMATIC_FULL_LOAD`, а пул считает от числа своих обработчиков и
расписаний. Не читает он только `ONECDC_MODE`: что и как запускать, решает он сам — он и есть
режим.

`ONECDC_AUTOMATIC_FULL_LOAD=false` отключает только ЗАКАЗ выгрузки — привычку помечать новый
объект пакета как требующий полной. Объект, помеченный руками в `onecdc_metadata_objects`,
контейнер выгрузит по-прежнему. Ставят его тем, кто инициирует первую загрузку на стороне 1С
(см. [README.md](README.md), «Полная (первоначальная) выгрузка»).

### Не `ONECDC_*`, но важна

| Переменная | Значение |
|---|---|
| `TZ` | часовой пояс процесса, например `Europe/Moscow` |

В контейнере время по умолчанию UTC. На чтение изменений это не влияет, но расписания
`FullLoadCron` считаются в локальном времени, поэтому в конфигурациях с расписаниями `TZ` задавать
обязательно.

## Ошибки конфигурации

Проверяются на старте, до соединений с 1С и БД, и сообщают о себе одной строкой — без трейсбека:

```
ONECDC_ODATA_URL is not set (required: ONECDC_ODATA_URL, ONECDC_EXCHANGE_NAME, ONECDC_DB_URL)
ONECDC_POLL_INTERVAL='xx' is not a number
ONECDC_FULL_LOAD_WORKERS='-1' must be positive
ONECDC_AUTOMATIC_FULL_LOAD='da' is not a flag (expected true/false)
Unknown ONECDC_MODE='step' (expected 'loop' or 'once')
Unknown ONECDC_LOG_LEVEL='TRACE' (expected DEBUG/INFO/WARNING/ERROR/CRITICAL)
```

Так же ведут себя недоступные БД и 1С — вместо полутора сотен строк трейса сквозь SQLAlchemy или
requests выводится причина:

```
cannot connect to the database postgresql+psycopg2://user:***@dbhost:5432/dwh: could not translate host name "dbhost" to address: Name or service not known
1C is not available at http://server/base/odata/standard.odata: ... [Errno 113] No route to host
```

Пароль в адресе БД при этом скрыт: строка попадает в лог.

## Примеры

Локально, разовая проверка доступов (один цикл и выход):

```bash
ONECDC_ODATA_URL="http://server/base/odata/standard.odata" \
ONECDC_ODATA_USER=odata ONECDC_ODATA_PASSWORD=secret \
ONECDC_EXCHANGE_NAME="ДляODATA" \
ONECDC_QUEUE_GUID="a9bc23c5-3689-11f1-926c-0800270bc6cb" \
ONECDC_DB_URL="postgresql+psycopg2://user:pass@localhost:5432/dwh" \
ONECDC_DB_SCHEMA=onecdc ONECDC_DB_TEMP_SCHEMA=onecdc_tmp \
ONECDC_MODE=once \
onecdc
```

В контейнере те же переменные передаются ключами `-e` либо перечисляются в
[docker-compose.yml](docker-compose.yml) — там к каждой есть комментарий, и настройка читается
одним экраном. Полные команды запуска — в [README](README.md), раздел «Запуск в docker».
