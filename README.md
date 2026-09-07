[![PyPI version](https://img.shields.io/pypi/v/onecdc.svg)](https://pypi.org/project/onecdc/)
![Status](https://img.shields.io/badge/status-beta-yellow)
[![Python versions](https://img.shields.io/pypi/pyversions/onecdc.svg)](https://pypi.org/project/onecdc/)


<p align="left">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/pavel-v-sobolev/onecdc/main/assets/logo_dark.png">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/pavel-v-sobolev/onecdc/main/assets/logo_light.png">
    <img alt="OneCDC logo" src="https://raw.githubusercontent.com/pavel-v-sobolev/onecdc/main/assets/logo_light.png" width="500">
  </picture>
</p>

**OneCDC** is a docker container and a Python library, that provides **1C** system data loading to data warehouse using Change Data Capture apporach. \
It engages standard ODATA mechanism and standard 1C exchange plan mechanism to extract data from 1C system and upsert changes to the target DB.

**OneCDC** — это докер контейнер и python-библиотека, предназначенные для получения данных из **1С**, использующий подход CDC (загрузка изменений данных). \
Продукт использует стандартный интерфейс **ODATA** и механизм **планов обмена** для выгрузки изменений данных из системы 1С и обновления данных в целевой БД.

# Общий принцип действия
1) Основной объект библиотеки это оркестратор `Replicator`, который циклично читает изменения из 1С (через OData + план обмена) и
пишет их в целевую БД Postgres, подтверждая приём пакета только после успешного сохранения.
2) В Postgres объекты сохраняются в виде отдельных таблиц на каждый регистр, справочник, документ, табличные части документа или справочника. Структура соответствует структуре хранения в 1С, но имена таблиц и полей автоматически переводятся в транслит.
3) Полученные таблицы вы можете сами использовать для построения запросов, но библиотека предлагает также механизм обработчиков (handlers). Когда из 1С приходят новые данные, обработчик обновляет соответствующую часть в витрине данных. Витрину вы описываете сами — представлением (view) в базе данных. В примере показано как сделать обновление витрины быстрым, только по изменениям. Также механизм обработчиков это по сути ваш код на python, в который вы можете вставить что нужно.


# Что нужно для работы
1) опубликовать базу 1с на web
2) настроить план обмена в конфигураторе и включить в его состав нужные объекты 1с
3) создать пользователя для доступа к odata и дать ему необходимые права (`чтение` и `изменение` к плану обмена, `чтение` к загружаемым объектам).
4) дать роль `чтение` всем пользователям к плану обмена (иначе будут ошибки при сохранении объектов)
5) создать узел обмена с использованием внешней обработки `onecdc.epf`
6) запустить загрузку: докер-образом `sobolevp/onecdc` (см. «Запуск в docker») либо python-библиотекой `onecdc` (см. ниже)

# Использование библиотеки python

## Установка
```bash
pip install onecdc
```

Для записи изменений необходим **PostgreSQL** (Другие СУБД не тестировались, хотя в теории возможны).
Для записи используется библиотека [dbmerge](https://github.com/pavel-v-sobolev/dbmerge). Все необходимые схемы, таблицы и поля модуль создает сам.


```python
from sqlalchemy import create_engine
from onecdc import Replicator

# pool_size >= full_load_workers + 3 (+1 на каждого обработчика и каждое расписание) —
# почему столько, см. README_DB.md, «Сколько нужно соединений к БД»
engine = create_engine("postgresql+psycopg2://user:pass@localhost:5432/onecdc", pool_size=5)

rep = Replicator(
    odata_url="http://host/base/odata/standard.odata",
    odata_auth=("odata", "secret"),        # (user, password) либо None без авторизации
    exchange_name="ВашПланОбмена",              # имя плана обмена в 1С
    queue_guid="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",  # Ref_Key узла обмена
    engine=engine,
    db_schema="onecdc",                    # None → схема БД по умолчанию (public у Postgres)
    db_temp_schema="onecdc_tmp",           # схема промежуточных таблиц merge; None → схема данных
    request_timeout=60,                    # таймаут HTTP-запросов к 1С, сек (по умолчанию 60 на коннект, 900 на ответ)
    full_load_workers=2,                   # число фоновых потоков полной выгрузки
)

rep.run_forever(interval=60)               # цикл опроса раз в 60 секунд
```

Вариант многопоточного запуска, дающий возможность добавления нескольких обработчиков (Handler) и 
дополнительных репликаторов (Replicator) для других планов обмена, можно посмотреть в этом файле: [runner.py](https://github.com/pavel-v-sobolev/onecdc/blob/main/config/runner.py)



## Запуск в docker

```bash
docker run --rm --network host \
  -e ONECDC_ODATA_URL="http://server/base/odata/standard.odata" \
  -e ONECDC_ODATA_USER=odata -e ONECDC_ODATA_PASSWORD=secret \
  -e ONECDC_EXCHANGE_NAME="ВашПланОбмена" \
  -e ONECDC_QUEUE_GUID="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" \
  -e ONECDC_DB_URL="postgresql+psycopg2://user:pass@database_host:5432/cdc" \
  -e ONECDC_DB_SCHEMA=onecdc -e ONECDC_DB_TEMP_SCHEMA=onecdc_tmp \
  -v "$PWD/config:/config:ro" \
  sobolevp/onecdc:latest
```

`-v "$PWD/config:/config:ro"` — монтирует папку `config` из текущего каталога. В ней лежит файл [runner.py](https://github.com/pavel-v-sobolev/onecdc/blob/main/config/runner.py), а также подпапка с примерами обработчиков (config/handlers).

**docker compose.** Пример — [docker-compose.yml](https://github.com/pavel-v-sobolev/onecdc/blob/main/docker-compose.yml) в репозитории.


## Запуск из окружения

Если хочется не писать код вовсе, есть готовый entrypoint — `python -m onecdc` (он же команда
`onecdc`). Он читает те же параметры из переменных окружения, см описание: [README_ENV.md](https://github.com/pavel-v-sobolev/onecdc/blob/main/README_ENV.md)



### Как узнать guid узла обмена

`queue_guid` — это `Ref_Key` узла плана обмена, того самого, на который 1С регистрирует изменения
(`ЭтотУзел` не подходит: он описывает саму базу-источник). Если guid неизвестен, оставьте параметр
пустым (`queue_guid=""`, или просто не задавайте `ONECDC_QUEUE_GUID`) и запустите: чтение изменений
выведет в лог список узлов плана обмена и остановится.

```
ERROR onecdc.change_reader: queue_guid is not set. Available nodes of exchange plan ДляODATA:
    a9bc23c5-3689-11f1-926c-0800270bc6cb  CDC  Витрина
```

Guid из первой колонки и есть искомый `queue_guid`.

## Режимы: `run_once` и `run_forever`

```python
rep.run_once()                 # один цикл: read → save → notify (подтверждение только после save)
rep.run_forever(interval=60)   # основной режим работы. бесконечный цикл run_once с паузой; фоном — полные выгрузки
```

- `run_once(notify_changes=False)` — не подтверждать приём (пакет останется в очереди 1С; сделано для отладки).
- `run_forever(interval, max_iterations=0)` — `max_iterations>0` ограничивает число итераций.


## Полная (первоначальная) выгрузка

При работе `run_forever` объекты, впервые встреченные в пакете изменений, автоматически ставятся в
очередь на полную выгрузку и грузятся фоновыми потоками. 
- Можно запустить выгрузку и вручную, поставив флаг в таблице onecdc_metadata_objects - full_load_is_required=`True`
- Полная выгрузка спроектирована так, чтобы работать параллельно с получением изменений объекта и не затирать свежие изменения объекта. Также она автоматически разбивает объект на отрезки по времени и выгружает его порциями с автоматическим подбором размера (batch_size).


## Что появляется в целевой БД

На каждый объект 1С заводится своя таблица: имя транслитерируется, к полям добавляются служебные
(`merged_on`, `inserted_on`, `is_deleted_or_empty`, `exchange_message_no`). Схемы, таблицы и новые
колонки библиотека создаёт сама. Рядом появляются служебные таблицы, по которым видно состояние
загрузки: журнал `onecdc_replicator_log`, реестр объектов `onecdc_metadata_objects`, состояние обработчиков
`onecdc_handlers` и реестр незавершённых записей `onecdc_writes_in_process`.

Подробно — [README_DB.md](https://github.com/pavel-v-sobolev/onecdc/blob/main/README_DB.md): состав полей, что означает `is_deleted_or_empty`, зачем
отдельная схема промежуточных таблиц и что лежит в каждой служебной таблице.

## Классы библиотеки

`Replicator` читает изменения и пишет их в БД, `Handler` + `HandlerLoop` дают возможность запуска своего кода
по событию изменения, `FullLoadCron` — полную выгрузку по расписанию. Что каждый из них принимает и
что у него можно вызвать — [README_API.md](https://github.com/pavel-v-sobolev/onecdc/blob/main/README_API.md).

## Логирование

Из коробки библиотека вешает вывод на логгер `onecdc` (INFO), если приложение не настроило логирование
само. Настроили своё — библиотека молчит и пишет через стандартный `logging`.

## Шум со стороны 1С

1С регистрирует изменение объекта на любую перезапись, поэтому в пакет приезжает масса записей, у которых поменялись только `DataVersion` и номер пакета. Такие записи библиотека изменением не считает: строка не обновляется, `merged_on` остаётся прежним — и обработчик, который выбирает данные по `merged_on`, впустую не запускается.


## Дальнейшая обработка и материализация

1С хранит данные нормализованно: чтобы дотянуться из регистра до кода товара, нужен `JOIN` со
справочником по guid. Для КХД обычно удобнее менее строгая нормализация — витрины, собранные
заранее.

Считать их вхолостую по расписанию не нужно: репликатор знает, когда данные изменились, и сам
вызывает ваш код. Такой код называется **обработчиком** — это класс с двумя строчками объявления:

```python
from onecdc import Handler, HandlerContext

class ZakazyKlientov(Handler):
    ON = ["AccumulationRegister_ZakazyKlientov"]   # имена ТАБЛИЦ в БД, не объектов 1С

    def handle(self, context: HandlerContext) -> None:
        self.execute(context, "insert into ... where merged_on > :last_run_at",
                     last_run_at=context.last_run_at)
```

Тем же способом изменения отправляют во внешнюю систему — очередь, вебхук, другую БД.

Начинают обычно с **представления (view)**: в нём собран `SELECT` со всеми `JOIN` по guid-ключам.
DDL можно прописать прямо в обработчике (`setup`) или завести отдельно. Дальше обработчик работает так:

- библиотека вызывает его, передавая дату и время `last_run_at` — с них и нужно обновлять;
- обработчик пишет `SELECT`, который находит по представлению изменившиеся ключи условием
  `merged_on > last_run_at` (`merged_on` есть и у таблиц, подключённых через `JOIN`, — см. пример);
- этот `SELECT` уходит в условия обновления
  [dbmerge](https://github.com/pavel-v-sobolev/dbmerge): `source_condition` говорит, какие ключи
  взять из источника, `delete_condition` — в каком множестве ключей чистить строки, если что-то
  удалилось. `delete_condition` нужен не всегда: строки, выпавшие из набора движений или
  табличной части, репликатор не удаляет, а помечает (см. «Шум со стороны 1С» и `is_deleted_or_empty`),
  поэтому витрине с тем же ключом, что у источника, достаточно обычного обновления. Удаление
  нужно там, где ключ витрины свой, — например, у агрегата по `GROUP BY`.

В примерах разобраны два варианта — с кодом и подробными пояснениями:

1) ключ витрины совпадает с первичным ключом объекта 1С (суррогатный guid). Годится для таблицы
   фактов: [config/handlers/zakazy_klientov.py](https://github.com/pavel-v-sobolev/onecdc/blob/main/config/handlers/zakazy_klientov.py);
2) ключ витрины заменён на бизнес-ключ — например, номер документа вместо guid. Связь с «сырыми»
   данными хранится в колонках-массивах (`ARRAY`) с индексами *GIN*: по ним обработчик быстро
   находит, какие агрегированные ключи задело изменение в источнике.
   [config/handlers/zakazy_klientov_grouped.py](https://github.com/pavel-v-sobolev/onecdc/blob/main/config/handlers/zakazy_klientov_grouped.py).


---

## Ссылки

| файл | описание |
|---|---|
| [README.md](https://github.com/pavel-v-sobolev/onecdc/blob/main/README.md) | этот файл: обзор, установка, запуск |
| [README_ENV.md](https://github.com/pavel-v-sobolev/onecdc/blob/main/README_ENV.md) | запуск из окружения: все переменные `ONECDC_*`, значения по умолчанию, ошибки конфигурации |
| [README_API.md](https://github.com/pavel-v-sobolev/onecdc/blob/main/README_API.md) | классы, которые собирает пользователь: `Replicator`, `Handler`, `HandlerLoop`, `FullLoadCron`; обработчики — окно изменений, когда их вызывают, пересборка витрины |
| [README_DB.md](https://github.com/pavel-v-sobolev/onecdc/blob/main/README_DB.md) | что появляется в целевой БД: таблицы, служебные поля, служебные таблицы |
| [DESIGN.md](https://github.com/pavel-v-sobolev/onecdc/blob/main/DESIGN.md) | внутреннее устройство для тех, кто правит код: интерфейс OData, цикл изменений, пагинация полной выгрузки, гонки со снимком, механика обработчиков |
| [CHANGELOG.md](https://github.com/pavel-v-sobolev/onecdc/blob/main/CHANGELOG.md) | что менялось от версии к версии |
| [config/runner.py](https://github.com/pavel-v-sobolev/onecdc/blob/main/config/runner.py) | шаблон точки входа: репликатор, обработчики и расписания в одном файле |
| [config/handlers](https://github.com/pavel-v-sobolev/onecdc/tree/main/config/handlers) | примеры обработчиков |


---

<sub>Неофициальный проект, не связан с фирмой «1С». «1С» и «1С:Предприятие» — товарные знаки ООО «1С», используются для указания совместимости.</sub>