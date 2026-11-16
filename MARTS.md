# Витрины dbmerge — beta

Требуется dbmerge >= 2.5.0. Существующие `Handler`/`HandlerLoop` работают по-прежнему.
Новые витрины используют отдельный журнал `dbmerge_tracking` и состояние
`dbmerge_marts`: старые сигналы и прогресс обработчиков автоматически не переносятся.

## Журнал репликатора

По умолчанию Replicator создаёт собственный `MergeTracker` в `db_schema`.
Он доступен как `rep.tracker` и закрывается вместе с репликатором. Если витрины
используют этот tracker, остановите их до `rep.close()`.

Чтобы хранить журнал отдельно от основных таблиц onecdc, передайте внешний
tracker со своей схемой:

```python
from dbmerge import MergeTracker
from onecdc import Replicator

# engine, odata_url и auth задаёт приложение; схема service уже существует.
with MergeTracker(engine, schema="service") as tracker:
    rep = Replicator(odata_url=odata_url, odata_auth=auth, engine=engine,
                     db_schema="source", tracker=tracker)
    try:
        rep.full_load("Catalog_Контрагенты")
    finally:
        rep.close()
```

Для CDC также задайте `exchange_name` и `queue_guid`. Перед закрытием tracker
остановите репликатор и дождитесь фоновых загрузок. Tracker принадлежит приложению.
В примере `service.dbmerge_tracking` хранит журнал, а данные и собственные
служебные таблицы onecdc остаются в схеме `source`. Внешний tracker не закрывается
при `rep.close()`. Для нескольких репликаторов и витрин удобно явно передавать
один общий tracker либо использовать один и тот же журнал из разных процессов.
В журнал попадают merge изменений, страницы полной загрузки и пометка пропавших
строк; нулевой результат не считается изменением. Scope — `changes`/`full_load`.
Все процессы записи источников должны использовать этот журнал: запись мимо него
не даёт витринам сигнала и не удерживает безопасную границу окна.

## Витрины принадлежат dbmerge

Витрина и её раннер не зависят от onecdc: импортируйте `MartHandler`,
`MartContext`, `MergeTracker` и функции управления напрямую из `dbmerge`.
OneCDC нужен только как производитель данных и записей общего журнала.
Репликатор и витрина могут работать в разных процессах, но должны видеть один
журнал. `tracker.schema` задаёт служебную схему; `destination` и `sources` —
бизнес-схемы `(schema, table)`.

Автономные примеры PostgreSQL и ClickHouse, блочная пересборка и раннер находятся
в [examples dbmerge 2.5.0](https://github.com/pavel-v-sobolev/dbmerge/tree/2.5.0/examples).
Полный контракт и ограничения описаны в
[TRACKING_AND_MARTS.md](https://github.com/pavel-v-sobolev/dbmerge/blob/2.5.0/TRACKING_AND_MARTS.md).

Локальный минимальный пример — `config/handlers/orders_by_customer.py`, запуск —
`config/runner_marts.py`. Он пересчитывает весь агрегат активных заказов и применяет его
через tracked merge с заимствованной арендой и учитывает удаления и перенос заказа между покупателями. Источник:
`orders(customer_id, amount, is_deleted_or_empty)`. Имена и типы адаптируйте
к своей схеме NameMapper. Для больших таблиц используйте примеры dbmerge.
Все четыре `ZakazyKlientov*` примера также переведены на `dbmerge.MartHandler`.
Они получают открытый tracker в конструкторе; каждая merge-запись явно передаёт
`tracker=context.tracker, lease_handle=context.lease_handle`.
PostgreSQL: `ZakazyKlientov(engine, tracker=tracker, schema="source")`.
ClickHouse: `ZakazyKlientovClickHouse(engine, clickhouse, "onecdc_pg",
tracker=tracker, schema="source")`. Классы `Grouped` настраиваются так же.
Схема источников PostgreSQL одновременно задаёт схему views и логическую
идентичность mart; физическая база ClickHouse задаётся аргументом `database`.
Междвижковая аренда не является атомарным SQL-fencing в ClickHouse.

```python
from dbmerge import MergeTracker
from config.handlers.orders_by_customer import OrdersByCustomer

with MergeTracker(engine, schema="service") as tracker:
    mart = OrdersByCustomer(engine, tracker=tracker,
                            destination=("dwh", "orders_by_customer"),
                            sources=[("source", "orders")])
    mart.run_once()               # один проход, исключения передаются вызывающему
    mart.run_once(rebuild=True)   # запросить полную пересборку и выполнить проход
    # mart.run_forever(poll_interval=60)
```

Tracker создаёт и закрывает приложение, только после остановки всех workers.
Для прямого управления циклом создайте `loop = mart.loop()` и гарантируйте
`loop.close()` в `finally`.
При непрерывном запуске `run_forever()` закрывает цикл на выходе.

## Явные аренды

Merge по умолчанию не арендует destination; mart по умолчанию координирует
setup, rebuild и changes. `lease_options=None` отключает координацию mart.
В callback, который пишет через dbmerge, явно передавайте аргументы
`tracker=ctx.tracker, lease_handle=ctx.lease_handle` конструктору merge.

`lease_options=LeaseOptions(...)` получает новую аренду, `lease_handle=...`
заимствует одно уже полученное владение без acquire/release. Вместе они
недопустимы. Неявного наследования аренды нет. Старые `lease=`, `with_lease=`
и `run_mart` удалены из beta API. `onecdc.marts` оставлен только для
совместимых импортов классов и функций управления, без адаптера запуска.

Все примеры пишут данные через tracked merge с явной арендой прохода.
Произвольный SQL вне merge не получает автоматически SQL-fencing.
Для защищённых merge используйте явный handle, как в примерах dbmerge.
Проверки между разными движками не являются распределённой транзакционной защитой.
Раздельные аренды не создают параллельное выполнение: один worker обрабатывает
changes между блоками rebuild. Эксперимент с двумя scope-фильтрованными циклами
заменён штатным контрактом dbmerge; перед миграцией остановите старые workers.

## Пересборка и остановка

```python
from dbmerge import request_rebuild, rebuild_requested, request_stop, reset_stop

destination = ("dwh", "orders_by_customer")
request_rebuild(engine, destination, schema="service")
pending = rebuild_requested(engine, destination, schema="service")
```

Состояние хранится в `dbmerge_marts`, без отдельной таблицы onecdc.
Активная пересборка сохраняет курсор; после ошибки проход продолжается.
Новый запрос во время пересборки не теряется. Прямое редактирование флага SQL
не заменяет API запроса. `request_stop()` останавливает все циклы dbmerge
в процессе; для нового запуска нужен `reset_stop()`. Перед закрытием tracker
дождитесь завершения workers. При сбое между коммитом данных и сохранением
watermark проход повторяется, поэтому callback должен быть идемпотентным.

## Живой тест с созданием трафика через OData

`tests/test_live_odata_traffic.py` предназначен только для тестового контура
`trade_demo1`. Явное разрешение записи обязательно:

```sh
ONECDC_LIVE_WRITE=1 uv run pytest tests/test_live_odata_traffic.py -m integration -o addopts='' -v -s
```

Тест полностью загружает источники во временную PostgreSQL-схему, строит обе
витрины, меняет код номенклатуры и номер заказа, восстанавливает поля,
перепроводит документ, отменяет и восстанавливает проведение. После каждого
CDC-прохода сравнивает все строки витрин с views через `EXCEPT ALL` в обе стороны.
Повторное чтение пакета не должно запускать лишние пересчёты.

`notify_changes=False`: пакеты не списываются. Исходные поля и состояние
проведения сохраняются в `/tmp/onecdc_traffic_*-restore.json` до записи;
восстановление выполняется в `finally`. Версии объектов, записи движений при
перепроведении и регистрации обмена не откатываются побитно. При отказе HTTP
восстановление тоже может не пройти: тест сообщает об ошибке и пути резервной
копии. Временная PostgreSQL-схема удаляется после прогона.
