# План проекта onecdc

## Context

Ядро CDC из 1С (OData → БД через dbmerge) реализовано и проверено на живой 1С + PostgreSQL:
чтение изменений (`ChangeReader`), разбор в колоночные `DataObject`, транслитерация и лимиты
имён (`NameMapper`), типы/ключи/`delete_key` из метаданных (`MetadataReader`), пер-объектное
сохранение со scoped-delete для регистров/ТЧ (`DBWriter`), спец-поля `is_deleted_or_empty` и
`exchange_message_no`.

Текущая цель: оформить наработку как opensource-продукт — Python-пакет на PyPI + Docker-образ для
запуска «из коробки» с минимумом настроек, и удобный оркестратор с простым вызовом.

## Решения
- Имя: дистрибутив **onecdc**, import-пакет **onecdc** (нижний регистр); классы PascalCase.
- Оркестратор: класс **`Replicator`** (имена публичных классов без суффикса `1C` — он и
  так есть в имени пакета; суффикс убран в 0.2.0, см. CHANGELOG).
- Конструктор: **отдельные именованные аргументы** (без объекта настроек) — прямой библиотечный
  вызов без обёртки, единый стиль с прочими классами. БД передаётся готовым **`engine`** (DI по-
  sqlalchemy'ному, тестируемость, переиспользование), а не строкой; тот же engine идёт в `DBWriter`.
- Объекта настроек нет: и entrypoint (`python -m onecdc`), и пользовательский runner.py читают
  окружение сами, присваивают аргументы явно и сами строят engine — так на месте вызова видно, что
  именно передано, одинаково для python-приложения и для контейнера.
- Режимы оркестратора: `run_once()` и `run_forever(interval)`.
- Python: **>=3.10**. Лицензия: **MIT** (предварительно).

---

## Сделано (ядро CDC)

- **`_get_register_records` / удаление наборов** — `_default_key_value`, `_make_deleted_register_record`:
  при пустом `RecordSet` запись дополняется полным ключом из метаданных (дефолты) + реальные
  `Recorder`/`Recorder_Type`.
- **`NameMapper`** — транслитерация; лимит `POSTGRES_MAX_IDENTIFIER = 63` (обрезка + 4-симв. хэш);
  служебные имена `RESERVED_FIELD_NAMES` (`merged_on`, `inserted_on`, `exchange_message_no`): наше
  служебное поле сохраняет имя, поле 1С с таким же транслитом хэшируется; журнал
  `object_mappings`/`field_mappings`. Ручной маппинг убран.
- **`DataReader`** — `DataObject.to_records_mapped()` (dict-of-lists → list-of-dict, маппинг
  колонок на лету, без копии); `_convert_value` для `Guid` → `uuid.UUID`.
- **`DBWriter`** — пер-объектное сохранение; удаление по `metadata_obj.delete_key`: документ/
  справочник → `delete_mode='no'`; регистр/ТЧ → `delete_mode='delete'` со scoped `delete_condition`
  (`_scoped_delete_condition`: подзапрос из temp-таблицы — `col IN (SELECT col FROM temp)` или
  row-value `tuple_(...).in_(select(...))`). `data_reader` в конструкторе, `save_all()`.
- **`MetadataReader`** — `MetadataObject.delete_key` + `_get_delete_key` (регистр →
  `Recorder`/`Recorder_Type`; ТЧ → `Ref_Key`; документ/справочник → None).
- **Табличные части** — строкам ТЧ проставляется `Ref_Key` владельца (в данных 1С его нет);
  пустая ТЧ → фиктивная запись (`_make_empty_table_part_record`) для scoped-delete по `Ref_Key`.
- **Спец-поля** — `is_deleted_or_empty` (Boolean: `DeletionMark` документа/справочника, проброс
  пометки в строки ТЧ, `True` у фиктивных записей) и `exchange_message_no` (номер пакета обмена,
  во всех записях; ставится в `ChangeReader.read_changes`).
- **A1. Переименование** — `src/onecdc` → `src/onecdc`, все импорты `onecdc` → `onecdc`,
  `pyproject` `name = "onecdc"`, `version("onecdc")`. Пересобрано `uv sync` (onecdc==0.1.0).

---

## Осталось

### Фаза A. Hardening ядра
- **A2. Конфигурируемый auth** — ✅ (частично) `MetadataReader`/`DataReader.__init__` принимают
  `auth: tuple[str,str] | None`, хранят `self.auth`; все `requests.*` используют `auth=self.auth`;
  `ChangeReader` пробрасывает `auth`. Захардкоженные `('admin','admin')` убраны (4 места).
  Осталось: общий `requests.Session`, `timeout`, опц. `verify`.
- **A3. Сброс состояния** — ✅ `ChangeReader.read_changes()` в начале делает `self.clear()`.
- **A4. Настройки из окружения** — ✅ разбор `ONECDC_*` живёт в `src/onecdc/__main__.py` (отдельный
  класс настроек убран как лишний слой). Осталось: опц. `request_timeout`, `verify_ssl` (с остатком A2).
- **A5. Оркестратор `Replicator`** — ✅ `src/onecdc/replicator.py`: собирает
  metadata/changes/mapper/writer из отдельных аргументов (engine + auth из user/password);
  `run_once()` (read → save → **notify только после
  успешного save**); `run_forever(interval)` с обработкой исключений (упал цикл → лог, без notify,
  повтор) и graceful SIGTERM/SIGINT (`_StopSignal`). Экспортирован из `onecdc`.
- **A6. Логирование** — ✅ во всех модулях `logger = logging.getLogger(__name__)`, `basicConfig()`/root
  убраны (ридеры/writer/replicator). Авто-вывод из коробки — `src/onecdc/logging_config.py`
  `_ensure_handler()` (вешает StreamHandler на логгер `onecdc` с INFO, только если `hasHandlers()` ==
  False), вызывается из `Replicator.__init__`. Если приложение настроило логирование — молчим.
  Уровень из `ONECDC_LOG_LEVEL` — настраивается в entrypoint (B2). Проверено: на чистом root INFO виден
  из коробки, при настроенном приложением логировании — молчим. Зависимость **dbmerge** переведена на
  тот же паттерн (`getLogger('dbmerge')` + `hasHandlers()`, без `basicConfig`) — root больше не
  загрязняется, обе библиотеки сосуществуют чисто.
- ✅ Файлы модулей в snake_case (`metadata_reader.py`, `data_reader.py`, `change_reader.py`,
  `name_mapper.py`, `db_writer.py`); классы и реэкспорт из `__init__` не изменены, публичный API
  (`from onecdc import …`) прежний.

### Фаза B. Упаковка для PyPI
- **B1. `pyproject.toml`** — описание, `license = "MIT"`, авторы, `requires-python = ">=3.10"`,
  keywords/classifiers/urls; ядро: `dbmerge`, `requests`, `sqlalchemy`, `xmltodict` (убрать `polars`
  и `psycopg2` из обязательных); `[project.optional-dependencies] postgres = ["psycopg[binary]>=3.2"]`
  (psycopg3, DSN `postgresql+psycopg`); `[project.scripts] onecdc = "onecdc.__main__:main"`.
- **B2. Entrypoint** — `src/onecdc/__main__.py`: `ONECDC_*` → явные аргументы `Replicator`; режим
  `ONECDC_MODE=once|loop` (по умолчанию loop). Работает как `python -m onecdc` и команда `onecdc`.
- **B3.** `LICENSE`, `src/onecdc/py.typed`, `CHANGELOG.md`; английский README (quickstart pip+Docker,
  таблица ENV, требования к плану обмена OData в 1С, поведение имён, спец-поля, ограничения);
  `tests/debug.py` — отладочный вход для ручных прогонов против живой 1С.

### Фаза C. Docker «из коробки» — НЕ СДЕЛАНО
Ни `Dockerfile`, ни `docker-compose.yml` в репозитории нет, хотя README уже говорит про контейнеры
и про том с `example_config/`. Что нужно:
- **C1. `Dockerfile`** — `python:3.12-slim`, `onecdc[postgres]`, `ENTRYPOINT ["onecdc"]`, ENV,
  graceful SIGTERM (перехват сигналов ставится в конструкторах, см. `stop_signal`).
- **C2. `docker-compose.yml`** — сервис `onecdc` + опциональный `postgres`; `.env.example`;
  README-раздел «Запуск в Docker за 1 минуту».

### Фаза D. Качество
- **D1. Тесты (pytest)** — ✅ идут против локального PostgreSQL, каждому тесту своя схема
  (`tests/conftest.py`). sqlite не используется: он отличается ровно в тех местах, которые тесты и
  проверяют (схемы, точность `CURRENT_TIMESTAMP`, соединение = отдельная база). Живые тесты 1С —
  под маркером `integration`, по умолчанию отсеиваются.
- **D2. CI (GitHub Actions)** — ✅ матрица 3.10–3.14 с `services: postgres`; публикация на PyPI по
  git-тегу (OIDC/trusted publishing). Линтера в CI пока нет.

---

## Известные ограничения (→ README)
- Фиктивные записи (удалённый набор регистра, пустая ТЧ) вставляются как заглушка с дефолтным
  ключом (`LineNumber=0`); реальные старые строки удаляются scoped-delete'ом. Отличаются по
  `is_deleted_or_empty=True`.
- Опустевшая ТЧ как `xsi:nil` сейчас отфильтровывается (`_get_record_table_parts`) — уточнить формат
  1С и допокрыть при необходимости.
- Уникальность обрезанных длинных имён строго не гарантируется (хэш от полного имени).

## Проверка (end-to-end)
1. `uv sync`; `pytest` — зелёные offline-тесты (без 1С, но Postgres нужен).
2. `Replicator(...).run_once()` против живой 1С + Postgres: таблицы, типы,
   scoped-delete, спец-поля; повторный прогон — идемпотентность.
3. `run_forever(interval=…)`: цикл чистит состояние, notify только после успешного save, реакция на SIGTERM.
4. Docker (после фазы C): `docker compose up` с заполненным `.env` → данные грузятся без правок
   кода; `docker stop` штатно завершает.
5. `uv build` + установка из wheel в чистом окружении (3.10): импорт и `onecdc`/`python -m onecdc`.
