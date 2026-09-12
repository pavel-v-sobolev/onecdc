"""
Имена таблиц и колонок в БД по именам объектов и полей 1С.

Транслитерация сама по себе НЕ инъективна и инъективной быть не может: `е` и `э` дают `e`,
`ъ` и `ь` исчезают, а кириллическое имя вообще способно совпасть с латинским (`Ёлка` → `Yolka`).
Сделать её инъективной можно только сменив таблицу замен, а это переименование всех существующих
таблиц и колонок у каждой установки.

Поэтому имя не вычисляется, а ЗАКРЕПЛЯЕТСЯ: таблица `onecdc_name_claims` хранит соответствие
«имя 1С ↔ идентификатор в БД», и арбитром выступает уникальный индекс по идентификатору — ровно
так же, как захват объекта под выгрузку разводится CAS-обновлением (см. full_load_claim).
Однажды выданное имя не меняется больше никогда, поэтому все процессы всех поколений — репликатор,
расписание, обработчики в других контейнерах — читают одно и то же.

Транслит при этом остаётся ПЕРВЫМ кандидатом, а не результатом. Из этого следует главное свойство
для действующих установок: при первом запуске после обновления каждый объект заявляет в точности
то имя, которое у него сейчас, и ничего не переименовывается.
"""

import hashlib
import threading
import uuid

from sqlalchemy import Column, DateTime, Engine, Index, MetaData, String, Table, func, insert, select
from sqlalchemy.exc import IntegrityError

from onecdc.common_functions import truncate_to_bytes
from onecdc.db_logs import (_check_create_schema, create_index_if_absent,
                            create_table_if_absent)
from onecdc.logging_config import get_logger

logger = get_logger(__name__)

_TRANSLIT_TABLE = str.maketrans({
    'а': 'a',  'б': 'b',  'в': 'v',  'г': 'g',  'д': 'd',
    'е': 'e',  'ё': 'yo', 'ж': 'zh', 'з': 'z',  'и': 'i',
    'й': 'j',  'к': 'k',  'л': 'l',  'м': 'm',  'н': 'n',
    'о': 'o',  'п': 'p',  'р': 'r',  'с': 's',  'т': 't',
    'у': 'u',  'ф': 'f',  'х': 'kh', 'ц': 'ts', 'ч': 'ch',
    'ш': 'sh', 'щ': 'sch','ъ': '',   'ы': 'y',  'ь': '',
    'э': 'e',  'ю': 'yu', 'я': 'ya',
    'А': 'A',  'Б': 'B',  'В': 'V',  'Г': 'G',  'Д': 'D',
    'Е': 'E',  'Ё': 'Yo', 'Ж': 'Zh', 'З': 'Z',  'И': 'I',
    'Й': 'J',  'К': 'K',  'Л': 'L',  'М': 'M',  'Н': 'N',
    'О': 'O',  'П': 'P',  'Р': 'R',  'С': 'S',  'Т': 'T',
    'У': 'U',  'Ф': 'F',  'Х': 'Kh', 'Ц': 'Ts', 'Ч': 'Ch',
    'Ш': 'Sh', 'Щ': 'Sch','Ъ': '',   'Ы': 'Y',  'Ь': '',
    'Э': 'E',  'Ю': 'Yu', 'Я': 'Ya',
})


def _translit(s: str) -> str:
    return s.translate(_TRANSLIT_TABLE)


# Лимит длины идентификатора в PostgreSQL — 63 БАЙТА (не символа: буквы вне таблицы транслита
# остаются многобайтовыми, см. truncate_to_bytes).
POSTGRES_MAX_IDENTIFIER = 63
HASH_LENGTH = 4
# Длина суффикса `_хэш`: на неё укорачивается основа, чтобы итог уложился в лимит.
_SUFFIX_LENGTH = HASH_LENGTH + 1
# Сколько раз пробовать имя с новой солью, прежде чем сдаться. Каждая попытка — независимый
# бросок по 16 битам, так что предел здесь только затем, чтобы в коде не было цикла без границы.
MAX_CLAIM_ATTEMPTS = 100

# Служебные имена колонок (добавляются при загрузке/сохранении). Заявляются в реестре за самих
# себя, поэтому поле 1С, которое транслитерируется в служебное имя, обнаружит идентификатор
# занятым и получит хэш общим порядком — без отдельной ветки.
RESERVED_FIELD_NAMES = ('merged_on', 'inserted_on', 'exchange_message_no', 'is_deleted_or_empty')

CLAIMS_TABLE = "onecdc_name_claims"
# Пространства имён: имя таблицы и имя колонки друг с другом не конфликтуют.
SCOPE_OBJECT = 'object'
SCOPE_FIELD = 'field'


def _short_hash(name: str) -> str:
    return hashlib.md5(name.encode('utf-8')).hexdigest()[:HASH_LENGTH]


def fit_identifier_length(mapped: str) -> str:
    """
    Уложить готовый идентификатор в лимит длины. Первый кандидат на имя таблицы/колонки — и он же
    единственное правило для производных имён, которые арбитр не выдаёт (имена индексов: они
    живут в своём пространстве имён и однозначно выводятся из уже уникального имени таблицы).

    Хэш здесь считается от ТРАНСЛИТА и имеет ровно ту ширину, что и раньше, — это не небрежность,
    а совместимость: так имя, выданное предыдущими версиями, совпадает с первым кандидатом, и при
    переходе на реестр заявок ни одна существующая таблица не переименовывается.
    """
    if len(mapped.encode('utf-8')) <= POSTGRES_MAX_IDENTIFIER:
        return mapped
    return _hashed(mapped, _short_hash(mapped))


def _hashed(mapped: str, digest: str) -> str:
    return truncate_to_bytes(mapped, POSTGRES_MAX_IDENTIFIER - _SUFFIX_LENGTH) + '_' + digest


def _candidates(source_name: str, mapped: str):
    """
    Кандидаты на идентификатор, от лучшего к запасным. Перебор останавливает арбитр — первый,
    который удалось закрепить за этим именем 1С.

    Хэшируем то, что ещё РАЗЛИЧАЕТ имена: при переполнении длины это транслит (до усечения имена
    различны), при коллизии транслит у обоих одинаков по определению — значит только оригинал.
    Ширины хэша в 16 бит хватает именно потому, что она ни за что не отвечает: промах стоит
    одной лишней попытки, а не смешения данных.
    """
    yield fit_identifier_length(mapped)
    yield _hashed(mapped, _short_hash(source_name))
    for _ in range(MAX_CLAIM_ATTEMPTS):
        # Соль, а не счётчик: суффикс остаётся той же длины, и основа усекается одинаково.
        # Случайная, а не по часам — попытки идут подряд, и метка низкого разрешения дала бы
        # тот же хэш и вечный цикл (а часы в этом проекте ещё и немонотонны, см. DESIGN.md).
        yield _hashed(mapped, _short_hash(source_name + uuid.uuid4().hex))


def _mapped(scope: str, name: str) -> str:
    """Транслит имени. У объекта тип (Document, Catalog, …) остаётся как есть — он и так латиница."""
    if scope == SCOPE_OBJECT and '_' in name:
        prefix, _, rest = name.partition('_')
        return f'{prefix}_{_translit(rest)}'
    return _translit(name)


def _claims_table(metadata: MetaData, schema_name: str | None) -> Table:
    return Table(
        CLAIMS_TABLE, metadata,
        # Пространство имён: SCOPE_OBJECT (таблицы) или SCOPE_FIELD (колонки).
        Column("scope", String(16), primary_key=True),
        # Имя в 1С. В паре со scope — ключ: у одного имени 1С ровно один идентификатор.
        Column("source_name", String(255), primary_key=True),
        Column("identifier", String(POSTGRES_MAX_IDENTIFIER), nullable=False),
        # Часы БД, как и у остальных служебных таблиц: время проставляет та же сторона,
        # что и сравнивает, — и поштучная вставка, и пакетная получают его одинаково.
        Column("claimed_at", DateTime, nullable=False, default=func.now()),
        schema=schema_name,
    )


def _identifier_index(table: Table) -> Index:
    # ЭТО и есть арбитр: он атомарно пропускает ровно одного претендента на идентификатор,
    # без блокировок и без повышения уровня изоляции.
    return Index(f'ux_{CLAIMS_TABLE}_identifier', table.c.scope, table.c.identifier, unique=True)


class NameMapper:
    """
    Транслитерирует имена 1С в идентификаторы PostgreSQL и закрепляет их в реестре заявок.

    С engine — рабочий режим: имя проходит через арбитра, столкнувшиеся имена разводятся.
    Без engine — offline (см. NameMapper.offline): считается только первый кандидат, реестр не
    читается и не пишется. Годится там, где базы рядом нет и коллизии не важны (экспорт в JSON,
    подсказки в текстах ошибок); данные в БД через такой маппер писать нельзя.
    """

    def __init__(self, engine: Engine | None = None, schema: str | None = None):
        self.engine = engine
        self.schema_name = _check_create_schema(engine, schema) if engine is not None else schema
        self.table = _claims_table(MetaData(), self.schema_name) if engine is not None else None
        # Соответствия (оригинал -> результат), сохраняются для отладки.
        self.object_mappings: dict[str, str] = {}
        self.field_mappings: dict[str, str] = {}
        # Кэш заявок: без него поход в базу случался бы на каждую пачку записей.
        self._claims: dict[tuple[str, str], str] = {}
        self._loaded = False
        self._lock = threading.Lock()

    @classmethod
    def offline(cls) -> "NameMapper":
        """Маппер без реестра — явным именем, чтобы это не получалось случайно."""
        return cls()

    def map_object_name(self, name: str) -> str:
        """
        Имя объекта вида "Document_ЗаказКлиента":
        тип (Document) оставляем без изменений, русскую часть транслитерируем.
        """
        result = self._resolve(SCOPE_OBJECT, name, _mapped(SCOPE_OBJECT, name))
        self.object_mappings[name] = result
        return result

    def map_field_name(self, name: str) -> str:
        result = self._resolve(SCOPE_FIELD, name, _mapped(SCOPE_FIELD, name))
        self.field_mappings[name] = result
        return result

    def get_column_mapping(self, columns: list[str]) -> dict[str, str]:
        return {col: self.map_field_name(col) for col in columns}

    def prefetch(self, scope: str, source_names) -> None:
        """
        Закрепляет пачку имён одной вставкой. Ускорение ПЕРВОГО запуска, не более: заявок столько
        же, сколько объектов и полей во всей конфигурации 1С (десятки тысяч), а по транзакции на
        каждую — это минута на старте. Дальше реестр читается одним запросом и ничего не пишется.

        Берёт только бесспорные имена: те, чей первый кандидат ещё никем не занят и не повторяется
        внутри самой пачки. Всё остальное — коллизии и проигранные гонки — расходится обычным
        поштучным путём, где и живёт вся логика перебора кандидатов. Поэтому метод можно не
        вызывать вовсе: он влияет на скорость, но не на результат.
        """
        if self.engine is None:
            return
        self._load_claims()
        taken = {identifier for (claim_scope, _), identifier in self._claims.items()
                 if claim_scope == scope}
        pending: dict[str, str] = {}
        for source_name in source_names:
            if (scope, source_name) in self._claims or source_name in pending:
                continue
            candidate = fit_identifier_length(_mapped(scope, source_name))
            if candidate in taken:
                continue
            taken.add(candidate)
            pending[source_name] = candidate
        if not pending:
            return

        try:
            with self.engine.begin() as conn:
                conn.execute(insert(self.table), [
                    {'scope': scope, 'source_name': source_name, 'identifier': identifier}
                    for source_name, identifier in pending.items()])
        except IntegrityError:
            # Параллельный процесс успел занять что-то из пачки. Разбирать, что именно, незачем:
            # поштучный путь перечитает реестр и выдаст верный ответ каждому имени.
            logger.info('Bulk name claim lost a race, falling back to one-by-one')
            return
        with self._lock:
            self._claims.update({(scope, source_name): identifier
                                 for source_name, identifier in pending.items()})

    # --- Реестр заявок ---

    def _resolve(self, scope: str, source_name: str, mapped: str) -> str:
        cached = self._claims.get((scope, source_name))
        if cached is not None:
            return cached

        candidates = _candidates(source_name, mapped)
        if self.engine is None:
            return next(candidates)

        self._load_claims()
        cached = self._claims.get((scope, source_name))
        if cached is not None:
            return cached

        for position, candidate in enumerate(candidates):
            claimed = self._claim(scope, source_name, candidate)
            if claimed is None:
                continue    # идентификатор занят другим именем 1С — следующий кандидат
            if claimed != candidate or position > 0:
                self._log_collision(scope, source_name, mapped, claimed)
            with self._lock:
                self._claims[(scope, source_name)] = claimed
            return claimed

        raise RuntimeError(
            f'Cannot allocate a database identifier for {scope} {source_name!r} after '
            f'{MAX_CLAIM_ATTEMPTS} attempts (base name {mapped!r})')

    def _claim(self, scope: str, source_name: str, identifier: str) -> str | None:
        """
        Пробует закрепить идентификатор. Возвращает имя, которое в итоге принадлежит source_name,
        либо None — идентификатор занят другим именем 1С, нужен следующий кандидат.

        Гонка разводится уникальным индексом, а не блокировкой: две попытки занять один
        идентификатор — это ровно то, от чего индекс и защищает, и проигравшему остаётся
        перечитать. Отдельная транзакция на попытку обязательна: нарушение ограничения в
        PostgreSQL отменяет транзакцию целиком, продолжать в ней уже нельзя.
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(insert(self.table).values(
                    scope=scope, source_name=source_name, identifier=identifier))
            return identifier
        except IntegrityError:
            pass

        # Конфликт двусмысленный: либо нас уже зарегистрировал параллельный процесс (тогда берём
        # его результат — он такой же законный), либо идентификатор достался чужому имени.
        t = self.table
        with self.engine.connect() as conn:
            return conn.execute(select(t.c.identifier).where(
                t.c.scope == scope, t.c.source_name == source_name)).scalar()

    def _load_claims(self) -> None:
        """Читает реестр целиком одним запросом — дальше поход в базу нужен только на новое имя."""
        if self._loaded:
            return
        with self._lock:
            if self._loaded:
                return
            create_table_if_absent(self.engine, self.table)
            create_index_if_absent(self.engine, _identifier_index(self.table),
                                   CLAIMS_TABLE, self.schema_name)
            t = self.table
            with self.engine.connect() as conn:
                rows = conn.execute(select(t.c.scope, t.c.source_name, t.c.identifier)).all()
            self._claims.update({(row.scope, row.source_name): row.identifier for row in rows})
            self._loaded = True

        # Служебные колонки закрепляем за собой, иначе их мог бы занять реквизит 1С с таким же
        # транслитом — и та бы колонка, которой управляет dbmerge или парсер, оказалась чужой.
        for name in RESERVED_FIELD_NAMES:
            if (SCOPE_FIELD, name) not in self._claims:
                claimed = self._claim(SCOPE_FIELD, name, name)
                with self._lock:
                    self._claims[(SCOPE_FIELD, name)] = claimed or name

    def _log_collision(self, scope: str, source_name: str, mapped: str, claimed: str) -> None:
        t = self.table
        with self.engine.connect() as conn:
            owner = conn.execute(select(t.c.source_name).where(
                t.c.scope == scope, t.c.identifier == fit_identifier_length(mapped))).scalar()
        logger.warning(
            'Name collision: %s %r transliterates to %r, which belongs to %r — using %r instead',
            scope, source_name, fit_identifier_length(mapped), owner, claimed)
