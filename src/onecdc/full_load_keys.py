"""
Ключи, увиденные полной выгрузкой, и пометка строк, которых в 1С больше нет.

Зачем. Физическое удаление объекта (после «Удаления помеченных объектов») в обмен не приходит:
в пакете изменений понятия «объект удалён» нет вовсе, а у независимого регистра сведений нет даже
scoped-удаления (у него нет регистратора, и набор нечем ограничить). Такая строка остаётся в целевой
таблице навсегда. Полная выгрузка — единственное место, где это видно: она читает объект целиком и
знает, чего в нём не оказалось.

Как. Диапазоном ключей страницы обойтись нельзя: 1С сортирует ссылочные ключи автоупорядочиванием
(по представлению), а не по guid (см. DESIGN.md, «Почему страницы берутся только через $skip»),
поэтому «всё между первым и последним ключом страницы» — не то множество. Вместо этого каждая
страница дописывает свои ключи в отдельную таблицу, а после успешного завершения прогона строки,
которых в ней нет, помечаются одним UPDATE.

Таблица ключей одноразовая и живёт один прогон: `tmpkeys_<yymmddHHMMSS>_<таблица>_<hex8>` — тот же
формат, что у промежуточных таблиц dbmerge (`tmp_...`), чтобы уборка разбирала оба вида одним
разбором имени. Метка времени по часам БД и первым элементом: Postgres времени создания таблиц не
хранит, а список таблиц схемы так сортируется по возрасту. На Postgres таблица UNLOGGED — ради
скорости и по той же причине, что и у dbmerge: она заведомо одноразовая.
"""

import uuid

from sqlalchemy import (Column, Index, MetaData, Table, and_, exists, func, insert, not_, or_,
                        select, update)
from sqlalchemy.engine import Engine

from cdc_1c.common_functions import DB_NOW_WITHOUT_TIMEZONE
from cdc_1c.logging_config import get_logger

logger = get_logger(__name__)

# Формат имени одноразовой таблицы: префикс без внутреннего подчёркивания, чтобы `tmp_...` и
# `tmpkeys_...` парсились одним разбором `<префикс>_<время>_<таблица>_<hex8>`.
KEYS_TABLE_PREFIX = 'tmpkeys_'
KEYS_TABLE_TIMESTAMP_FORMAT = '%y%m%d%H%M%S'
# Столько же, сколько у dbmerge: 63 байта лимита Postgres минус запас на суффикс индекса.
MAX_KEYS_TABLE_NAME_LEN = 58
UNIQUE_ID_LENGTH = 8


def _truncate_to_bytes(name: str, max_bytes: int) -> str:
    """Усечение по БАЙТАМ (Postgres считает идентификатор в них) с отбрасыванием оборванной
    многобайтовой последовательности — имя таблицы может быть и кириллическим."""
    encoded = name.encode('utf-8')
    if len(encoded) <= max_bytes:
        return name
    return encoded[:max_bytes].decode('utf-8', errors='ignore')


class FullLoadKeys:
    """
    Таблица ключей одного прогона полной выгрузки: заполняется постранично, в конце по ней
    помечаются пропавшие строки. Контекстный менеджер — таблица удаляется при выходе, в том числе
    при ошибке.

    Ключ здесь — первичный ключ целевой таблицы (имена колонок уже транслитерированы). Уникальность
    не объявляется: страницы, которые листаются через $skip, при параллельных вставках в 1С могут
    выдать одну запись дважды, и падать из-за этого прогону незачем. Индекс по ключу нужен для
    анти-join в mark_missing.
    """

    def __init__(self, engine: Engine, target_table_name: str, key_columns: dict,
                 schema: str | None = None):
        self.engine = engine
        self.schema = schema
        self.key_columns = key_columns
        self.table = Table(self._make_name(target_table_name), MetaData(),
                           *(Column(name, type_) for name, type_ in key_columns.items()),
                           schema=schema,
                           prefixes=['UNLOGGED'] if engine.dialect.name == 'postgresql' else [])
        self._index = Index(f'ix_{self.table.name}', *(self.table.c[c] for c in key_columns))

    @property
    def name(self) -> str:
        return self.table.name

    def _make_name(self, target_table_name: str) -> str:
        """`tmpkeys_<время>_<таблица>_<hex8>`; усекается только часть с именем таблицы, всё
        остальное остаётся целым и разбираемым."""
        with self.engine.connect() as conn:
            # Часы БД, а не процесса: имя сравнивают со строками в базе и с таблицами других
            # процессов, а общие часы у всех — только базы.
            now = conn.scalar(select(DB_NOW_WITHOUT_TIMEZONE))
        prefix = f'{KEYS_TABLE_PREFIX}{now.strftime(KEYS_TABLE_TIMESTAMP_FORMAT)}_'
        suffix = f'_{uuid.uuid4().hex[:UNIQUE_ID_LENGTH]}'
        budget = MAX_KEYS_TABLE_NAME_LEN - len(prefix) - len(suffix)
        return prefix + _truncate_to_bytes(target_table_name, budget) + suffix

    def __enter__(self) -> "FullLoadKeys":
        # Индекс объявлен на таблице, поэтому создаётся вместе с ней одним create().
        self.table.create(self.engine, checkfirst=False)
        logger.debug("Full load keys table %s created", self.table.name)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.drop()

    def drop(self) -> None:
        self.table.drop(self.engine, checkfirst=True)
        logger.debug("Full load keys table %s dropped", self.table.name)

    def add(self, rows: list[dict]) -> None:
        """Ключи одной страницы. Пусто — ничего не делаем: пустой INSERT SQLAlchemy не примет."""
        if not rows:
            return
        with self.engine.begin() as conn:
            conn.execute(insert(self.table), rows)

    def _not_seen(self, target: Table):
        """«Строки нет среди увиденных» — анти-join по ключу."""
        return not_(exists().where(and_(*(self.table.c[c] == target.c[c] for c in self.key_columns))))

    @staticmethod
    def _older_than_run(target: Table, started_at):
        """«Строку не переписывали с момента старта прогона» — тот же guard, что у самой выгрузки
        (DBWriter1C._not_touched_since), включая ветку NULL: строка из времён, когда колонки
        merged_on ещё не было, заведомо старше любого прогона. Без этой ветки такую строку не
        пометила бы никакая выгрузка — NULL не меньше и не больше отметки."""
        merged_on = target.c['merged_on']
        return or_(merged_on.is_(None), merged_on < started_at)

    def missing_rows(self, target: Table, started_at, mark_field: str,
                     scope=None) -> list[dict]:
        """
        Ключи строк-кандидатов на пометку: не встретились в прогоне, ещё не помечены и не переписаны
        после старта прогона (тот же guard по merged_on, что и у самой выгрузки, — строку, которую
        изменения переписали уже во время прогона, снимок трогать не вправе).

        scope — необязательное условие «строка входит в то, что прогон вообще читал». Нужно
        выгрузке за период: она видела только своё окно, и без такого ограничения кандидатом
        оказалась бы вся остальная таблица. См. Replicator1C._marking_scope.
        """
        query = (select(*(target.c[c] for c in self.key_columns))
                 .where(and_(*self._conditions(target, started_at, mark_field, scope))))
        with self.engine.connect() as conn:
            return [dict(row) for row in conn.execute(query).mappings()]

    def _conditions(self, target: Table, started_at, mark_field: str, scope) -> list:
        """Условия отбора кандидатов — одни и те же у missing_rows и mark_missing: разойдись они,
        перепроверенный список и то, что реально помечается, описывали бы разные множества."""
        conditions = [self._older_than_run(target, started_at),
                      not_(target.c[mark_field].is_(True)),
                      self._not_seen(target)]
        if scope is not None:
            conditions.append(scope)
        return conditions

    def mark_missing(self, target: Table, started_at, mark_field: str,
                     reset_values: dict | None = None, scope=None) -> int:
        """
        Помечает строки-кандидаты (см. missing_rows) и поднимает им merged_on.

        Именно пометка, а не удаление: обработчик замечает изменения только по merged_on, и
        физически удалённая строка не оставила бы витрине ни следа. Числовые ресурсы регистра
        гасятся в NULL (reset_values) — иначе «переехавшая» строка продолжила бы попадать в SUM.
        """
        values = {mark_field: True, 'merged_on': func.now(), **(reset_values or {})}
        statement = (update(target)
                     .where(and_(*self._conditions(target, started_at, mark_field, scope)))
                     .values(**values))
        with self.engine.begin() as conn:
            return conn.execute(statement).rowcount
