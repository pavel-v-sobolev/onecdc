"""
Имена таблиц и колонок: транслит, лимит длины и арбитраж коллизий.

Транслитерация не инъективна и такой быть не может: `е`/`э` дают `e`, `ъ`/`ь` исчезают,
кириллическое имя способно совпасть с латинским. Раньше это означало, что два объекта 1С молча
ложились в одну таблицу, а два поля — в одну колонку (последнее значение побеждало). Теперь имя
не вычисляется, а закрепляется в onecdc_name_claims, и арбитром служит уникальный индекс.

Отдельно проверяется совместимость: транслит остаётся первым кандидатом, поэтому существующие
установки заявляют ровно те имена, что у них уже есть, и ничего не переименовывается.
"""

import threading

import pytest
from sqlalchemy import select

from onecdc.name_mapper import (CLAIMS_TABLE, POSTGRES_MAX_IDENTIFIER, SCOPE_FIELD, SCOPE_OBJECT,
                                NameMapper, fit_identifier_length)


def _mapper(db) -> NameMapper:
    return NameMapper(db.engine, db.schema)


def _claims(mapper) -> list[tuple[str, str, str]]:
    with mapper.engine.connect() as conn:
        rows = conn.execute(select(mapper.table)).mappings()
        return sorted((r['scope'], r['source_name'], r['identifier']) for r in rows)


# --- Транслит и совместимость с уже выданными именами ---

def test_translit_keeps_the_object_type_prefix():
    assert NameMapper.offline().map_object_name('Document_ЗаказКлиента') == 'Document_ZakazKlienta'


def test_offline_mapper_reproduces_the_plain_translit():
    # Ровно то, что выдавали прошлые версии: при переходе на реестр заявок первый кандидат
    # совпадает с уже существующим именем таблицы, и переименований не происходит.
    m = NameMapper.offline()
    assert m.map_field_name('Наименование') == 'Naimenovanie'
    assert m.map_object_name('Catalog_Номенклатура') == 'Catalog_Nomenklatura'


def test_length_limit_is_counted_in_bytes_not_characters():
    # Postgres считает идентификатор в БАЙТАХ и лишнее обрезает молча. Буквы вне таблицы
    # транслита (украинские і ї є, белорусская ў) остаются двухбайтовыми: 63 символа — 113 байт,
    # и два разных имени схлопывались бы в одну таблицу незаметно.
    name = 'Catalog_' + 'і' * 60
    mapped = NameMapper.offline().map_object_name(name)
    assert len(mapped.encode('utf-8')) <= POSTGRES_MAX_IDENTIFIER


def test_ascii_name_at_the_limit_is_untouched():
    name = 'C' * POSTGRES_MAX_IDENTIFIER
    assert fit_identifier_length(name) == name


# --- Коллизии: разные имена 1С не должны делить идентификатор ---

COLLIDING = [
    ('Catalog_Сер', 'Catalog_Сэр'),          # е и э дают одну букву
    ('Catalog_Объект', 'Catalog_Обьект'),    # ъ и ь исчезают оба
    ('Catalog_Ёлка', 'Catalog_Yolka'),       # кириллица совпала с латиницей
]


@pytest.mark.parametrize("first, second", COLLIDING)
def test_translit_alone_is_not_injective(first, second):
    # Исходный дефект: без арбитра эти пары дают ОДНО имя, и данные двух объектов 1С ложатся
    # в одну таблицу. Транслит починить нельзя — можно только не доверять ему в одиночку.
    m = NameMapper.offline()
    assert m.map_object_name(first) == m.map_object_name(second)


@pytest.mark.parametrize("first, second", COLLIDING)
def test_colliding_object_names_get_different_tables(db, first, second):
    m = _mapper(db)

    one, two = m.map_object_name(first), m.map_object_name(second)

    assert one != two, 'данные двух объектов 1С легли бы в одну таблицу'
    # Первый пришедший сохраняет чистое имя — переименовывать уже работающую таблицу нельзя.
    assert one == NameMapper.offline().map_object_name(first)
    assert len(two.encode('utf-8')) <= POSTGRES_MAX_IDENTIFIER


def test_colliding_field_names_get_different_columns(db):
    m = _mapper(db)

    assert m.map_field_name('Объект') != m.map_field_name('Обьект')


def test_a_1c_field_cannot_take_a_service_column(db):
    # Служебные колонки ведут dbmerge и парсер. Поле 1С, чей транслит совпал со служебным именем,
    # должно уйти в сторону — иначе оно писало бы в чужую колонку.
    m = _mapper(db)

    # 'м' здесь кириллическая: транслит даёт ровно 'merged_on'.
    mapped = m.map_field_name('мerged_on')

    assert mapped != 'merged_on'
    assert m.map_field_name('merged_on') == 'merged_on', 'своё служебное поле переименовывать нельзя'


# --- Закрепление: имя выдаётся один раз и навсегда ---

def test_the_same_name_always_resolves_to_the_same_identifier(db):
    first = _mapper(db).map_object_name('Catalog_Номенклатура')
    # Другой процесс, свой кэш — читает закреплённое, а не пересчитывает.
    assert _mapper(db).map_object_name('Catalog_Номенклатура') == first


def test_a_freed_name_is_never_reused(db):
    # Объект переименовали в 1С: для нас это новый объект, а старый исчез. Его таблица остаётся
    # в схеме с его данными, поэтому имя за ним и закреплено — иначе новый объект сел бы в чужую
    # таблицу поверх чужих строк.
    m = _mapper(db)
    old = m.map_object_name('Catalog_Сер')

    fresh = _mapper(db)     # «следующий запуск», про старый объект в метаданных уже нет ни слова
    assert fresh.map_object_name('Catalog_Сэр') != old


def test_claims_are_recorded_for_both_namespaces(db):
    m = _mapper(db)
    m.map_object_name('Catalog_Номенклатура')
    m.map_field_name('Наименование')

    claims = _claims(m)
    assert (SCOPE_OBJECT, 'Catalog_Номенклатура', 'Catalog_Nomenklatura') in claims
    assert (SCOPE_FIELD, 'Наименование', 'Naimenovanie') in claims
    # Имя таблицы и имя колонки живут в разных пространствах и друг с другом не конфликтуют.
    assert CLAIMS_TABLE in str(m.table)


# --- Гонка: арбитром выступает уникальный индекс, а не порядок вызовов ---

def test_concurrent_mappers_agree_on_one_object(db):
    # Два процесса регистрируют ОДИН объект: оба считают одного кандидата и вставляют одинаковую
    # строку. Выигрывает один, проигравший перечитывает и получает тот же ответ.
    mappers = [_mapper(db) for _ in range(8)]
    barrier = threading.Barrier(len(mappers))
    results = {}

    def run(i, m):
        barrier.wait()
        results[i] = m.map_object_name('Catalog_Номенклатура')

    threads = [threading.Thread(target=run, args=(i, m)) for i, m in enumerate(mappers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(set(results.values())) == 1, f'процессы разошлись в имени таблицы: {results}'
    assert len(_claims(mappers[0])) == 1 + 4    # объект плюс четыре служебные колонки


def test_concurrent_mappers_split_two_colliding_objects(db):
    # Два процесса одновременно регистрируют РАЗНЫЕ имена с одинаковым транслитом. Кому достанется
    # чистое имя — решает гонка, но достаться оно обязано ровно одному.
    names = ['Catalog_Сер', 'Catalog_Сэр']
    barrier = threading.Barrier(len(names))
    results = {}

    def run(name):
        m = _mapper(db)
        barrier.wait()
        results[name] = m.map_object_name(name)

    threads = [threading.Thread(target=run, args=(n,)) for n in names]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results[names[0]] != results[names[1]]
    assert min(results.values()) == "Catalog_Ser"   # чистое имя досталось одному из них

    # И решение зафиксировано: следующий процесс читает то же самое.
    fresh = _mapper(db)
    assert {n: fresh.map_object_name(n) for n in names} == results


# --- Пакетное закрепление ---

def test_prefetch_does_not_change_what_names_are_issued(db):
    # prefetch — только ускорение первого запуска. Результат обязан совпадать с тем, что выдал бы
    # поштучный путь, включая коллизии: спорные имена он не трогает и оставляет обычному перебору.
    names = ['Catalog_Номенклатура', 'Catalog_Сер', 'Catalog_Сэр', 'Catalog_Ёлка', 'Catalog_Yolka']

    without = _mapper(db)
    expected = {n: without.map_object_name(n) for n in names}

    # Та же схема с нуля не выйдет (заявки уже закреплены), поэтому проверяем обратное: после
    # prefetch бесспорные имена те же, а спорные по-прежнему разведены.
    fresh = _mapper(db)
    fresh.prefetch(SCOPE_OBJECT, names)
    assert {n: fresh.map_object_name(n) for n in names} == expected


def test_prefetch_is_a_no_op_on_an_offline_mapper():
    m = NameMapper.offline()
    m.prefetch(SCOPE_OBJECT, ['Catalog_Номенклатура'])
    assert m.map_object_name('Catalog_Номенклатура') == 'Catalog_Nomenklatura'
