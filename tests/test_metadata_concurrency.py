"""
Словарь метаданных читают чужие потоки, и менять его под ними нельзя.

Фоновая полная выгрузка перебирает его на КАЖДОЙ странице (`Replicator._full_load_tables` — так
берётся отметка страницы по реестру незавершённых merge), а поток изменений перечитывает метаданные,
встретив незнакомое поле. Блокировку читатели не берут и брать не должны: это горячий путь.

Разбор `$metadata` идёт по объектам конфигурации в цикле, и пока он писал их в живой словарь по
одному (`self[item_name] = ...`, полторы-две тысячи присваиваний подряд), совпадение давало
«RuntimeError: dictionary changed size during iteration»: полная выгрузка обрывалась посреди
прогона, а на крупном объекте это часы работы впустую. Воспроизводилось у всех четырёх читающих
потоков из четырёх.

Теперь словарь собирается отдельно и публикуется подменой ссылки — неделимой операцией.
"""

import threading
import time

from onecdc.metadata_reader import MetadataObject, MetadataReader


def _objects(first: int, last: int) -> dict:
    return {f'Catalog_{i}': MetadataObject(f'Catalog_{i}', {"Ref_Key": "Guid"}, {"Ref_Key": "Guid"})
            for i in range(first, last)}


def test_a_refresh_replaces_the_dictionary_rather_than_merging():
    """
    Проверяемое следствие подмены: объект, пропавший из `$metadata`, уходит и из памяти.

    Раньше словарь только пополнялся, и пропавший жил до перезапуска. Это было безопасно ровно
    потому, что реестр синхронизировался УДАЛЕНИЕМ строк: пропавший объект оставался в списке
    синхронизации, и его состояние (захват, история) не сносило. Теперь реестр помечает, а не
    удаляет, — и держать пропавшего в памяти больше незачем.
    """
    md = MetadataReader(odata_url="http://fake")
    md.data = {'Catalog_Старый': MetadataObject('Catalog_Старый', {"Ref_Key": "Guid"},
                                                {"Ref_Key": "Guid"})}
    md._fetch_and_parse_metadata = lambda: _objects(0, 3)

    md.get_metadata()

    assert 'Catalog_Старый' not in md, 'пропавший объект остался в памяти до перезапуска'
    assert 'Catalog_1' in md


def test_a_reader_never_sees_a_half_built_dictionary():
    """
    Читатель видит либо старый словарь целиком, либо новый целиком — промежуточного состояния нет.

    Разбор здесь намеренно медленный: иначе он проскочил бы между двумя чтениями, и тест ничего
    бы не проверял.
    """
    md = MetadataReader(odata_url="http://fake")
    md.data = _objects(0, 10)

    def slow_parse():
        time.sleep(0.2)
        return _objects(100, 120)

    md._fetch_and_parse_metadata = slow_parse

    sizes: set[int] = set()
    errors: list[str] = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            try:
                names = [name for name in md]
            except Exception as error:
                errors.append(f'{type(error).__name__}: {error}')
                return
            sizes.add(len(names))

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for thread in threads:
        thread.start()
    try:
        md.get_metadata()
    finally:
        stop.set()
        for thread in threads:
            thread.join()

    assert errors == [], 'чтение словаря упало под перечитыванием метаданных'
    assert sizes <= {10, 20}, f'читатель увидел словарь на середине сборки: {sorted(sizes)}'


def test_a_failed_refresh_leaves_the_previous_metadata_intact():
    """
    Сборка отдельным словарём нужна и для этого: неудача на середине (сеть моргнула, ответ неполон)
    не оставляет метаданные наполовину разобранными — публиковать нечего, работает старое.
    """
    md = MetadataReader(odata_url="http://fake")
    md.data = _objects(0, 10)
    md.is_loaded = True

    def boom():
        raise ConnectionError('1С недоступна')

    md._fetch_and_parse_metadata = boom
    try:
        md.get_metadata()
    except ConnectionError:
        pass

    assert len(md) == 10, 'старые метаданные потеряны'
    assert 'Catalog_5' in md


def test_is_loaded_is_set_only_after_a_successful_sync(db):
    # Иначе следующий цикл считает метаданные загруженными и работает с objects_table = None.
    md = MetadataReader(odata_url="http://fake", engine=db.engine, schema=db.schema)
    md._fetch_and_parse_metadata = lambda: _objects(0, 3)

    def boom(_names):
        raise RuntimeError('БД недоступна')

    md._sync_objects = boom
    try:
        md.get_metadata()
    except RuntimeError:
        pass

    assert md.is_loaded is False, 'метаданные числятся загруженными, хотя реестр не синхронизирован'
