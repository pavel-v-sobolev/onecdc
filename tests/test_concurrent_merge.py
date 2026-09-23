"""
Одну таблицу пишут одновременно: страница полной выгрузки и пакет изменений — разные потоки, а то
и разные процессы.

Фаза вставки dbmerge ищет недостающие строки анти-джойном (`WHERE target.pk IS NULL`), и при
READ COMMITTED обе конкурентные вставки одного ключа через него проходят — проигравшая упирается
в первичный ключ. Измерено до починки: два потока на один ключ сталкивались в 17 кругах из 40
(CDC-39).

Цена несимметрична, и ради этого всё и делалось: пакет изменений повторится сам, а страница валит
ВЕСЬ прогон полной выгрузки, который потом ждёт до получаса.
"""

import threading
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from onecdc.data_reader import DataObject
from onecdc.db_writer import DBWriter, _is_unique_violation
from onecdc.metadata_reader import MetadataObject
from onecdc.name_mapper import NameMapper

OBJECT = "Catalog_X"
KEY = "11111111-1111-1111-1111-111111111111"


METADATA = MetadataObject(OBJECT, {"Ref_Key": "Guid", "Val": "String"}, {"Ref_Key": "Guid"})


def _writer(db) -> DBWriter:
    return DBWriter(db.engine, NameMapper(), schema=db.schema)


def _data(value: str, key: str = KEY) -> DataObject:
    # Метаданные живут в самом DataObject: оттуда save берёт первичный ключ и типы.
    return DataObject(METADATA, records=[{"Ref_Key": key, "Val": value}])


def _rows(db) -> int:
    with db.engine.connect() as conn:
        return conn.execute(text(f'SELECT count(*) FROM "{db.schema}"."Catalog_X"')).scalar()


def test_two_threads_writing_the_same_row_both_succeed(db):
    """
    Главное свойство: столкновение не должно доходить до вызывающего. Раньше проигравший поток
    получал UniqueViolation, и для полной выгрузки это означало потерю всего прогона.
    """
    writer = _writer(db)
    writer.save(OBJECT, _data("первый"))          # заводим таблицу
    failures = []

    for _ in range(15):
        with db.engine.begin() as conn:
            conn.execute(text(f'DELETE FROM "{db.schema}"."Catalog_X"'))   # строки снова нет

        def save(marker):
            try:
                _writer(db).save(OBJECT, _data(marker))
            except Exception as error:                     # noqa: BLE001 — записываем любой
                failures.append(f'{type(error).__name__}: {error}')

        threads = [threading.Thread(target=save, args=(f'поток{n}',)) for n in (1, 2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    assert failures == [], f'столкновения дошли до вызывающего: {failures[:2]}'
    assert _rows(db) == 1


def test_a_duplicate_key_inside_one_batch_still_fails_loudly(db):
    """
    Ради этого повтор и выбран вместо ON CONFLICT DO NOTHING: дубль ключа ВНУТРИ пачки — это
    ошибка вычисления ключа (так вскрывались CDC-13 и CDC-14), и она обязана падать. Повтор
    разделяет случаи сам: гонка на второй попытке исчезает, а наш дубль воспроизводится.
    """
    writer = _writer(db)

    with pytest.raises(IntegrityError):
        writer.save(OBJECT, DataObject(METADATA, records=[{"Ref_Key": KEY, "Val": "a"},
                                                          {"Ref_Key": KEY, "Val": "b"}]))


def test_only_a_unique_violation_is_retried(db, caplog):
    """
    Прочие IntegrityError постоянны (NOT NULL, внешний ключ): повторять их — трижды сделать ту же
    работу впустую и на столько же отложить понятную ошибку.
    """
    import logging

    writer = _writer(db)
    attempts = []

    def not_null_violation():
        attempts.append(1)
        with db.engine.begin() as conn:
            conn.execute(text(f'CREATE TABLE "{db.schema}"."NN" (id int NOT NULL)'
                              if not attempts[1:] else 'SELECT 1'))
            conn.execute(text(f'INSERT INTO "{db.schema}"."NN" VALUES (NULL)'))

    with caplog.at_level(logging.WARNING, logger='onecdc.db_writer'):
        with pytest.raises(IntegrityError):
            writer._merge_with_retry(not_null_violation, 'NN')

    assert len(attempts) == 1, 'постоянную ошибку повторять незачем'
    assert caplog.records == []


def test_a_unique_violation_is_recognised_by_its_sqlstate(db):
    """Код берём у драйвера, а не разбираем текст: он локализуется и меняется между версиями."""
    writer = _writer(db)
    writer.save(OBJECT, _data("первый"))

    with db.engine.begin() as conn:
        conn.execute(text(f'INSERT INTO "{db.schema}"."Catalog_X" ("Ref_Key") VALUES (:k)'),
                     {'k': str(uuid.uuid4())})
        try:
            conn.execute(text(f'INSERT INTO "{db.schema}"."Catalog_X" ("Ref_Key") VALUES (:k)'),
                         {'k': KEY})
        except IntegrityError as error:
            assert _is_unique_violation(error)
        else:
            pytest.fail('дубля не случилось — тест ничего не проверяет')
