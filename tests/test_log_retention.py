"""
Хранение журнала загрузок (CDC-43).

Строка пишется на КАЖДЫЙ объект КАЖДОГО пакета: при опросе раз в минуту и двух десятках объектов
это миллион строк в месяц, а миллион строк — 95 МБ (измерено). Журнал нужен, чтобы разобрать
недавнее, а не чтобы хранить историю за годы, — поэтому у него есть срок хранения.

Уборка живёт там же, где журнал, и зовётся из записи в него: отдельного планировщика ради одного
DELETE в сутки заводить незачем, а цикл про уборку знать не обязан.
"""

import time
from datetime import timedelta

import pytest
from sqlalchemy import func, select, text

from onecdc.db_logs import (DEFAULT_LOG_RETENTION_DAYS, LOAD_TYPE_CHANGES, REPLICATOR_LOG,
                            ReplicatorLog)
from onecdc.replicator import _check_log_retention_days


def _age_row(log, log_id: int, days: float) -> None:
    """Отодвигает строку в прошлое — так выглядит запись, сделанная N суток назад."""
    with log.engine.begin() as conn:
        conn.execute(log.table.update().where(log.table.c.id == log_id)
                     .values(started_at=func.now() - timedelta(days=days)))


def _rows(log) -> int:
    with log.engine.connect() as conn:
        return conn.execute(select(func.count()).select_from(log.table)).scalar()


def test_old_rows_are_removed_and_recent_ones_are_kept(db):
    log = ReplicatorLog(db.engine, db.schema, retention_days=30)
    old = log.start('E', 'Catalog_X', 1, LOAD_TYPE_CHANGES)
    fresh = log.start('E', 'Catalog_Y', 2, LOAD_TYPE_CHANGES)
    _age_row(log, old, days=31)
    _age_row(log, fresh, days=29)

    assert log.cleanup() == 1

    with db.engine.connect() as conn:
        left = conn.execute(select(log.table.c.id)).scalars().all()
    assert left == [fresh]


def test_zero_means_keep_everything(db):
    """Выключатель нужен: кому-то журнал — источник отчётности, и удалять там нечего."""
    log = ReplicatorLog(db.engine, db.schema, retention_days=0)
    ancient = log.start('E', 'Catalog_X', 1, LOAD_TYPE_CHANGES)
    _age_row(log, ancient, days=1000)

    log.cleanup_if_due()

    assert _rows(log) == 1


def test_cleanup_happens_once_a_day_not_on_every_write(db):
    """
    start() зовут на каждый объект каждого пакета — щупать таблицу столько же раз незачем.
    Проверка стоит сравнения в памяти, а до БД дело доходит раз в сутки.
    """
    log = ReplicatorLog(db.engine, db.schema, retention_days=30)
    cleanups = []
    log.cleanup = lambda: cleanups.append(1) or 0

    for _ in range(5):
        log.cleanup_if_due()
    assert cleanups == [], 'конструктор уже убрался, следующая уборка — через сутки'

    log._next_cleanup_at = time.monotonic() - 1        # сутки прошли
    log.cleanup_if_due()
    log.cleanup_if_due()
    assert len(cleanups) == 1, 'после уборки отметка должна отодвинуться снова'


def test_a_failed_cleanup_does_not_break_the_load(db, caplog):
    """
    Уборка — дело служебное: ни её отказ, ни отсутствие прав не имеют права уронить загрузку.
    И повторяться на каждой записи журнала она тоже не должна.
    """
    import logging
    from sqlalchemy.exc import OperationalError

    log = ReplicatorLog(db.engine, db.schema, retention_days=30)

    def boom():
        raise OperationalError('DELETE', {}, Exception('нет прав'))
    log.cleanup = boom
    log._next_cleanup_at = 0.0

    with caplog.at_level(logging.WARNING, logger='onecdc.db_logs'):
        assert log.cleanup_if_due() == 0
    assert 'Could not clean up' in '\n'.join(r.getMessage() for r in caplog.records)

    # И запись в журнал после этого работает.
    assert log.start('E', 'Catalog_X', 1, LOAD_TYPE_CHANGES)


def test_the_log_is_indexed_by_time_and_by_unfinished(db):
    """
    Без индекса «что грузилось ночью» и «что идёт сейчас» — полный скан миллионов строк. Второй
    индекс частичный: незавершённых строк в норме единицы.
    """
    ReplicatorLog(db.engine, db.schema)

    with db.engine.connect() as conn:
        indexes = conn.execute(text("""
            SELECT indexdef FROM pg_indexes WHERE schemaname = :s AND tablename = :t"""),
            {'s': db.schema, 't': REPLICATOR_LOG}).scalars().all()

    definitions = '\n'.join(indexes)
    assert 'started_at' in definitions
    assert 'finished_at IS NULL' in definitions, 'частичный индекс под мониторинг'


def test_the_batch_loop_removes_everything(db):
    """
    Удаляем партиями (целиком такой DELETE держал бы длинную транзакцию и раздувал таблицу),
    поэтому цикл обязан доходить до конца — иначе хвост копился бы вечно.
    """
    from onecdc import db_logs

    log = ReplicatorLog(db.engine, db.schema, retention_days=1)
    with db.engine.begin() as conn:
        conn.execute(text(f'''
            INSERT INTO "{db.schema}"."{REPLICATOR_LOG}" (exchange, object, type, started_at)
            SELECT 'E', 'Catalog_X', 'changes', now() - interval '10 days'
              FROM generate_series(1, 25)'''))

    monkeyed = db_logs._LOG_CLEANUP_BATCH
    try:
        db_logs._LOG_CLEANUP_BATCH = 10               # три партии и остаток
        assert log.cleanup() == 25
    finally:
        db_logs._LOG_CLEANUP_BATCH = monkeyed
    assert _rows(log) == 0


@pytest.mark.parametrize("value", [-1, 1.5, "30", True, None])
def test_a_nonsensical_retention_is_refused(value):
    # Отрицательное молча снесло бы журнал целиком, дробное выглядит как попытка задать часы.
    with pytest.raises(ValueError):
        _check_log_retention_days(value)


def test_the_default_is_a_month():
    assert _check_log_retention_days(DEFAULT_LOG_RETENTION_DAYS) == 30
