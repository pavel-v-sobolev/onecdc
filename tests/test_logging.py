"""
Пометки режима загрузки в логе (CHANGES / FULL RELOAD), человекочитаемый размер ответа и
отсутствие построчного спама при разборе страницы.
"""

import logging
import threading

import pytest

from onecdc.common_functions import format_bytes, format_duration
from onecdc.logging_config import (LOAD_MODE_CHANGES, LOAD_MODE_FULL, LOAD_MODE_METADATA,
                                   NOISY_LOGGERS, _ensure_handler, get_logger, load_mode,
                                   log_prefix)
from onecdc.logging_config import logger as package_logger


@pytest.mark.parametrize("size, expected", [
    (0, "0 B"),
    (512, "512 B"),
    (1024, "1.0 KB"),
    (1536, "1.5 KB"),
    (10 * 1024 * 1024 + 200 * 1024, "10.2 MB"),
    (3 * 1024 ** 3, "3.0 GB"),
])
def test_format_bytes(size, expected):
    assert format_bytes(size) == expected


@pytest.mark.parametrize("seconds, expected", [
    (0, "0.0s"),
    (4.14, "4.1s"),
    (59.9, "59.9s"),
    (60, "1m 00s"),
    (64.3, "1m 04s"),
    (3600, "1h 00m 00s"),
    (3903, "1h 05m 03s"),
])
def test_format_duration(seconds, expected):
    assert format_duration(seconds) == expected


def test_log_prefix_only_inside_load_mode():
    assert log_prefix() == ''
    with load_mode(LOAD_MODE_CHANGES):
        assert log_prefix() == '[CHANGES] '
        with load_mode(LOAD_MODE_FULL):     # вложенный режим перекрывает
            assert log_prefix() == '[FULL RELOAD] '
        assert log_prefix() == '[CHANGES] '
    assert log_prefix() == ''               # режим снимается на выходе из блока


def test_adapter_prefixes_message(caplog):
    logger = get_logger('onecdc.test')
    with caplog.at_level(logging.INFO, logger='onecdc.test'):
        logger.info('Saving %s records', 5)
        with load_mode(LOAD_MODE_FULL):
            logger.info('Saving %s records', 5)

    assert caplog.messages == ['Saving 5 records', '[FULL RELOAD] Saving 5 records']


def test_metadata_read_is_not_tagged_as_changes(monkeypatch):
    # Чтение $metadata общее для изменений и полной выгрузки — метка у него своя, а не вызвавшей
    # операции: раньше внутри run_once оно уезжало в лог как [CHANGES].
    from onecdc.metadata_reader import MetadataReader

    seen = []
    md = MetadataReader('http://x')
    monkeypatch.setattr(MetadataReader, '_fetch_and_parse_metadata',
                        lambda self: seen.append(log_prefix()))

    with load_mode(LOAD_MODE_CHANGES):
        md.get_metadata()

    assert seen == ['[METADATA] ']
    assert LOAD_MODE_METADATA == 'METADATA'


def _prepare_ensure_handler(monkeypatch, logging_configured: bool):
    """Готовит окружение для _ensure_handler: настроено ли логирование снаружи и чистые уровни.
    hasHandlers подменяем целиком — он смотрит вверх по цепочке, а под pytest на root уже висит
    свой хендлер, и одной очисткой logger.handlers сценарий «не настроено» не воспроизвести."""
    monkeypatch.setattr(package_logger, 'hasHandlers', lambda: logging_configured)
    monkeypatch.setattr(package_logger, 'addHandler', lambda handler: None)
    for name in NOISY_LOGGERS:
        target = logging.getLogger(name)
        monkeypatch.setattr(target, 'level', logging.INFO)


def test_ensure_handler_quiets_noisy_loggers(monkeypatch):
    # При автонастройке логирования чужие шумные логгеры (alembic из dbmerge) уводим на WARNING.
    # Сам dbmerge не трогаем — его строки про merge нужны.
    _prepare_ensure_handler(monkeypatch, logging_configured=False)

    _ensure_handler()

    assert all(logging.getLogger(name).level == logging.WARNING for name in NOISY_LOGGERS)
    assert 'dbmerge' not in NOISY_LOGGERS


def test_ensure_handler_keeps_app_logging_untouched(monkeypatch):
    # Если логирование настроило приложение — не трогаем ни его, ни уровни чужих логгеров.
    _prepare_ensure_handler(monkeypatch, logging_configured=True)

    _ensure_handler()

    assert all(logging.getLogger(name).level == logging.INFO for name in NOISY_LOGGERS)


def test_load_mode_does_not_leak_between_threads():
    # Полная выгрузка идёт фоновым потоком параллельно с чтением изменений: режим одного потока
    # не должен просачиваться в другой (ради этого ContextVar, а не глобальная переменная).
    seen = {}

    def worker():
        seen['thread_before'] = log_prefix()
        with load_mode(LOAD_MODE_FULL):
            seen['thread_inside'] = log_prefix()

    with load_mode(LOAD_MODE_CHANGES):
        thread = threading.Thread(target=worker)
        thread.start()
        thread.join()
        seen['main'] = log_prefix()

    assert seen == {'thread_before': '', 'thread_inside': '[FULL RELOAD] ',
                    'main': '[CHANGES] '}
