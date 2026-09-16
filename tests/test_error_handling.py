"""
Ошибки HTTP и ретрай цикла: тело ответа 1С попадает в ошибку/лог (raise_for_status),
а упавший run_forever ретраится с экспоненциальной паузой (см. replicator.BACKOFF_FACTOR).
"""

import json
import logging

import pytest
import requests

from onecdc import Replicator
from onecdc.common_functions import MAX_ERROR_BODY_CHARS, extract_error_text, raise_for_status
from onecdc.replicator import _log_failure
from onecdc.replicator import DEFAULT_MAX_BACKOFF
from conftest import FakeResponseMixin, TEST_QUEUE_GUID


class _Resp(FakeResponseMixin):
    def __init__(self, status_code=500, text='', reason='Internal Server Error'):
        self.ok = 200 <= status_code < 300
        self.status_code = status_code
        self.text = text
        self.reason = reason
        self.url = 'http://1c/odata/SelectChanges'


def test_raise_for_status_keeps_body_and_context(db):
    # Содержательное у 1С только в теле — оно должно быть и в тексте ошибки, вместе с контекстом.
    resp = _Resp(text='Ошибка блокировки данных при попытке чтения')

    with pytest.raises(requests.HTTPError) as excinfo:
        raise_for_status(resp, 'SelectChanges (message 42)')

    message = str(excinfo.value)
    assert 'Ошибка блокировки данных' in message
    assert 'SelectChanges (message 42)' in message
    assert '500' in message
    assert excinfo.value.response is resp


def test_raise_for_status_truncates_long_body(db):
    with pytest.raises(requests.HTTPError) as excinfo:
        raise_for_status(_Resp(text='x' * (MAX_ERROR_BODY_CHARS + 500)), 'ctx')

    assert '[+500 chars]' in str(excinfo.value)


def test_extract_error_text_odata_xml(db):
    body = ('<m:error xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">'
            '<m:code>-1</m:code><m:message>{(3, 23)}: Неверные параметры в операции\n'
            ' сравнения.</m:message></m:error>')

    assert extract_error_text(body) == '{(3, 23)}: Неверные параметры в операции сравнения.'


def test_extract_error_text_application_exception_json(db):
    # Реальный ответ 1С при запрете входа: полезен только descr, а рядом лежат creationStack
    # с адресами в DLL и base64-дамп — в лог они не нужны.
    body = '﻿' + json.dumps({
        "#exception": "{http://v8.1c.ru/8.2/virtual-resource-system}Exception",
        "exception": {
            "reason": 403,
            "descr": "HTTP: Forbidden\nОшибка при выполнении запроса GET к ресурсу /odata/x:",
            "inner": {
                "descr": "Начало сеанса с информационной базой запрещено.\n"
                         "Ведутся технические работы",
                "inner": {
                    "descr": "Ведутся технические работы",
                    "creationStack": "core83.dll:0x0000000000085928 " * 40,
                    "data": "77u/ew0Ke2EwMWY0NjVjLWVkNzAtNDQyZS1hZGE1" * 30,
                },
            },
        },
    }, ensure_ascii=False)

    text = extract_error_text(body)

    assert text == 'Начало сеанса с информационной базой запрещено. Ведутся технические работы'
    # внешняя HTTP-обёртка отброшена: код и ресурс и так есть в нашем сообщении
    assert 'HTTP: Forbidden' not in text
    assert 'creationStack' not in text and 'core83.dll' not in text
    assert '77u/ew0K' not in text


def test_extract_error_text_keeps_http_wrapper_when_alone(db):
    # Если кроме обёртки ничего нет — оставляем её, лучше так, чем пустая ошибка.
    body = json.dumps({"exception": {"descr": "HTTP: Forbidden\nОшибка запроса"}}, ensure_ascii=False)

    assert extract_error_text(body) == 'HTTP: Forbidden Ошибка запроса'


def test_extract_error_text_platform_html(db):
    body = ('<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN">\n<html><head>'
            '<title>1C:Enterprise 8 application error</title></head><body>'
            '<h2>1C:Enterprise 8 application error:</h2>Unrecoverable error<br>'
            '<b>by reason: </b><br>The device is full &apos;/tmp/v8_x.tmp&apos;. '
            '28(0x0000001C): No space left on device</body></html>')

    text = extract_error_text(body)

    assert 'No space left on device' in text
    assert '<' not in text and '\n' not in text


def test_extract_error_text_unknown_format_falls_back_to_body(db):
    assert extract_error_text('  просто текст  ') == 'просто текст'
    assert extract_error_text('') == '<empty body>'


def test_log_failure_http_error_without_traceback_and_text(db, caplog):
    # Описание от 1С уже вывел raise_for_status строкой выше — второй раз не повторяем,
    # и traceback (внутренности requests) не тащим.
    exc = requests.HTTPError('1C request failed: 403 Forbidden: Ведутся технические работы')
    with caplog.at_level(logging.ERROR, logger='onecdc.replicator'):
        _log_failure(exc, "Replication cycle failed, retry in %ss", 1800.0)

    record = caplog.records[-1]
    assert record.getMessage() == '[CHANGES] Replication cycle failed, retry in 1800.0s' \
        or record.getMessage() == 'Replication cycle failed, retry in 1800.0s'
    assert record.exc_info is None
    assert 'Ведутся технические работы' not in record.getMessage()


def test_log_failure_connection_error_keeps_text(db, caplog):
    # Таймаут/обрыв нигде не логируется до этого — текст нужен, traceback по-прежнему нет.
    with caplog.at_level(logging.ERROR, logger='onecdc.replicator'):
        _log_failure(requests.ConnectTimeout('connect timed out'), "Cycle failed")

    record = caplog.records[-1]
    assert 'connect timed out' in record.getMessage()
    assert record.exc_info is None


def test_log_failure_keeps_traceback_for_code_errors(db, caplog):
    # Не ошибка обмена — похоже на баг в коде, traceback оставляем.
    with caplog.at_level(logging.ERROR, logger='onecdc.replicator'):
        try:
            raise ValueError('boom')
        except ValueError as exc:
            _log_failure(exc, "Cycle failed")

    assert caplog.records[-1].exc_info is not None


def test_raise_for_status_passes_ok_response(db):
    assert raise_for_status(_Resp(status_code=200, text='ok'), 'ctx') is None


def _replicator(db):
    return Replicator(
        odata_url='http://1c/odata',
        odata_auth=('u', 'p'),
        exchange_name='X',
        queue_guid=TEST_QUEUE_GUID,
        engine=db.engine,
        db_schema=db.schema,
    )


def _run_and_collect_delays(db, monkeypatch, error: Exception, waits: int, interval: float = 60.0):
    """Гоняет run_forever с падающим run_once, возвращая паузы, которые он запросил.
    Последняя итерация обрывается по max_iterations до паузы, поэтому итераций на одну больше."""
    repl = _replicator(db)
    delays = []

    def fake_run_once(*args, **kwargs):
        raise error

    monkeypatch.setattr(repl, 'run_once', fake_run_once)
    monkeypatch.setattr('onecdc.stop_signal.StopSignal.wait', lambda self, d: delays.append(d))
    repl.run_forever(interval=interval, max_iterations=waits + 1)
    return delays


def test_transient_failures_back_off_exponentially(db, monkeypatch):
    # Слепой повтор каждые interval секунд накладывает попытки друг на друга — пауза растёт.
    delays = _run_and_collect_delays(db, monkeypatch, requests.ConnectionError('1C is down'), 3)

    assert delays == [120.0, 240.0, 480.0]


def test_backoff_is_capped(db, monkeypatch):
    delays = _run_and_collect_delays(db, monkeypatch, requests.ConnectionError('1C is down'), 12)

    assert max(delays) == DEFAULT_MAX_BACKOFF
    assert delays[-1] == DEFAULT_MAX_BACKOFF


def test_permanent_error_goes_straight_to_max_backoff(db, monkeypatch):
    # 403 не лечится повтором: сразу потолок, а не 15 попыток по мере роста паузы.
    error = requests.HTTPError('forbidden', response=_Resp(status_code=403, reason='Forbidden'))
    delays = _run_and_collect_delays(db, monkeypatch, error, 2)

    assert delays == [DEFAULT_MAX_BACKOFF, DEFAULT_MAX_BACKOFF]


def test_backoff_resets_after_success(db, monkeypatch):
    repl = _replicator(db)
    delays = []
    calls = {'n': 0}

    def flaky_run_once(*args, **kwargs):
        calls['n'] += 1
        if calls['n'] in (1, 2):
            raise requests.ConnectionError('1C is down')

    monkeypatch.setattr(repl, 'run_once', flaky_run_once)
    monkeypatch.setattr(repl, '_dispatch_full_loads', lambda executor: None)
    monkeypatch.setattr('onecdc.stop_signal.StopSignal.wait', lambda self, d: delays.append(d))
    repl.run_forever(interval=60.0, max_iterations=4)

    assert delays == [120.0, 240.0, 60.0]

