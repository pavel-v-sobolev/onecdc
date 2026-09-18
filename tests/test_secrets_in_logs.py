"""
Что из конфигурации и данных попадает в журналы и в служебную таблицу.

Утечка здесь не про «злоумышленника»: конфигурацию задаёт оператор, а данные и так лежат в БД.
Дело в том, что у журналов и у таблиц доступ РАЗНЫЙ. Лог контейнера стекается в общий сборщик,
а onecdc_handlers читает любой, у кого есть SELECT на схему, — то есть все потребители витрины.
Пароль и значения полей уезжают таким образом туда, где их никто не охранял (CDC-29).

Плюс сюда же CDC-30: Basic-авторизация по открытому HTTP. Это не запрет — публикация 1С по http
во внутренней сети обычное дело, — а одно предупреждение при старте.
"""

import base64
import logging
import uuid

import pytest
import requests
from requests.auth import _basic_auth_str
from sqlalchemy import select

from onecdc import Replicator
from onecdc.common_functions import Utf8BasicAuth, odata_auth_header
from onecdc.data_reader import DataReader
from onecdc.handlers import Handler, HandlerLoop
from onecdc.metadata_reader import MetadataObject, MetadataReader
from onecdc.replicator import (_check_odata_auth, _check_odata_url,
                               _warn_if_credentials_go_in_clear)
from conftest import TEST_QUEUE_GUID

PASSWORD = 's3cret-pass'
# Уровень в тестах поднимаем этому логгеру поимённо: caplog.at_level без имени трогает только
# корневой, а уровень нашего мог поднять соседний тест — и logger.debug не дошёл бы вовсе.
READER_LOGGER = 'onecdc.data_reader'


# --- CDC-29: учётные данные ------------------------------------------------------------------

def test_a_mistyped_auth_tuple_does_not_print_the_password():
    """
    Сценарий аудита дословно: лишний элемент в кортеже. Раньше значение уходило в ValueError
    целиком (`got {odata_auth!r}`) — и дальше в трейсбек, в лог контейнера и в его сборщик.
    """
    with pytest.raises(ValueError) as failure:
        _check_odata_auth(('odata', PASSWORD, 'x'))

    assert PASSWORD not in str(failure.value)
    # Но понять, что не так, по сообщению всё ещё можно: названа форма значения.
    assert 'tuple of 3' in str(failure.value)


def test_requests_keeps_credentials_in_the_url_which_is_why_we_refuse_them():
    """
    Обоснование запрета, а не догадка о нём: requests userinfo из адреса НЕ убирает. Пароль
    остаётся в response.url и попадает в текст любой ошибки про этот запрос, то есть протекал бы
    в лог каждым сообщением. Если requests это когда-нибудь изменит, тест скажет об этом первым.
    """
    prepared = requests.Request('GET', f'http://odata:{PASSWORD}@host/base').prepare()

    assert PASSWORD in prepared.url
    assert prepared.headers['Authorization'].startswith('Basic ')


@pytest.mark.parametrize("url", [
    f'http://odata:{PASSWORD}@host/base/odata/standard.odata',
    f'https://odata:{PASSWORD}@host/base/odata/standard.odata',
    f'htp://odata:{PASSWORD}@host/base',              # ещё и схема с опечаткой
])
def test_credentials_in_the_url_are_refused_without_echoing_them(url):
    with pytest.raises(ValueError) as failure:
        _check_odata_url(url)

    assert PASSWORD not in str(failure.value), 'пароль в сообщении об ошибке'
    assert '***@' in str(failure.value), 'адрес показан, но без учётных данных'


def test_a_normal_url_passes_and_is_not_masked():
    assert _check_odata_url('http://host/base/odata/standard.odata  ') == \
        'http://host/base/odata/standard.odata'


# --- CDC-30: Basic по открытому HTTP ----------------------------------------------------------

@pytest.mark.parametrize("url, auth, expected", [
    ('http://host/base/odata/standard.odata', ('odata', PASSWORD), 1),
    ('https://host/base/odata/standard.odata', ('odata', PASSWORD), 0),
    ('http://host/base/odata/standard.odata', None, 0),
    ('http://host/base/odata/standard.odata', ('odata', ''), 0),   # пустой пароль нечего беречь
])
def test_a_password_over_plain_http_is_warned_about(caplog, url, auth, expected):
    with caplog.at_level(logging.WARNING):
        _warn_if_credentials_go_in_clear(url, auth)

    warnings = [r for r in caplog.records if 'clear text' in r.getMessage()]
    assert len(warnings) == expected
    assert all(PASSWORD not in r.getMessage() for r in caplog.records)


def test_the_warning_is_issued_once_at_startup(db, caplog):
    """Предупреждение — при создании репликатора, а не на каждый запрос: в сеть он тут не ходит."""
    with caplog.at_level(logging.WARNING):
        Replicator(odata_url='http://host/base/odata/standard.odata',
                   odata_auth=('odata', PASSWORD), exchange_name='ДляODATA',
                   queue_guid=TEST_QUEUE_GUID, engine=db.engine, db_schema=db.schema)

    assert len([r for r in caplog.records if 'clear text' in r.getMessage()]) == 1


# --- CDC-29: трейсбек обработчика в служебной таблице -----------------------------------------

def test_last_error_keeps_the_message_but_not_the_sql_and_its_parameters(db, caplog):
    """
    Раньше в onecdc_handlers клали хвост трейсбека в 4000 символов. У ошибки SQLAlchemy в нём
    едут и запрос, и значения параметров — то есть данные из витрины оказывались в таблице,
    которую читают все её потребители. Полный трейсбек остаётся в логе.
    """
    def noop(context):
        pass
    noop.ON = ["Catalog_X"]
    loop = HandlerLoop(engine=db.engine, schema=db.schema, handler=noop)

    class _Context:
        engine, schema = db.engine, db.schema

    try:
        Handler().execute(_Context(), 'SELECT 1 FROM "нет такой таблицы" WHERE "ИНН" = :inn',
                          inn='7712345678')
    except Exception as error:
        assert '[SQL:' in str(error) and '7712345678' in str(error), 'иначе тест ничего не ловит'
        loop._save_error(error)

    with db.engine.connect() as conn:
        stored = conn.execute(select(loop.table.c.last_error)
                              .where(loop.table.c.name == 'noop')).scalar()

    assert 'ProgrammingError' in stored, 'по строке всё ещё видно, что случилось'
    assert 'нет такой таблицы' in stored
    assert '[SQL:' not in stored and '7712345678' not in stored
    assert len(stored) <= 500
    loop.close()


# --- CDC-29: значения полей и флуд в логе читателя --------------------------------------------

def test_a_value_that_failed_conversion_is_not_in_the_warning(caplog):
    """Значение поля — бизнес-данные. В WARNING остаётся, ГДЕ и ЧТО не сошлось, само оно в DEBUG."""
    with caplog.at_level(logging.WARNING, logger=READER_LOGGER):
        assert DataReader._convert_value('ИНН7712345678', 'Guid',
                                         context='Catalog_Контрагенты.ИНН') is None

    message = '\n'.join(r.getMessage() for r in caplog.records)
    assert 'Catalog_Контрагенты.ИНН' in message and 'Guid' in message
    assert 'ИНН7712345678' not in message

    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger=READER_LOGGER):
        DataReader._convert_value('ИНН7712345678', 'Guid', context='Catalog_Контрагенты.ИНН')
    assert 'ИНН7712345678' in '\n'.join(r.getMessage() for r in caplog.records)


def test_an_unknown_field_is_reported_once_per_field_and_not_once_per_record(caplog):
    """
    Перечитывание метаданных было ограничено одним разом на объект, а сама строка лога — нет:
    поле вне $metadata давало запись на КАЖДУЮ запись страницы. На странице их десятки тысяч.
    """
    obj = "Catalog_Контрагенты"
    metadata = MetadataReader(odata_url="http://fake")
    metadata[obj] = MetadataObject(obj, {"Ref_Key": "Guid"}, {"Ref_Key": "Guid"})
    metadata.is_loaded = True
    metadata.get_metadata = lambda: None          # перечитывание поля не вернёт: его там нет
    reader = DataReader(odata_url="http://fake", metadata=metadata)

    with caplog.at_level(logging.WARNING, logger=READER_LOGGER):
        for _ in range(5):
            reader._get_record_fields({"d:Ref_Key": str(uuid.uuid4()), "d:ИНН": "7712345678"}, obj)

    unknown = [r for r in caplog.records if 'not found for object' in r.getMessage()]
    assert len(unknown) == 1, f'строк в логе: {len(unknown)}'
    assert 'ИНН' in unknown[0].getMessage()


# --- Сверх аудита: кириллические учётные данные ------------------------------------------------

CYRILLIC = ('админ', 'секрет')


def test_requests_cannot_send_cyrillic_credentials_itself():
    """
    Почему пара (пользователь, пароль) уходит в requests не кортежем: его Basic кодируется в
    latin-1 (историческое поведение, до RFC 7617) и роняет ЛЮБОЙ запрос ещё до отправки. Для 1С
    это не экзотика — пользователь «админ» с русским паролем заводится повсеместно.
    """
    with pytest.raises(UnicodeEncodeError):
        requests.Request('GET', 'http://host/', auth=CYRILLIC).prepare()


def test_cyrillic_credentials_are_sent_in_utf8():
    """
    UTF-8 — не компромисс, а единственное, что работает: на живой 1С (демо УТ, публикация на IIS)
    заголовок в UTF-8 принимается (200), тот же заголовок в cp1251 — 401.
    """
    prepared = requests.Request('GET', 'http://host/',
                                auth=odata_auth_header(CYRILLIC)).prepare()

    token = prepared.headers['Authorization'].removeprefix('Basic ')
    assert base64.b64decode(token).decode('utf-8') == 'админ:секрет'


def test_ascii_credentials_are_sent_exactly_as_before():
    # Для латиницы UTF-8 совпадает с тем, что собирает сам requests, — менять поведение не должно.
    assert odata_auth_header(('odata', PASSWORD))._header == _basic_auth_str('odata', PASSWORD)


def test_anything_else_is_passed_through_untouched():
    # None — анонимный доступ; готовый объект авторизации (NTLM, свой AuthBase) не трогаем.
    own = requests.auth.HTTPDigestAuth('odata', PASSWORD)
    assert odata_auth_header(None) is None
    assert odata_auth_header(own) is own


@pytest.mark.parametrize("reader", [
    lambda: DataReader(odata_url="http://fake", metadata=MetadataReader(odata_url="http://fake"),
                       odata_auth=CYRILLIC),
    lambda: MetadataReader(odata_url="http://fake", odata_auth=CYRILLIC),
])
def test_readers_convert_the_pair_themselves(reader):
    # Конвертация в ридерах, а не в конструкторе репликатора: ридеры создают и напрямую
    # (см. tests/debug_trade.py), и тогда кортеж дошёл бы до requests как есть.
    auth = reader().odata_auth

    assert isinstance(auth, Utf8BasicAuth)
    # И сам заголовок в logging не утечёт: repr объекта его не печатает.
    assert 'секрет' not in repr(auth)
