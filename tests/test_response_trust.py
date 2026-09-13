"""
Отказ инфраструктуры не должен читаться как «данных нет».

Два разных механизма приводили к одному и тому же — ложным удалениям.

1. Любой 404 считался ответом «объекта нет». Смысл задуман верный: 1С на несуществующий объект
   отвечает честным 404. Но такой же 404 отдаёт IIS со снятой публикацией, ingress без правила
   во время обновления, чужой vhost. А вызывается это из перепроверки кандидатов на пометку, где
   «нет» означает «пометить удалённым» и погасить ресурсы.

2. Любое тело, которое разобралось как XML, но не содержало `feed`, считалось пустым набором.
   Честное «нет данных» от 1С — это feed с нулём entry, а не отсутствие feed. Страница шлюза
   с кодом 200 («Service temporarily unavailable», корректный XHTML) читалась как пустой объект,
   и полная выгрузка помечала удалённым ВСЁ, чего не увидела.
"""

from pathlib import Path

import pytest
import requests
from sqlalchemy import text

import fake_1c
from onecdc import Replicator
from onecdc.common_functions import ODataFormatError, is_entity_absent, parse_odata

GATEWAY_PAGE = ('<?xml version="1.0"?><html xmlns="http://www.w3.org/1999/xhtml">'
                '<body>Service temporarily unavailable</body></html>')
ODATA_NOT_FOUND = ('<?xml version="1.0" encoding="UTF-8"?>'
                   '<error xmlns="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">'
                   '<code/><message xml:lang="ru">Экземпляр сущности не найден</message></error>')


class _Response:
    def __init__(self, text_body: str, status: int = 200):
        self.text = text_body
        self.content = text_body.encode()
        self.status_code = status
        self.reason = 'OK' if status == 200 else 'Not Found'
        self.ok = status < 400
        self.url = 'http://fake'


# --- Кто ответил: 1С или то, что стоит перед ней ---

@pytest.mark.parametrize("body", [
    ODATA_NOT_FOUND,
    '<m:error xmlns:m="x"><m:message>Экземпляр сущности не найден</m:message></m:error>',
    'Экземпляр сущности не найден',                      # тело без разметки
    '{"odata.error": {"message": "Entity instance not found"}}',
])
def test_a_1c_answer_is_recognised(body):
    assert is_entity_absent(_Response(body, 404))


@pytest.mark.parametrize("body", [
    GATEWAY_PAGE,
    '<html><head><title>404 - Not Found</title></head><body><h1>404</h1></body></html>',
    '<!DOCTYPE html><html><body>Nothing matches the given URI.</body></html>',
    '',                                                  # прокси вообще без тела
])
def test_an_infrastructure_404_is_not_an_answer(body):
    assert not is_entity_absent(_Response(body, 404)), \
        'отказ инфраструктуры принят за ответ «объекта нет» — это ложные удаления'


# --- Структура ответа обязана быть OData ---

def test_an_empty_feed_is_legitimate_no_data():
    # Честное «нет данных»: feed есть, entry нет. Это НЕ ошибка.
    assert (parse_odata('<feed><id>x</id></feed>', 'feed', 'test') or {}).get('entry') is None
    assert parse_odata('<feed/>', 'feed', 'test') is None


@pytest.mark.parametrize("body", [
    GATEWAY_PAGE,
    '<?xml version="1.0"?><error><message>backend down</message></error>',
    '<html><body>maintenance</body></html>',
])
def test_a_body_without_a_feed_is_a_format_error(body):
    # Раньше всё это читалось как «ноль записей».
    with pytest.raises(ODataFormatError):
        parse_odata(body, 'feed', 'test')


def test_a_body_that_is_not_xml_at_all_says_so():
    with pytest.raises(ODataFormatError, match='not XML'):
        parse_odata('<html>\r\n<hr>\r\n</html>', 'feed', 'test')


# --- Сквозной сценарий: страница шлюза больше не вычищает таблицу ---

def _marked(db) -> tuple[int, int]:
    with db.engine.connect() as conn:
        return conn.execute(text(
            f'select count(*), count(*) filter (where is_deleted_or_empty) '
            f'from "{db.schema}"."Catalog_Nomenklatura"')).one()


def test_a_gateway_page_aborts_the_run_instead_of_marking_everything(db, monkeypatch):
    """
    Худший исход из возможных для сверки, воспроизведённый целиком: балансировщик отдаёт 200
    со страницей обслуживания, полная выгрузка читает «ноль записей», объявляет объект дочитанным
    и помечает удалённым всё, что в нём было. Прогон при этом отчитывается успехом.
    """
    from onecdc import data_reader

    with fake_1c.running_server(Path(__file__).parent / "responses" / "trade_demo_8.5") as (
            url, fake):
        repl = Replicator(odata_url=url, odata_auth=None, exchange_name="ДляODATA",
                          queue_guid=fake.queue_guid, engine=db.engine, db_schema=db.schema)
        repl.run_once()
        assert _marked(db) == (5, 0), 'предпосылка: пять живых строк'

        real_get = data_reader.requests.get
        monkeypatch.setattr(data_reader.requests, 'get',
                            lambda u, *a, **kw: (_Response(GATEWAY_PAGE)
                                                 if 'Номенклатура' in u else real_get(u, *a, **kw)))

        with pytest.raises(ODataFormatError):
            repl.full_load('Catalog_Номенклатура')

        assert _marked(db) == (5, 0), 'страница шлюза вычистила таблицу'


def test_an_infrastructure_404_during_recheck_does_not_mark_rows(db, monkeypatch):
    """
    Перепроверка кандидатов у регистра идёт по одному прямому запросу на регистратор. Пока любой
    404 означал «набора нет», перезапуск веб-сервера на полминуты помечал удалённым всё, что
    успели спросить за эти полминуты.
    """
    from onecdc import data_reader
    from onecdc.data_reader import DataReader
    from onecdc.metadata_reader import MetadataObject, MetadataReader

    OBJ = "AccumulationRegister_X"
    metadata = MetadataReader(odata_url="http://fake")
    metadata[OBJ] = MetadataObject(OBJ, {"Recorder": "Guid"}, {"Recorder": "Guid"})
    metadata.is_loaded = True
    reader = DataReader(odata_url="http://fake", metadata=metadata)

    monkeypatch.setattr(data_reader.requests, 'get',
                        lambda *a, **kw: _Response('<html><body>404</body></html>', 404))
    with pytest.raises(requests.HTTPError):
        reader.read_by_key(OBJ, {"Recorder": "x"})

    # А честный ответ 1С по-прежнему означает «набора нет» и исключением не становится.
    monkeypatch.setattr(data_reader.requests, 'get',
                        lambda *a, **kw: _Response(ODATA_NOT_FOUND, 404))
    assert reader.read_by_key(OBJ, {"Recorder": "x"}) == 0
