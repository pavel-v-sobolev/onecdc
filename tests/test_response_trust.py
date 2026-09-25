"""
Отказ инфраструктуры не должен читаться как «данных нет».

Любое тело, которое разобралось как XML, но не содержало `feed`, считалось пустым набором. Честное
«нет данных» от 1С — это feed с нулём entry, а не отсутствие feed. Страница шлюза с кодом 200
(«Service temporarily unavailable», корректный XHTML) читалась как пустой объект, и полная
выгрузка помечала удалённым ВСЁ, чего не увидела.

Второй механизм той же беды — «любой 404 значит, что объекта нет» — из кода ушёл совсем. Его
проверяла отдельная функция (`is_entity_absent`), но спрашивать «404 от 1С или от веб-сервера»
стало негде: перепроверка кандидатов убрана в CDC-05, а поиск плана видов субконто переведён на
$filter, где ответ «нет такого» — это 200 и пустая коллекция (CDC-42).
"""

from pathlib import Path

import pytest
import requests
from sqlalchemy import text

import fake_1c
from onecdc import Replicator
from onecdc.common_functions import ODataFormatError, parse_odata

from conftest import FakeResponseMixin

GATEWAY_PAGE = ('<?xml version="1.0"?><html xmlns="http://www.w3.org/1999/xhtml">'
                '<body>Service temporarily unavailable</body></html>')
ODATA_NOT_FOUND = ('<?xml version="1.0" encoding="UTF-8"?>'
                   '<error xmlns="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">'
                   '<code/><message xml:lang="ru">Экземпляр сущности не найден</message></error>')


class _Response(FakeResponseMixin):
    def __init__(self, text_body: str, status: int = 200):
        self.text = text_body
        self.content = text_body.encode()
        self.status_code = status
        self.reason = 'OK' if status == 200 else 'Not Found'
        self.ok = status < 400
        self.url = 'http://fake'


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

