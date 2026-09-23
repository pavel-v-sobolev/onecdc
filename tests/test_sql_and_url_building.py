"""
Текст, который потом кто-то разбирает: SQL обработчика и URL запроса к 1С.

Оба случая — ошибки доверенной конфигурации, а не удалённые дыры: и схему, и имя плана обмена
задаёт оператор. Но цена у них разная. Имя плана с особым символом даёт молчаливый простой:
1С отвечает 400, цикл считает ошибку неустранимой и уходит в получасовую паузу. А помощник для SQL
ломался вообще без всякой злонамеренности — на обычной регулярке в запросе витрины: он разбирал
текст запроса через str.format. Теперь текст не разбирается вовсе, и это здесь и проверяется —
запросами на живом PostgreSQL, а не сравнением строк.
"""

import pytest

from config.handlers import zakazy_klientov, zakazy_klientov_grouped
from onecdc.db_logs import _check_create_schema
from onecdc.handlers import Handler, HandlerLoop
from onecdc.replicator import _check_db_schema, _check_exchange_name


class _Context:
    """Минимальный HandlerContext: execute/query берут из него только движок и схему."""

    def __init__(self, engine, schema):
        self.engine = engine
        self.schema = schema


# --- CDC-28: SQL обработчика ---

def test_a_handler_writes_the_schema_into_its_sql_itself(db):
    """
    Схему в свой запрос обработчик пишет сам, обычной f-строкой: `f'... "{schema}"."T" ...'`.
    Своего механизма подстановки у библиотеки нет — execute/query берут текст как есть.
    """
    handler, context = Handler(), _Context(db.engine, db.schema)
    schema = context.schema

    handler.execute(context, f'CREATE TABLE "{schema}"."T" (n int)')
    handler.execute(context, f'INSERT INTO "{schema}"."T" VALUES (:n)', n=7)

    assert handler.query(context, f'SELECT n FROM "{schema}"."T"') == [(7,)]
    # Таблица завелась именно в нашей схеме, а не в схеме по умолчанию.
    assert handler.query(context, "SELECT table_schema FROM information_schema.tables "
                                  "WHERE table_name = 'T'") == [(db.schema,)]


@pytest.mark.parametrize("sql, expected", [
    ("""SELECT ('{"k": 1}'::jsonb ->> 'k')::int""", 1),      # JSON-литерал
    (r"""SELECT ('{"k"\:1}'::jsonb ->> 'k')::int""", 1),     # он же, без пробела после двоеточия
    ("""SELECT ('{1,2,3}'::int[])[2]""", 2),                 # массив Postgres
    ("""SELECT ('abc123' ~ '^[a-z]+[0-9]{3}$')::int""", 1),  # квантификатор регулярки
])
def test_the_query_text_is_not_parsed_on_the_way(db, sql, expected):
    r"""
    `str.format` разбирал ВЕСЬ текст запроса и падал ещё до обращения к БД — с сообщением вроде
    `KeyError: '"k"'`, по которому не догадаться, что дело в помощнике. Первый же, кто напишет в
    витрине регулярку или JSON, упирался в это. Текст теперь доезжает до сервера нетронутым.

    Двоеточие — забота уже не наша, а SQLAlchemy: в text() оно начинает параметр, поэтому
    `'{"k":1}'` пишется как `'{"k"\:1}'` (или с пробелом после двоеточия). Обе формы здесь и
    проверены, чтобы было видно, где чья ответственность.
    """
    assert Handler().query(_Context(db.engine, db.schema), sql) == [(expected,)]


def test_the_shipped_examples_build_their_sql_the_same_way(db):
    """Примеры — то, что копируют, поэтому способ в них должен быть ровно тот же."""
    context = _Context(db.engine, db.schema)

    texts = [zakazy_klientov.ddl(context), zakazy_klientov.rebuild_blocks_sql(context),
             zakazy_klientov_grouped.ddl(context),
             zakazy_klientov_grouped.groups_to_handle_sql(context),
             zakazy_klientov_grouped.rebuild_blocks_sql(context)]

    for sql in texts:
        assert f'"{db.schema}".' in sql, 'схема не подставлена'
        assert '{' not in sql, 'остался маркер или неудвоенная скобка'
    # Окно осталось параметром, а не уехало в текст.
    assert ':last_run_at' in zakazy_klientov_grouped.groups_to_handle_sql(context)


@pytest.mark.parametrize("value", ['on"ecdc', 'one\ncdc', 'onecdc\t;', 'щ' * 32])
def test_a_schema_name_that_looks_like_a_mistake_is_refused(value):
    """
    Проверка про написание, а не про безопасность: значение с кавычкой или переводом строки — это
    почти всегда неразвёрнутая переменная окружения. А 32 кириллические буквы это 64 байта, и
    Postgres обрезал бы имя МОЛЧА.
    """
    with pytest.raises(ValueError):
        _check_db_schema(value)


@pytest.mark.parametrize("value", ['onecdc', 'схема_1', '  onecdc  ', None, ''])
def test_a_normal_schema_name_passes(value):
    assert _check_db_schema(value) in ('onecdc', 'схема_1', None)


def test_every_path_to_the_database_checks_the_schema_name(db):
    """
    Проверка стоит там, где схему заводят, а не у одного вызывающего: репликатор свой параметр
    проверяет сам, а цикл обработчиков создаётся и напрямую (см. config/runner.py).
    """
    def noop(context):
        pass
    noop.ON = ["Catalog_X"]

    with pytest.raises(ValueError, match="quotes"):
        HandlerLoop(engine=db.engine, schema='on"ecdc', handler=noop)
    with pytest.raises(ValueError, match="quotes"):
        _check_create_schema(db.engine, 'on"ecdc')


# --- CDC-31: имя плана обмена в URL ---

@pytest.mark.parametrize("name", ["A'B", 'A&B', 'A#B', 'A/B', 'A B', '1A', 'A-B'])
def test_a_plan_name_that_would_break_the_url_is_refused(name):
    """
    Имя уходит в URL внутрь ВЛОЖЕННОГО литерала:
        DataExchangePoint='…/ExchangePlan_<имя>(guid'…')'&MessageNo=N
    Кавычка ломает литерал, «&» дописывает свой параметр, «#» отрезает всё после себя — включая
    MessageNo. Раньше запрещались только «/» и пробел, то есть перечень был дырявым по построению.
    """
    with pytest.raises(ValueError):
        _check_exchange_name(name)


@pytest.mark.parametrize("name", ['ДляODATA', 'План_1', 'A1', '_X'])
def test_a_normal_plan_name_passes(name):
    assert _check_exchange_name(name) == name


@pytest.mark.parametrize("name", [None, '', '   '])
def test_no_plan_name_means_full_loads_only(name):
    # Пустое имя больше не ошибка: так собирают репликатор, который только выгружает. Читать
    # изменения он откажется (см. test_config_validation), а конструктор не мешает.
    assert _check_exchange_name(name) == ''


def test_a_prefixed_plan_name_is_still_accepted():
    # Префикс снимается и только потом проверяется — иначе точка в «ПланОбмена.X» не прошла бы.
    assert _check_exchange_name('ПланОбмена.ДляODATA') == 'ДляODATA'
    assert _check_exchange_name('ExchangePlan_ДляODATA') == 'ДляODATA'
