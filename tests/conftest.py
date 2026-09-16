"""
Общие фикстуры тестов: подключение к локальному PostgreSQL.

Целевая СУБД у продукта — PostgreSQL, на ней и тестируем. sqlite в тестах не используется: он
отличается ровно в тех местах, которые здесь и проверяются (нет схем, CURRENT_TIMESTAMP с точностью
до секунды, соединение = отдельная база в in-memory режиме), поэтому зелёный тест на sqlite ничего
не говорил бы о боевом поведении.

Каждый тест получает СВОЮ схему с уникальным именем, которая создаётся перед ним и сносится после.
Так тесты не мешают друг другу и не оставляют мусора в рабочих схемах базы.

Адрес базы переопределяется переменной ONECDC_TEST_DB_URL.
"""

import os
import uuid
from dataclasses import dataclass

import pytest
from sqlalchemy import Engine, create_engine, text

# Узел обмена в тестах: конструктор Replicator требует именно guid (см. _check_queue_guid),
# поэтому фиктивные "Q"/"guid" не подойдут.
TEST_QUEUE_GUID = "11111111-2222-3333-4444-555555555555"

TEST_DB_URL = os.environ.get(
    "ONECDC_TEST_DB_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432/onecdc")


@dataclass(frozen=True)
class TestDB:
    """Подключение и схема, отведённые одному тесту."""

    engine: Engine
    schema: str


@pytest.fixture
def db():
    schema = f"onecdc_test_{uuid.uuid4().hex[:8]}"
    engine = create_engine(TEST_DB_URL)
    try:
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        yield TestDB(engine=engine, schema=schema)
    finally:
        with engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        engine.dispose()


class FakeResponseMixin:
    """
    Дополняет самодельные заглушки ответа 1С до той части интерфейса requests, которой пользуется
    боевой код: потоковое чтение с потолком размера (`iter_content`, `headers`) и контекстный
    менеджер.

    Заглушки писались, когда код читал только `.text`/`.content`. После появления потолка размера
    ответа (он читает тело кусками, чтобы отказаться от гигантского, не приняв его целиком) этого
    стало мало. Заводим одно место, а не восемь копий: интерфейс общий, и расходиться ему незачем.
    """

    headers: dict = {}
    # Кодировку requests берёт из заголовков; у заглушек их нет, а боевой код на неё смотрит.
    encoding = 'utf-8'

    def iter_content(self, chunk_size=None):
        body = getattr(self, 'content', None)
        if body is None:
            body = (getattr(self, 'text', '') or '').encode('utf-8')
        yield body

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False
