"""
Один неразбираемый объект не должен останавливать весь план обмена.

Пакет читается и подтверждается целиком, поэтому до этого механизма любое неустранимое исключение
на одной entry выбрасывало нас из всего разбора: пакет не дочитан, ничего не сохранено,
подтверждение не отправлено. И так каждый цикл — а backoff разводил ошибку в логе до одной строки в
полчаса, то есть устойчивость цикла саму проблему и маскировала.

Измерено на записанном пакете: убрать из метаданных ОДНУ табличную часть справочника — и не
сохраняется ничего, включая сам справочник и две другие его части, с которыми всё в порядке.
"""

import logging
from pathlib import Path

import pytest
from sqlalchemy import inspect, text

import fake_1c
from onecdc import Replicator
from onecdc.replicator import PARSE_FAILURE_LIMIT

RESPONSES = Path(__file__).parent / "responses" / "trade_demo_8.5"
# Табличная часть, которая реально приезжает в первом пакете записанных ответов.
VICTIM = "Catalog_Номенклатура_ДополнительныеРеквизиты"


def _replicator(db, url, fake):
    return Replicator(odata_url=url, odata_auth=None, exchange_name="ДляODATA",
                      queue_guid=fake.queue_guid, engine=db.engine, db_schema=db.schema)


def _break_metadata(repl):
    """Убирает табличную часть из метаданных — как если бы её _RowType не включили в состав OData."""
    repl.metadata.get_metadata()
    del repl.metadata[VICTIM]
    repl.metadata.get_metadata = lambda: None      # перечитывание её не вернёт: её там и нет


def _data_tables(db):
    return sorted(t for t in inspect(db.engine).get_table_names(schema=db.schema)
                  if not t.startswith('onecdc_'))


def test_the_rest_of_the_package_is_saved(db, caplog):
    with fake_1c.running_server(RESPONSES) as (url, fake):
        repl = _replicator(db, url, fake)
        try:
            _break_metadata(repl)
            with caplog.at_level(logging.WARNING):
                repl.run_once()
        finally:
            repl.close()

    # Сам справочник и уцелевшие части сохранены — блокировка больше не полная.
    tables = _data_tables(db)
    assert "Catalog_Nomenklatura" in tables
    assert len(tables) >= 2, 'сохранилось только то, что упало вместе с частью'
    assert 'could not be parsed' in caplog.text


def test_the_package_is_not_confirmed_while_attempts_remain(db, caplog):
    # Причина может быть временной, поэтому изменения не выбрасываем с первого раза: пакет не
    # подтверждаем, и 1С пришлёт его снова.
    with fake_1c.running_server(RESPONSES) as (url, fake):
        repl = _replicator(db, url, fake)
        try:
            _break_metadata(repl)
            with caplog.at_level(logging.WARNING):
                repl.run_once()

            assert fake.received_no == 0, 'пакет подтверждён, хотя попытки ещё есть'
            assert 'is NOT confirmed' in caplog.text
            assert f'attempt 1 of {PARSE_FAILURE_LIMIT}' in caplog.text
        finally:
            repl.close()


def test_attempts_run_out_and_the_plan_moves_on(db, caplog):
    """
    Главное свойство: попытки кончаются. Иначе один объект держит весь план обмена бесконечно.
    """
    with fake_1c.running_server(RESPONSES) as (url, fake):
        repl = _replicator(db, url, fake)
        try:
            _break_metadata(repl)
            for attempt in range(PARSE_FAILURE_LIMIT):
                with caplog.at_level(logging.ERROR):
                    repl.run_once()

            assert fake.received_no == 1, 'пакет так и не подтверждён — план стоит'
            assert 'CHANGES LOST' in caplog.text
            assert VICTIM in caplog.text, 'в сообщении обязан стоять виновник'
        finally:
            repl.close()


def test_an_object_that_fixes_itself_does_not_carry_old_failures(db):
    """
    Счётчик обнуляется успешным разбором: объект, починившийся сам, не должен нести груз прошлых
    неудач и вылетать в карантин на ровном месте.

    Сбрасывать надо именно тем, кто разобрался ЦЕЛИКОМ. Владелец попадает в пакет раньше, чем
    разбор доходит до его табличных частей, поэтому упавший на части числится и среди разобранных,
    и среди упавших — сброс по одному лишь присутствию обнулял бы ему счётчик каждый цикл, и
    попытки не кончались бы никогда.
    """
    with fake_1c.running_server(RESPONSES) as (url, fake):
        repl = _replicator(db, url, fake)
        try:
            repl.metadata.get_metadata()
            restored = repl.metadata[VICTIM]        # снимок ДО поломки: метаданные уже загружены
            _break_metadata(repl)
            repl.run_once()
            assert repl._parse_failures.get("Catalog_Номенклатура") == 1

            # Часть опубликовали — следующий пакет разбирается целиком.
            repl.metadata[VICTIM] = restored
            repl.run_once()

            assert "Catalog_Номенклатура" not in repl._parse_failures
        finally:
            repl.close()
