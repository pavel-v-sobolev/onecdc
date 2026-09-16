"""
Неверная конфигурация обязана падать сразу и с внятным текстом.

Правило это в проекте объявлено, и конструкторы ему следуют. Но часть значений сквозь проверки
проходила — и расплата была хуже обычной ошибки: зависание, горячий цикл или обработчик, который
зарегистрирован и не вызывается НИКОГДА, без единой строки в логе.
"""

import math
import os
from datetime import date, datetime, timedelta, timezone

import pytest

from onecdc.cron_runner import _bound
from onecdc.handlers import Handler, as_handler
from onecdc.replicator import _check_batch_size, _check_period_bound, _check_request_timeout


def test_on_as_a_string_is_refused():
    """
    Самый молчаливый случай из всех: строка — это коллекция СИМВОЛОВ.

    `frozenset("Catalog_X")` давал множество букв, обработчик регистрировался без возражений и не
    вызывался никогда. Витрина просто не обновлялась, и понять почему было нечем.
    """
    class Odin(Handler):
        NAME = 'Odin'
        ON = "Catalog_X"

        def handle(self, context):
            pass

    with pytest.raises(TypeError, match=r"ON = \['Catalog_X'\]"):
        as_handler(Odin())


def test_on_as_a_list_still_works():
    class Odin(Handler):
        NAME = 'Odin'
        ON = ["Catalog_X"]

        def handle(self, context):
            pass

    assert as_handler(Odin()).on == frozenset({"Catalog_X"})


@pytest.mark.parametrize("raw", ['nan', 'inf', '-inf'])
def test_a_non_finite_environment_number_is_refused(raw, monkeypatch):
    # nan проходил проверку на положительность (сравнение с ним всегда ложно) и делал паузу цикла
    # пустой — горячий цикл, который долбит 1С без остановки. inf останавливал опрос после
    # первого оборота.
    from onecdc.__main__ import _number

    monkeypatch.setenv('ONECDC_X', raw)
    with pytest.raises(SystemExit, match='finite'):
        _number('ONECDC_X', '60')


def test_a_normal_environment_number_still_works(monkeypatch):
    from onecdc.__main__ import _number

    monkeypatch.setenv('ONECDC_X', '30')
    assert _number('ONECDC_X', '60') == 30.0


@pytest.mark.parametrize("value", [(30,), (30, 900, 1), 'abc', 0, -1, (0, 900)])
def test_a_malformed_timeout_is_refused(value):
    # Кортеж из одного мы принимали, а requests его отвергает («Invalid timeout (30,)») — то есть
    # ошибка всплывала не при создании репликатора, а при первом запросе.
    with pytest.raises(ValueError):
        _check_request_timeout(value)


@pytest.mark.parametrize("value", [30, (30, 900), None])
def test_a_correct_timeout_passes(value):
    assert _check_request_timeout(value) == value


@pytest.mark.parametrize("value", [0, -1, 1.5, True, '10', None])
def test_a_non_positive_batch_size_is_refused(value):
    # Ноль зацикливал прогон намертво: пустая страница, `0 < 0` ложно, смещение растёт на ноль.
    with pytest.raises(ValueError, match='positive integer'):
        _check_batch_size(value)


def test_an_aware_period_bound_is_refused():
    """
    Aware-datetime уходил в OData-литерал БЕЗ смещения, а в SQL-условие области пометки — вместе
    с ним. Прогон читал одно окно, а помечал другое, и разница измерялась часами.

    Догадываться, что имел в виду пользователь — отбросить пояс или перевести время, — библиотека
    не вправе, а 1С работает без поясов вовсе.
    """
    aware = datetime(2026, 6, 1, 10, 0, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match='naive datetime'):
        _check_period_bound(aware, 'date_from')


def test_a_period_bound_is_truncated_to_a_second():
    # Микросекунды в литерал не попадают (1С их не принимает), а в область пометки уходили как
    # есть — те же две разные границы, только разница в долях секунды.
    assert _check_period_bound(datetime(2026, 6, 1, 10, 30, 45, 123456), 'date_from') \
        == datetime(2026, 6, 1, 10, 30, 45)
    # Дата и строка проходят как были: у даты своя семантика (весь день), строку разбирает 1С.
    assert _check_period_bound(date(2026, 6, 1), 'date_to') == date(2026, 6, 1)
    assert _check_period_bound('2026-06-01', 'date_to') == '2026-06-01'


def test_a_sub_day_schedule_offset_keeps_its_hours():
    """
    `date.today() - timedelta(hours=12)` отбрасывал часы молча: окно «полсуток назад» оказывалось
    сегодняшним, то есть пустым.
    """
    got = _bound(timedelta(hours=12))

    assert isinstance(got, datetime), 'часы потеряны — граница снова стала датой'
    assert timedelta(hours=11, minutes=59) < datetime.now() - got < timedelta(hours=12, minutes=1)


def test_a_whole_day_schedule_offset_still_starts_at_midnight():
    # Ночное расписание «перечитать последнюю неделю» должно брать неделю целиком, а не с
    # текущего часа, — поэтому целые сутки считаются от ДАТЫ, как и раньше.
    assert _bound(timedelta(days=7)) == date.today() - timedelta(days=7)
    assert _bound(timedelta(days=1)) == date.today() - timedelta(days=1)
