"""Витрины dbmerge: единые аренда, журнал, остановка и состояние пересборки.

``schema`` — служебная схема журнала; бизнес-схемы задаются в ``destination`` и
``sources`` обработчика. Пример запроса пересборки:
``request_rebuild(engine, ("dwh", "orders_by_customer"), schema="service")``.
Состояние хранится в dbmerge_marts; отдельной таблицы onecdc_marts нет.
"""

from dbmerge.mart import (MartContext, MartHandler, rebuild_requested, request_rebuild,
                         request_stop, reset_stop)
from dbmerge.mart import run_mart as _run_mart


def run_mart(mart, engine=None, *, poll_interval=60.0, schema=None, tracker=None,
             lease=None, run_forever=True):
    """Запустить витрину либо один проход с ``run_forever=False``.

    Принимает MartHandler, модуль или функцию с destination/sources. Собственный
    tracker закрывается на выходе, переданный остаётся у вызывающего кода.
    Каждый проход защищён арендой destination, в том числе между процессами.
    """
    return _run_mart(mart, engine=engine, tracker=tracker, poll_interval=poll_interval,
                     schema=schema, lease=lease, run_forever=run_forever)