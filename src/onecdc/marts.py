"""Совместимые импорты beta-витрин; реализация целиком принадлежит dbmerge.

Новый код импортирует API напрямую из dbmerge. Передайте открытый tracker
конструктору MartHandler и вызывайте run_once(), run_forever() или loop().
Удалённый в dbmerge 2.5 run_mart здесь не эмулируется.
"""

from dbmerge.mart import (MartContext, MartHandler, rebuild_requested, request_rebuild,
                         request_stop, reset_stop)

__all__ = ["MartContext", "MartHandler", "rebuild_requested", "request_rebuild",
           "request_stop", "reset_stop"]