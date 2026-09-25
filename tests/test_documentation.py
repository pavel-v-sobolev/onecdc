"""
Документация как контракт: что в ней написано про код, должно быть правдой.

Проверяется механически то, что механически проверяемо, — сигнатуры и имена. Разошлись они молча:
конструктор `Replicator` поменял порядок и обзавёлся тремя параметрами, а README_API ещё неделю
показывал старый (CDC-38).
"""

import inspect
import re
from pathlib import Path

import pytest

from onecdc import FullLoadCron, HandlerLoop, Replicator

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / 'src' / 'onecdc'

# Что удалено и больше не должно описываться как действующее. CHANGELOG и разбор аудита не
# считаем — там это история, и она обязана остаться.
REMOVED = ('read_by_key', '_recheck_batch', '_still_in_1c', 'is_entity_absent',
           '_supports_keyset', '_keyset_filter', '_period_partitions',
           'DB_NOW_WITHOUT_TIMEZONE', 'read_accounting_register')
HISTORY_IS_FINE = {'CHANGELOG.md', 'PLAN.md', 'audit_status_0.2.1.md'}

# Механизм, у которого не осталось имени в коде: перепроверку кандидатов на пометку убрали вместе
# с её методами, а рассказ о ней остался в семи местах — и по именам его было не поймать.
REMOVED_PHRASES = (r'перепроверк\w* кандидат', r'фаз\w* перепроверк')


def _documented_signature(text: str, cls: str) -> list[str]:
    """Имена параметров из блока ```python Cls( … ) ``` в документации."""
    block = re.search(rf'```python\n{cls}\((.*?)\n\)\n```', text, re.S)
    assert block, f'в README_API нет блока с сигнатурой {cls}'
    return re.findall(r'^\s{4}(\w+)\s*[:=]', block.group(1), re.M)


@pytest.mark.parametrize("cls", [Replicator])
def test_the_documented_constructor_matches_the_code(cls):
    documented = _documented_signature((ROOT / 'README_API.md').read_text(), cls.__name__)
    actual = [name for name in inspect.signature(cls.__init__).parameters if name != 'self']

    assert documented == actual, 'порядок и состав параметров разошлись с кодом'


def test_documented_methods_exist_with_the_documented_parameters():
    """
    В справочнике методы подписаны параметрами. Параметр, которого нет, — обещание, за которым
    пользователь придёт с TypeError.

    Сверяем со ВСЕМИ публичными классами сразу: `run_forever` есть у трёх, и какой из них описывает
    конкретная строка справочника, из текста не вывести. Ошибка — параметр, которого нет ни у
    одного.
    """
    text = (ROOT / 'README_API.md').read_text()
    known: dict[str, set[str]] = {}
    for cls in (Replicator, HandlerLoop, FullLoadCron):
        for name, member in inspect.getmembers(cls, inspect.isfunction):
            if not name.startswith('_'):
                known.setdefault(name, set()).update(
                    set(inspect.signature(member).parameters) - {'self'})

    for method, mention in re.findall(r'`(\w+)\(([^`]*)\)`', text):
        if method not in known:
            continue
        for parameter in re.findall(r'(\w+)=', mention):
            assert parameter in known[method], \
                f'{method}: параметра {parameter} нет ни у одного класса'


def test_the_readme_engine_example_caps_the_pool():
    """
    Пример из README копируют первым. Один `pool_size` без `max_overflow` — это молча до
    пятнадцати соединений на процесс вместо заявленных (CDC-40), а без `pool_pre_ping` первый
    запрос после ночного простоя падает.
    """
    example = re.search(r'```python\n(from sqlalchemy import create_engine.*?)```',
                        (ROOT / 'README.md').read_text(), re.S)
    assert example, 'в README нет примера сборки репликатора'

    for parameter in ('pool_size', 'max_overflow', 'pool_pre_ping'):
        assert parameter in example.group(1), f'{parameter} пропал из примера'


def test_the_docs_do_not_describe_removed_machinery():
    """
    Удалённый механизм, описанный как действующий, хуже отсутствия описания: по нему принимают
    решения. Историю в CHANGELOG и в разборе аудита это не трогает.
    """
    offenders = []
    for path in list(ROOT.glob('*.md')) + list(SRC.glob('*.py')):
        if path.name in HISTORY_IS_FINE:
            continue
        text = path.read_text()
        for name in REMOVED:
            for number, line in enumerate(text.splitlines(), 1):
                # Упоминание в прошедшем времени законно: «раньше здесь было …».
                if name in line and not re.search(r'раньше|прежде|больше не|удал|ушл', line, re.I):
                    offenders.append(f'{path.name}:{number}: {name}')
        for phrase in REMOVED_PHRASES:
            for number, line in enumerate(text.splitlines(), 1):
                if re.search(phrase, line, re.I) and not re.search(
                        r'раньше|прежде|больше не|удал|ушл|нет|не пробуем', line, re.I):
                    offenders.append(f'{path.name}:{number}: {phrase}')

    assert offenders == [], offenders
