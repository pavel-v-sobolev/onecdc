"""
Воспроизводимость поставки: чем закреплены версии (CDC-41).

Дефекта логики здесь нет — есть риск поставки. Мажорное обновление соседней библиотеки приезжает
в собранный образ само, и «CI зелёный на вчерашних версиях, образ собран сегодня» — это не
гипотеза: onecdc держится на возможностях dbmerge, которых у обычного merge нет, а поведение
одного из его параметров на нашей же памяти уже менялось (см. историю temp_schema).
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = ROOT / "pyproject.toml"
LOCK = ROOT / "uv.lock"
CI = ROOT / ".github" / "workflows" / "ci.yml"
DOCKERFILE = ROOT / "Dockerfile"

tomllib = pytest.importorskip("tomllib", reason="tomllib появился в 3.11; на 3.10 пропускаем")


def _requirements() -> list[str]:
    """
    Что уезжает пользователю: обычные зависимости и экстры, кроме dev.

    dev не считаем намеренно: эти версии закрепляет uv.lock, которым CI и собирается, а ограничить
    сверху pytest значило бы мешать себе же — на пользовательскую установку он не попадает.
    """
    project = tomllib.loads(PYPROJECT.read_text())["project"]
    extras = [spec for name, group in project.get("optional-dependencies", {}).items()
              if name != 'dev' for spec in group]
    return project["dependencies"] + extras


def test_every_dependency_has_an_upper_bound():
    """
    Нижняя граница говорит «нам нужно не старее», верхняя — «мажор мы не проверяли». Без второй
    обновление соседнего проекта попадает в образ, минуя наши тесты.
    """
    unbounded = [spec for spec in _requirements() if '<' not in spec]

    assert unbounded == [], f'без верхней границы: {unbounded}'


def test_the_lock_file_is_in_the_repository():
    """Файл есть — но сам по себе он ничего не гарантирует, см. следующий тест."""
    assert LOCK.exists() and LOCK.stat().st_size > 0


def test_ci_refuses_to_resolve_dependencies_anew():
    """
    `uv sync` при расхождении с pyproject молча пересчитает зависимости и перепишет lock: матрица
    тестов собиралась бы каждый раз из «последних» версий. `--locked` делает расхождение отказом.
    """
    sync_steps = [line for line in CI.read_text().splitlines() if 'uv sync' in line]

    assert sync_steps, 'шаг установки зависимостей исчез — тест устарел'
    assert all('--locked' in step for step in sync_steps), sync_steps


def test_the_image_installs_an_exact_version():
    """Версия образа = версия пакета: диапазон здесь сделал бы образ невоспроизводимым."""
    dockerfile = DOCKERFILE.read_text()

    assert re.search(r'pip install .*onecdc\[postgres\]==\$\{ONECDC_VERSION\}', dockerfile)
    # Базовый образ по тегу — решение осознанное, и оно объяснено на месте.
    assert 'FROM python:' in dockerfile
    assert 'патчи безопасности' in dockerfile, 'почему не дайджест — должно быть написано'
