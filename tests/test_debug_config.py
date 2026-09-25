"""
Настройки лабораторных контуров (CDC-44).

Файлы `tests/<контур>.env` лежат в репозитории намеренно — это описание стенда, а не секреты. Но
правка их «под себя» рано или поздно уезжает в коммит вместе с чужими адресами, поэтому у своего
контура должен быть путь, не требующий трогать отслеживаемый файл.

И отдельно: ничто здесь не вправе падать на импорте. Живые тесты импортируются при ОБЫЧНОМ прогоне
(маркер integration отсеивает их после сбора), и отказ «не настроено» сломал бы весь pytest.
"""

from pathlib import Path

import pytest

from debug_config import TESTS_DIR, contour

CONTOURS = ('trade_demo1', 'buh_demo1')


def _write(path: Path, **values) -> None:
    path.write_text('\n'.join(f'{k}={v}' for k, v in values.items()), encoding='utf-8')


@pytest.mark.parametrize("name", CONTOURS)
def test_the_shipped_contours_are_complete(name):
    """Файлы в репозитории должны быть рабочими, иначе они только сбивают с толку."""
    settings = contour(name)

    assert settings.is_configured, settings.why_not
    assert settings.odata_url.startswith('http')
    assert settings.db_schema


def test_a_local_file_overrides_the_tracked_one(tmp_path, monkeypatch):
    """Свой контур — соседним *.local.env, он в .gitignore. Отслеживаемый файл трогать не надо."""
    monkeypatch.setattr('debug_config.TESTS_DIR', tmp_path)
    _write(tmp_path / 'stand.env', ONECDC_ODATA_URL='http://lab/odata',
           ONECDC_EXCHANGE_NAME='ДляODATA', ONECDC_QUEUE_GUID='guid', ONECDC_DB_URL='postgresql://x')
    _write(tmp_path / 'stand.local.env', ONECDC_ODATA_URL='http://моя-машина/odata')

    settings = contour('stand')

    assert settings.odata_url == 'http://моя-машина/odata'
    assert settings.exchange_name == 'ДляODATA', 'остальное берётся из общего файла'


def test_the_environment_wins_over_both(tmp_path, monkeypatch):
    monkeypatch.setattr('debug_config.TESTS_DIR', tmp_path)
    _write(tmp_path / 'stand.env', ONECDC_ODATA_URL='http://lab/odata')
    _write(tmp_path / 'stand.local.env', ONECDC_ODATA_URL='http://local/odata')
    monkeypatch.setenv('ONECDC_ODATA_URL', 'http://из-окружения/odata')

    assert contour('stand').odata_url == 'http://из-окружения/odata'


def test_an_unconfigured_contour_says_what_is_missing_instead_of_raising(tmp_path, monkeypatch):
    """
    Падение на импорте сломало бы обычный прогон тестов: живые тесты импортируются всегда.
    Поэтому «не настроено» — это состояние с объяснением, а не исключение.
    """
    monkeypatch.setattr('debug_config.TESTS_DIR', tmp_path)
    for name in ('ONECDC_ODATA_URL', 'ONECDC_EXCHANGE_NAME', 'ONECDC_QUEUE_GUID', 'ONECDC_DB_URL'):
        monkeypatch.delenv(name, raising=False)

    settings = contour('нет_такого')

    assert not settings.is_configured
    assert settings.odata_url is None
    assert 'ONECDC_ODATA_URL' in settings.why_not
    assert 'нет_такого.local.env' in settings.why_not, 'должно быть видно, куда это класть'


def test_the_file_is_parsed_and_not_executed(tmp_path, monkeypatch):
    """
    Отличие от `source`: тот исполняет файл как код оболочки — подстановка `$(…)` сработает, а
    значение с пробелом развалится на слова. Разбор ведёт себя так, как человек и написал.
    """
    monkeypatch.setattr('debug_config.TESTS_DIR', tmp_path)
    monkeypatch.delenv('ONECDC_ODATA_PASSWORD', raising=False)
    monkeypatch.delenv('ONECDC_ODATA_USER', raising=False)
    (tmp_path / 'stand.env').write_text(
        '# комментарий\n'
        'ONECDC_ODATA_USER=админ\n'
        'ONECDC_ODATA_PASSWORD="$(echo опасно) два слова"\n', encoding='utf-8')

    settings = contour('stand')

    assert settings.odata_auth == ('админ', '$(echo опасно) два слова')


@pytest.mark.parametrize("name", CONTOURS)
def test_a_contour_file_says_it_is_a_lab_stand(name):
    """
    Эти файлы копируют и правят. В них должно быть написано, что боевым значениям здесь не место
    и куда класть своё, — иначе первый же форк унесёт чужой контур в корпоративный git.
    """
    text = (TESTS_DIR / f'{name}.env').read_text(encoding='utf-8')

    assert 'Боевым значениям здесь не место' in text
    assert f'{name}.local.env' in text
