"""Изменяет ТОЛЬКО тестовую trade_demo1 через OData, затем восстанавливает поля.

Запуск: ONECDC_LIVE_WRITE=1 pytest tests/test_live_odata_traffic.py
        -m integration -o addopts='' -s
Пакеты не подтверждаются; CDC пишет в удаляемую временную PostgreSQL-схему.
Версии объектов и регистрации обмена после записи восстановить невозможно.
"""

import json
import os
from pathlib import Path
import uuid

import pytest
import requests
from dbmerge import MergeTracker, reset_stop
from sqlalchemy import create_engine, inspect, text

from config.handlers import ZakazyKlientov, ZakazyKlientovGrouped
from debug_config import contour
from onecdc import Replicator
from onecdc.common_functions import odata_auth_header


@pytest.mark.integration
@pytest.mark.skipif(os.environ.get('ONECDC_LIVE_WRITE') != '1',
                    reason='OData writes require explicit ONECDC_LIVE_WRITE=1')
def test_live_odata_traffic_updates_marts():
    settings = contour('trade_demo1')
    if not settings.is_configured:
        pytest.skip(settings.why_not)
    engine = create_engine(settings.db_url, pool_pre_ping=True)
    schema = f'onecdc_traffic_{uuid.uuid4().hex[:12]}'
    backup = Path(f'/tmp/{schema}-restore.json')
    session = requests.Session()
    session.auth = odata_auth_header(settings.odata_auth)
    originals = []
    repl = None
    reset_stop()

    def request(method, path, **kwargs):
        response = session.request(method, settings.odata_url.rstrip('/') + '/' + path,
                                   timeout=(30, 120), **kwargs)
        assert response.ok, f'{method}: HTTP {response.status_code}: {response.text[:1500]}'
        return response

    def read(path):
        return request('GET', path, params={'$format': 'json'}).json()

    def patch(path, field, value):
        current = read(path)
        headers = {'If-Match': current['DataVersion']} if current.get('DataVersion') else {}
        request('PATCH', path, params={'$format': 'json'}, json={field: value}, headers=headers)
        assert read(path)[field] == value

    def remember(path, field):
        value = read(path)[field]
        originals.append({'path': path, 'field': field, 'value': value})
        backup.write_text(json.dumps(originals, ensure_ascii=False, indent=2))
        return value

    try:
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        with MergeTracker(engine, schema=schema) as tracker:
            repl = Replicator(odata_url=settings.odata_url, odata_auth=settings.odata_auth,
                              exchange_name=settings.exchange_name, queue_guid=settings.queue_guid,
                              engine=engine, db_schema=schema, tracker=tracker,
                              automatic_full_load=False)
            try:
                # Полный исходный набор нужен для проверки переезда групп, а не только пакета CDC.
                for source in ZakazyKlientovGrouped.sources:
                    repl.full_load(source, skip_if_busy=False)
                repl.run_once(notify_changes=False)
                assert not repl.changes.failed_objects
                marts = [cls(engine, tracker=tracker, schema=schema)
                         for cls in (ZakazyKlientov, ZakazyKlientovGrouped)]
                for mart in marts:
                    assert mart.run_once(rebuild=True)

                with engine.connect() as conn:
                    candidate = conn.execute(text(f'''
                        SELECT s."Recorder", s."Nomenklatura_Key"
                        FROM "{schema}"."AccumulationRegister_ZakazyKlientov" s
                        JOIN "{schema}"."Document_ZakazKlienta" d ON d."Ref_Key" = s."Recorder"
                        JOIN "{schema}"."Catalog_Nomenklatura" n ON n."Ref_Key" = s."Nomenklatura_Key"
                        WHERE d."Posted" AND NOT s.is_deleted_or_empty AND s."Zakazano" <> 0
                          AND s."Recorder_Type" = 'Document_ЗаказКлиента'
                        LIMIT 1''')).one()
                document = f"Document_ЗаказКлиента(guid'{candidate[0]}')"
                catalog = f"Catalog_Номенклатура(guid'{candidate[1]}')"
                old_code = remember(catalog, 'Code')
                old_number = remember(document, 'Number')
                assert remember(document, 'Posted') is True

                def cycle(label):
                    repl.run_once(notify_changes=False)
                    assert not repl.changes.failed_objects
                    ran = [mart.run_once() for mart in marts]
                    for mart in marts:
                        name = mart.destination[1]
                        columns = ', '.join('"' + c['name'] + '"' for c in
                                            inspect(engine).get_columns(name, schema=schema))
                        with engine.connect() as conn:
                            differences = conn.execute(text(f'''
                                (SELECT {columns} FROM "{schema}"."{name}"
                                 EXCEPT ALL SELECT {columns} FROM "{schema}"."{name}_view")
                                UNION ALL
                                (SELECT {columns} FROM "{schema}"."{name}_view"
                                 EXCEPT ALL SELECT {columns} FROM "{schema}"."{name}")
                            ''')).all()
                        assert not differences, f'{label}: {name}: {differences[:3]}'
                    print(f'{label}: CDC rows={repl.changes.rows_read()}, mart passes={ran}, '
                          'full row comparison passed')
                    return ran

                token = uuid.uuid4().hex
                new_code = ('T' + token)[:len(old_code)]
                patch(catalog, 'Code', new_code)
                assert all(cycle('catalog code changed'))
                with engine.connect() as conn:
                    assert conn.scalar(text(f'SELECT count(*) FROM "{schema}"."ZakazyKlientov" '
                                            'WHERE "Artikul" = :code'), {'code': new_code}) > 0

                new_number = ('T' + token[::-1])[:len(old_number)]
                patch(document, 'Number', new_number)
                assert any(cycle('document moved to new group'))
                with engine.connect() as conn:
                    assert conn.scalar(text(f'SELECT count(*) FROM "{schema}"."ZakazyKlientovGrouped" '
                                            'WHERE "Number" = :number'), {'number': new_number}) > 0

                patch(document, 'Number', old_number)
                patch(catalog, 'Code', old_code)
                assert all(cycle('original fields restored'))

                # Штатное перепроведение уже проведённого документа, без Unpost.
                request('POST', document + '/Post', params={'PostingModeOperational': 'false'})
                assert read(document)['Posted'] is True
                cycle('document reposted')

                request('POST', document + '/Unpost', params={'PostingModeOperational': 'false'})
                assert read(document)['Posted'] is False
                cycle('document unposted: movements removed')
                with engine.connect() as conn:
                    assert conn.scalar(text(
                        f'SELECT count(*) FROM "{schema}"."AccumulationRegister_ZakazyKlientov" '
                        'WHERE "Recorder" = :ref AND NOT is_deleted_or_empty'),
                        {'ref': candidate[0]}) == 0
                request('POST', document + '/Post', params={'PostingModeOperational': 'false'})
                assert read(document)['Posted'] is True
                cycle('document posted again: movements restored')
                with engine.connect() as conn:
                    assert conn.scalar(text(
                        f'SELECT count(*) FROM "{schema}"."AccumulationRegister_ZakazyKlientov" '
                        'WHERE "Recorder" = :ref AND NOT is_deleted_or_empty'),
                        {'ref': candidate[0]}) > 0
                assert not any(cycle('same package replayed'))
                assert not any(cycle('same package replayed twice'))
            finally:
                # Восстановление выполняется и после ошибки/неопределённого результата HTTP.
                try:
                    failures = []
                    for original in reversed(originals):
                        try:
                            if read(original['path'])[original['field']] != original['value']:
                                if original['field'] == 'Posted':
                                    action = 'Post' if original['value'] else 'Unpost'
                                    request('POST', original['path'] + '/' + action,
                                            params={'PostingModeOperational': 'false'})
                                else:
                                    patch(original['path'], original['field'], original['value'])
                            assert read(original['path'])[original['field']] == original['value']
                        except Exception as error:
                            failures.append(f"{original['field']}: {error}")
                    assert not failures, f'Restore failed; backup={backup}: {failures}'
                    print('OData original fields verified restored')
                finally:
                    repl.close()
    finally:
        session.close()
        reset_stop()
        try:
            with engine.begin() as conn:
                conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        finally:
            engine.dispose()