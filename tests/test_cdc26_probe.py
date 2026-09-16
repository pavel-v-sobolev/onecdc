from sqlalchemy import text
from onecdc.metadata_reader import MetadataObject, MetadataReader
from onecdc.full_load_claim import FullLoadClaim


def _reader(db, objects):
    md = MetadataReader(odata_url="http://fake", engine=db.engine, schema=db.schema)
    for name in objects:
        md[name] = MetadataObject(name, {"Ref_Key": "Guid"}, {"Ref_Key": "Guid"})
    return md


def _state(db):
    with db.engine.connect() as c:
        return {r[0]: (r[1], r[2]) for r in c.execute(text(
            f'select object_full_name, full_load_owner, last_full_load_dt '
            f'from "{db.schema}".onecdc_metadata_objects'))}


def test_probe(db):
    # Процесс 1: оба объекта опубликованы, по одному идёт выгрузка, другой уже выгружался.
    md = _reader(db, ['Catalog_A', 'Catalog_B'])
    md._sync_objects(['Catalog_A', 'Catalog_B'])
    claim = FullLoadClaim(db.engine, table_provider=lambda: md.objects_table,
                          owner='процесс-1')
    assert claim.claim('Catalog_B')
    md.mark_full_loaded('Catalog_A', rows_modified=0, minutes=1.0)
    print('\nДО:', _state(db))

    # Администратор переустанавливает состав OData — на секунды $metadata неполна.
    # Процесс перезапустился, поэтому в памяти только то, что пришло сейчас.
    fresh = _reader(db, ['Catalog_A'])
    fresh._sync_objects(['Catalog_A'])
    print('ПОСЛЕ неполной $metadata:', _state(db))

    # Объекты вернулись.
    back = _reader(db, ['Catalog_A', 'Catalog_B'])
    back._sync_objects(['Catalog_A', 'Catalog_B'])
    print('ПОСЛЕ возврата:', _state(db))
    print('захват процесса-1 жив:', 'Catalog_B' in claim.live_claims())
    claim.close()
