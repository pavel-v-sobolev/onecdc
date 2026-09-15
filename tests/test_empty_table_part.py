"""
Опустевшая табличная часть обязана оставить НАДГРОБИЕ.

Строки табличной части заменяются группой целиком при приходе владельца (scoped-удаление по
Ref_Key), а для этого в пакете должна быть хоть одна запись этой части. Если часть опустела и
записи нет, заменять нечего — и ранее загруженные строки живут дальше со своими суммами. Поэтому на
пустую часть подставляется фиктивная запись.

Загвоздка в том, что «часть пуста» 1С кодирует ЧЕТЫРЬМЯ разными способами, и по форме значения
узнаётся только один. Поэтому часть опознаётся по МЕТАДАННЫМ — по имени «владелец_ЧастьИмени».
"""

import logging

import pytest
import xmltodict

from onecdc.data_reader import DataReader
from onecdc.metadata_reader import MetadataObject, MetadataReader

OWNER = "Document_X"
PART = "Document_X_Rows"
REF = "5e51e8e9-6821-11ec-a232-00155de3390c"


def _reader(part_in_metadata: bool = True) -> DataReader:
    metadata = MetadataReader(odata_url="http://fake")
    metadata[OWNER] = MetadataObject(OWNER, {"Ref_Key": "Guid", "DeletionMark": "Boolean"},
                                     {"Ref_Key": "Guid"})
    if part_in_metadata:
        metadata[PART] = MetadataObject(PART, {"Ref_Key": "Guid", "LineNumber": "Int64"},
                                        {"Ref_Key": "Guid", "LineNumber": "Int64"},
                                        object_key=["Ref_Key"], is_table_part=True)
    metadata.is_loaded = True
    # Поле вне метаданных иначе дёргает их перечитывание, а это сетевой запрос.
    metadata.get_metadata = lambda: None
    reader = DataReader(odata_url="http://fake", metadata=metadata)
    reader.exchange_message_no = 7
    return reader


def _read(reader: DataReader, fragment: str) -> None:
    xml = (f'<m:properties xmlns:d="d" xmlns:m="m" xmlns:xsi="xsi">'
           f'<d:Ref_Key>{REF}</d:Ref_Key><d:DeletionMark>false</d:DeletionMark>'
           f'{fragment}</m:properties>')
    # force_list — как в боевом разборе (parse_odata): без него единственная строка части
    # приходит словарём, а не списком из одного, и разбор пошёл бы другим путём.
    properties = xmltodict.parse(xml, force_list=('d:element',))['m:properties']
    reader.read_data_entries([{"category": {"@term": f"StandardODATA.{OWNER}"},
                               "content": {"m:properties": properties}}])


EMPTY_FORMS = [
    pytest.param('<d:Rows m:type="Collection(StandardODATA.Document_X_Rows)"/>',
                 id='m:type=Collection — единственная форма в записанных ответах 8.5'),
    pytest.param('<d:Rows xsi:nil="true"/>', id='xsi:nil'),
    pytest.param('<d:Rows/>', id='пустой элемент'),
    pytest.param('<d:Rows m:null="true"/>', id='m:null'),
]


@pytest.mark.parametrize("fragment", EMPTY_FORMS)
def test_every_empty_form_leaves_a_tombstone(fragment):
    reader = _reader()

    _read(reader, fragment)

    assert reader[PART].data_length == 1, 'надгробия нет — старые строки части останутся живыми'
    assert reader[PART].data["is_deleted_or_empty"] == [True]
    assert [str(v) for v in reader[PART].data["Ref_Key"]] == [REF]


@pytest.mark.parametrize("fragment", EMPTY_FORMS)
def test_an_empty_table_part_never_becomes_a_column(fragment):
    """
    Обратная сторона: часть не должна протечь в колонку владельца.

    Пустой элемент `<d:Rows/>` после CDC-01 разбирается как пустой СКАЛЯР, и до починки он заводил
    владельцу колонку Rows — да ещё и перечитывал метаданные, не найдя её там.
    """
    reader = _reader()

    _read(reader, fragment)

    assert "Rows" not in reader[OWNER].data


def test_a_part_absent_from_metadata_is_still_recognised_by_its_shape():
    """
    Запасной путь: часть появилась между чтением метаданных и приходом пакета.

    Опознать её по имени тогда нечем — объекта в метаданных ещё нет. Форма `Collection(...)`
    узнаётся сама, как и раньше, а метаданные перечитываются по ходу разбора строк.
    """
    reader = _reader(part_in_metadata=False)
    # Так ведёт себя настоящее перечитывание: объект в метаданных появляется.
    def appear():
        reader.metadata[PART] = MetadataObject(PART, {"Ref_Key": "Guid", "LineNumber": "Int64"},
                                               {"Ref_Key": "Guid", "LineNumber": "Int64"},
                                               object_key=["Ref_Key"], is_table_part=True)
    reader.metadata.get_metadata = appear

    _read(reader, '<d:Rows m:type="Collection(StandardODATA.Document_X_Rows)">'
                  '<d:element><d:LineNumber>1</d:LineNumber></d:element></d:Rows>')

    assert reader[PART].data_length == 1
    assert "Rows" not in reader[OWNER].data


def test_rows_of_a_non_empty_part_still_arrive(caplog):
    # Проверка, что починка не съела обычный случай.
    reader = _reader()

    with caplog.at_level(logging.WARNING):
        _read(reader, '<d:Rows m:type="Collection(StandardODATA.Document_X_Rows)">'
                      '<d:element><d:LineNumber>1</d:LineNumber></d:element>'
                      '<d:element><d:LineNumber>2</d:LineNumber></d:element></d:Rows>')

    assert reader[PART].data["LineNumber"] == [1, 2]
    assert reader[PART].data[ "is_deleted_or_empty"] == [False, False]
    assert [str(v) for v in reader[PART].data["Ref_Key"]] == [REF, REF]
