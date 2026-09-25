"""
Субконто регистра бухгалтерии (оффлайн — ответ 1С подставляется заглушкой).

В описании движения (`_RowType`) субконто нет вообще: они живут только в виртуальной таблице
`RecordsWithExtDimensions` и приезжают СЛОТАМИ (`ExtDimensionDr1..3`). Номер слота смысла не имеет
— субконто1 счёта 10 это Номенклатура, счёта 60 Контрагенты, — поэтому слоты сворачиваются в JSON
с ключом по ВИДУ субконто. Проверяем именно свёртку, состав колонок и сшивку с пакетом изменений.
"""

import logging
from urllib.parse import unquote

import pytest

from onecdc.data_reader import DataReader
from onecdc.metadata_reader import (ACCOUNTING_REGISTER_TYPE, EXT_DIMENSIONS_FIELDS,
                                    EXT_DIMENSIONS_TYPE, MetadataObject, MetadataReader)

from conftest import FakeResponseMixin

REG = f"{ACCOUNTING_REGISTER_TYPE}_Main"
REC = "6a85159f-8ba8-11dd-89d9-00055dcfc5ca"
KIND_1 = "6a6ada17-52bb-4311-b1cc-cf7913896204"
KIND_2 = "ac901067-a86f-48d4-93e0-bc525fc3dbe0"
VALUE_1 = "e5b0e2f0-aa22-11dc-a0f4-0011d85708ff"

# Поля движения (как в _RecordType) плюс синтетические колонки субконто.
_FIELDS = {"Recorder": "Guid", "Recorder_Type": "String", "LineNumber": "Int64",
           "Period": "DateTime", "AccountDr_Key": "Guid", "Summa": "Double",
           "KolichestvoDr": "Double",
           **{field: EXT_DIMENSIONS_TYPE for field in EXT_DIMENSIONS_FIELDS.values()}}
_PRIMARY_KEY = {"Recorder": "Guid", "LineNumber": "Int64", "Recorder_Type": "String"}


def _element(line_number: str = "1", period: str = "2013-01-14T12:00:01") -> str:
    """Один <d:element> виртуальной таблицы: движение + слоты субконто + лишние поля."""
    return f"""
      <d:element>
        <d:Period>{period}</d:Period>
        <d:Recorder>{REC}</d:Recorder>
        <d:Recorder_Type>StandardODATA.Document_AvansovyjOtchet</d:Recorder_Type>
        <d:LineNumber>{line_number}</d:LineNumber>
        <d:AccountDr_Key>51817a38-e8d9-4e9b-a6d8-ae22629ba12c</d:AccountDr_Key>
        <d:Summa>1650</d:Summa>
        <d:KolichestvoDr m:null="true"/>
        <d:ExtDimensionDr1>{VALUE_1}</d:ExtDimensionDr1>
        <d:ExtDimensionDr1_Type>StandardODATA.Catalog_StatiZatrat</d:ExtDimensionDr1_Type>
        <d:ExtDimensionTypeDr1_Key>{KIND_1}</d:ExtDimensionTypeDr1_Key>
        <d:ExtDimensionDr2 m:null="true"/>
        <d:ExtDimensionTypeDr2_Key>{KIND_2}</d:ExtDimensionTypeDr2_Key>
        <d:ExtDimensionCr1>1650</d:ExtDimensionCr1>
        <d:ExtDimensionCr1_Type>Edm.Double</d:ExtDimensionCr1_Type>
        <d:ExtDimensionTypeCr1_Key>{KIND_2}</d:ExtDimensionTypeCr1_Key>
        <d:PointInTime>2013-01-14T12:00:01</d:PointInTime>
      </d:element>"""


CHART = "ChartOfCharacteristicTypes_VidySubkonto"


def _ext_urls(reader) -> list[str]:
    """Только запросы к виртуальной таблице: поиск плана видов характеристик тут ни при чём."""
    return [unquote(u) for u in reader.requested_urls if 'RecordsWithExtDimensions' in unquote(u)]


def _chart_feed(names: dict) -> str:
    """Ответ плана видов характеристик: Ref_Key + предопределённое имя каждого элемента."""
    entries = ''.join(
        '<entry><content><m:properties>'
        f'<d:Ref_Key>{ref}</d:Ref_Key><d:PredefinedDataName>{name}</d:PredefinedDataName>'
        '</m:properties></content></entry>' for ref, name in names.items())
    return ('<feed xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"'
            ' xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">'
            + entries + '</feed>')


def _result(*elements: str) -> str:
    return ('<?xml version="1.0" encoding="UTF-8"?>'
            '<d:Result xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"'
            ' xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">'
            + ''.join(elements) + '</d:Result>')


class _Response(FakeResponseMixin):
    headers: dict = {}
    reason = 'Bad Request'
    url = 'http://fake'

    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.content = text.encode()
        self.status_code = status_code
        self.ok = status_code < 400


@pytest.fixture
def reader(monkeypatch):
    """DataReader с метаданными регистра бухгалтерии; запросы в 1С перехвачены."""
    metadata = MetadataReader(odata_url="http://fake")
    metadata[REG] = MetadataObject(REG, dict(_FIELDS), dict(_PRIMARY_KEY),
                                     object_key=["Recorder", "Recorder_Type"])
    # План видов характеристик, в котором лежат виды субконто. Его приходится искать перебором:
    # в поле вида субконто голый Guid, а навигационной ссылки на владельца 1С не отдаёт.
    metadata[CHART] = MetadataObject(CHART, {"Ref_Key": "Guid"}, {"Ref_Key": "Guid"})
    metadata.is_loaded = True
    obj = DataReader(odata_url="http://fake", metadata=metadata)
    obj.exchange_message_no = 7
    obj.requested_urls = []
    obj.response_text = _result(_element())
    # Очередь подставных ответов: (код, тело). Пусто — обычный 200 с response_text.
    obj.response_queue = []

    import onecdc.data_reader as module

    # Ответы плана видов характеристик: None — вида субконто в этом плане нет, иначе
    # {Ref_Key: PredefinedDataName}.
    obj.chart_names = None
    # Код ответа плана, если он отвечает не данными: 403 — нет прав на чужой план учёта.
    obj.chart_error = None

    def fake_get(url, **kwargs):
        obj.requested_urls.append(url)
        if CHART in unquote(url):
            # Поиск плана идёт ОТБОРОМ, а не прямым адресом: и на поиск ($filter), и на чтение
            # плана ответ — feed. «Вида здесь нет» — это 200 и пустая коллекция, а не 404,
            # неотличимый от отказа веб-сервера (CDC-42).
            if obj.chart_error is not None:
                return _Response('<html>403</html>', obj.chart_error)
            return _Response(_chart_feed(obj.chart_names or {}))
        if obj.response_queue:
            status, text = obj.response_queue.pop(0)
            return _Response(text, status)
        return _Response(obj.response_text)

    monkeypatch.setattr(module.requests, 'get', fake_get)
    return obj


def _read_with_subconto(reader, times: int = 1) -> None:
    """
    Боевой путь чтения регистра с субконто: страница — НАБОР ЗАПИСЕЙ, аналитика добирается вторым
    запросом к виртуальной таблице (DataReader._fill_subconto).

    Отдельного чтения регистра из виртуальной таблицы больше нет: листать её нечем (`$skip` — 400,
    `Top` режет по строкам и обрезает набор посередине), поэтому сам регистр читается обычным
    `$skip` по наборам, а таблица нужна только за субконто.
    """
    reader.read_subconto = True
    for _ in range(times):
        reader.response_queue = [(200, _record_set_feed()), (200, reader.response_text)]
        reader.read_object(REG, key_fields=["Recorder"])


def test_ext_dimension_slots_are_folded_by_kind(reader):
    _read_with_subconto(reader)

    data = reader[REG].data
    dr = data[EXT_DIMENSIONS_FIELDS['Dr']][0]
    cr = data[EXT_DIMENSIONS_FIELDS['Cr']][0]
    # Ключ — вид субконто, а не номер слота.
    assert dr == {KIND_1: {"value": VALUE_1, "type": "Catalog_StatiZatrat"}}
    # Слот без значения (m:null) пропущен, хотя вид субконто у него пришёл.
    assert KIND_2 not in dr
    # Субконто бывает и не ссылкой — тип значения нужен именно для этого, префикс Edm. снят.
    assert cr == {KIND_2: {"value": "1650", "type": "Double"}}


def test_virtual_table_extras_do_not_become_columns(reader):
    # Виртуальная таблица шире движения: сами слоты и PointInTime в регистре не существуют,
    # и колонок под них в таблице быть не должно.
    _read_with_subconto(reader)

    columns = set(reader[REG].data)
    assert not [c for c in columns if c.startswith('ExtDimensionDr')
                or c.startswith('ExtDimensionCr') or c.startswith('ExtDimensionType')]
    assert 'PointInTime' not in columns
    assert {'Recorder', 'LineNumber', 'Summa'} <= columns


def test_changes_package_is_enriched_by_periods(reader):
    # Пакет изменений приносит набор записей БЕЗ субконто — их дочитывают по периодам пакета
    # и сшивают по (регистратор, номер строки).
    reader._get_register_records(REG, {
        "d:Recorder": REC,
        "d:Recorder_Type": "StandardODATA.Document_AvansovyjOtchet",
        "d:RecordSet": {"d:element": [
            {"d:LineNumber": "1", "d:Period": "2013-01-14T12:00:01", "d:Summa": "1650"},
            {"d:LineNumber": "2", "d:Period": "2013-01-14T12:00:01", "d:Summa": "1600"},
        ]},
    })
    assert EXT_DIMENSIONS_FIELDS['Dr'] not in reader[REG].data   # в наборе субконто нет

    reader.response_text = _result(_element(line_number="1"))
    assert reader.fill_ext_dimensions(REG) == 1                  # один период — один запрос

    data = reader[REG].data
    # Адресно по регистратору субконто не спросить (Condition по Recorder врёт), поэтому
    # спрашиваем период — он общий для обеих строк, а сшивка идёт по номеру строки.
    condition = _ext_urls(reader)[-1]
    assert "/RecordsWithExtDimensions(" in condition   # имя функции — сегмент пути, не %2F
    assert "Period eq datetime''2013-01-14T12:00:01''" in condition
    assert data[EXT_DIMENSIONS_FIELDS['Dr']][0] == {KIND_1: {"value": VALUE_1,
                                                             "type": "Catalog_StatiZatrat"}}
    # Строке без пары достаётся пустой JSON, а не NULL: «субконто нет» != «не читали».
    assert data[EXT_DIMENSIONS_FIELDS['Dr']][1] == {}
    assert len(data[EXT_DIMENSIONS_FIELDS['Cr']]) == reader[REG].data_length


def _package(reader, periods: list[str]) -> None:
    """Кладёт в reader набор записей на заданные периоды — по одному движению на период."""
    reader._get_register_records(REG, {
        "d:Recorder": REC,
        "d:Recorder_Type": "StandardODATA.Document_AvansovyjOtchet",
        "d:RecordSet": {"d:element": [
            {"d:LineNumber": str(i + 1), "d:Period": period, "d:Summa": "1"}
            for i, period in enumerate(periods)
        ]},
    })


def test_periods_are_split_by_url_segment_budget(reader):
    # Параметры функции лежат В АДРЕСЕ, и режет их http.sys своим лимитом на длину одного
    # сегмента URL (260 символов) — раньше, чем IIS дойдёт до maxUrl. Проверено на живой 1С:
    # пять периодов проходят, шесть дают 400 «Invalid URL». Поэтому периоды бьются по бюджету.
    _package(reader, [f"2013-01-{day:02d}T12:00:00" for day in range(1, 13)])

    reader.fill_ext_dimensions(REG)

    segments = [url.split('/RecordsWithExtDimensions', 1)[1] for url in _ext_urls(reader)]
    assert len(segments) > 1                                    # в один запрос не влезло
    assert all(len('RecordsWithExtDimensions' + s) <= 260 for s in segments)
    # Ни один период не потерян и ни один не спрошен дважды.
    asked = sum(s.count('Period eq datetime') for s in segments)
    assert asked == 12


def test_url_too_long_halves_the_budget_and_retries(reader):
    # Страховка на случай, когда лимит урезан ниже умолчания: сервер отвечает 400 с приметой
    # в теле, бюджет делится и ТА ЖЕ пачка повторяется короче.
    _package(reader, [f"2013-01-{day:02d}T12:00:00" for day in range(1, 5)])
    reader.response_queue = [(400, '<h2>Bad Request - Invalid URL</h2>')]

    reader.fill_ext_dimensions(REG)

    assert reader._ext_dimensions_segment_limit == 130           # 260 // 2
    # Первый запрос отвергнут, дальше пачки короче — но все четыре периода спрошены.
    segments = _ext_urls(reader)
    assert len(segments) > 2
    assert sum(s.count('Period eq datetime') for s in segments[1:]) == 4


def test_other_400_is_not_treated_as_a_long_url(reader):
    # Иначе любая ошибка 400 молча урезала бы бюджет до дна вместо внятного отказа.
    _package(reader, [f"2013-01-{day:02d}T12:00:00" for day in range(1, 5)])
    reader.response_queue = [(400, 'Неправильный запрос')]

    with pytest.raises(Exception):
        reader.fill_ext_dimensions(REG)
    assert reader._ext_dimensions_segment_limit == 260


def test_kind_key_is_the_predefined_name(reader):
    # Ключ JSON — вид субконто, и читать его должно быть можно: Guid вида ни о чём не говорит,
    # а предопределённое имя из плана видов характеристик говорит всё.
    reader.chart_names = {KIND_1: "StatiZatrat", KIND_2: "RabotnikiOrganizatsij"}

    _read_with_subconto(reader)

    data = reader[REG].data
    assert data[EXT_DIMENSIONS_FIELDS['Dr']][0] == {
        "StatiZatrat": {"value": VALUE_1, "type": "Catalog_StatiZatrat"}}
    assert data[EXT_DIMENSIONS_FIELDS['Cr']][0] == {
        "RabotnikiOrganizatsij": {"value": "1650", "type": "Double"}}


def test_kind_without_predefined_name_stays_a_guid(reader):
    # Вид субконто, заведённый пользователем руками, предопределённого имени не имеет. Ключом у
    # него остаётся Guid — это по-прежнему рабочий вариант, вид join-ится к плану видов.
    reader.chart_names = {KIND_2: "RabotnikiOrganizatsij"}

    _read_with_subconto(reader)

    assert list(reader[REG].data[EXT_DIMENSIONS_FIELDS['Dr']][0]) == [KIND_1]


def test_missing_chart_leaves_guids_and_warns(reader, caplog):
    # План видов характеристик не нашёлся — молчать нельзя: разница видна прямо в данных.
    reader.chart_names = None

    with caplog.at_level(logging.WARNING):
        _read_with_subconto(reader)

    assert list(reader[REG].data[EXT_DIMENSIONS_FIELDS['Dr']][0]) == [KIND_1]
    assert 'JSON keys stay GUIDs' in caplog.text


def test_chart_is_read_once_per_process(reader):
    # Карта имён строится один раз: перебор планов и чтение — это сетевые запросы, а состав
    # видов субконто за прогон не меняется.
    reader.chart_names = {KIND_1: "StatiZatrat"}

    _read_with_subconto(reader, times=2)

    chart_calls = [u for u in reader.requested_urls if CHART in unquote(u)]
    assert len(chart_calls) == 2, 'поиск отбором + чтение плана, и только на первом чтении'


# --- Субконто как опция ---

def _record_set_feed() -> str:
    """Ответ 1С на чтение НАБОРА ЗАПИСЕЙ регистра: движения внутри d:RecordSet, субконто в них нет."""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom"
          xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"
          xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
      <entry>
      <category term="StandardODATA.{REG}"/>
      <content><m:properties>
        <d:Recorder>{REC}</d:Recorder>
        <d:Recorder_Type>StandardODATA.Document_AvansovyjOtchet</d:Recorder_Type>
        <d:RecordSet m:type="Collection(StandardODATA.{REG}_RecordType)">
          <d:element>
            <d:LineNumber>1</d:LineNumber>
            <d:Period>2013-01-14T12:00:01</d:Period>
            <d:AccountDr_Key>51817a38-e8d9-4e9b-a6d8-ae22629ba12c</d:AccountDr_Key>
            <d:Summa>1650</d:Summa>
            <d:KolichestvoDr m:null="true"/>
          </d:element>
        </d:RecordSet>
      </m:properties></content></entry>
    </feed>"""


def test_subconto_are_not_read_by_default(reader):
    """
    По умолчанию субконто НЕ читаются: чтение регистра — один запрос, к виртуальной таблице не
    ходим вовсе.

    Так решено потому, что дёшево её читать нельзя: `$skip` она не поддерживает, а `Top` режет по
    строкам, обрезая набор регистратора посередине, и отдаёт произвольное подмножество (проверено
    на живой 1С). Остаётся перечислять периоды — на большом регистре это много запросов.
    """
    reader.response_text = _record_set_feed()
    assert reader.read_subconto is False, 'умолчание должно быть «не читать»'

    reader.read_object(REG, key_fields=["Recorder"])

    assert _ext_urls(reader) == [], 'к виртуальной таблице ходить не должны'
    assert "ExtDimensionsDr" not in reader[REG].data, 'колонок субконто быть не должно'


def test_subconto_are_read_when_asked(reader):
    # Та же страница, но с включённой опцией: субконто добираются вторым запросом по периодам.
    reader.read_subconto = True
    reader.chart_names = {KIND_1: "StatiZatrat"}
    reader.response_queue = [(200, _record_set_feed()), (200, _result(_element()))]

    reader.read_object(REG, key_fields=["Recorder"])

    assert _ext_urls(reader), 'виртуальную таблицу обязаны спросить'
    assert reader[REG].data["ExtDimensionsDr"][0] == {
        "StatiZatrat": {"value": VALUE_1, "type": "Catalog_StatiZatrat"}}


def test_an_empty_numeric_of_a_record_set_becomes_zero(reader):
    """
    Пустой ресурс регистра — это НОЛЬ, а не NULL, и в наборе записей тоже.

    1С отдаёт незаполненное число элементом `<d:KolichestvoDr m:null="true"/>`, а такую форму
    разобрать однозначно нельзя — поле из записи выпадает. Раньше выравнивание на ноль стояло
    только на пути виртуальной таблицы, и комментарий рядом утверждал, что набор записей отдаёт
    ноль сам. Измерение на живой 1С показало обратное: `m:null` приходит из ОБОИХ источников.
    Значит пакет изменений писал NULL, полная выгрузка — ноль, и при работающем CDC они
    переписывали бы строку по очереди без конца.

    NULL в колонке ресурса занят другим смыслом — им помечается погашенная строка.
    """
    reader.response_text = _record_set_feed()

    reader.read_object(REG, key_fields=["Recorder"])

    assert reader[REG].data["KolichestvoDr"] == [0]


def test_a_chart_that_refuses_is_skipped_instead_of_killing_the_read(reader, caplog):
    """
    Прав на чужой план учёта может не быть, и его 403 — не повод ронять чтение всего регистра:
    не найдём нужный план, останемся с GUID-ключами, а это описанный штатный режим.

    Раньше любой код, кроме 404, уходил в raise_for_status и валил страницу или пакет — причём
    на каждом прогоне, пока карта не построена.
    """
    reader.chart_error = 403

    with caplog.at_level(logging.WARNING):
        _read_with_subconto(reader)

    assert list(reader[REG].data[EXT_DIMENSIONS_FIELDS['Dr']][0]) == [KIND_1], 'данные прочитаны'
    assert 'skipping this chart' in caplog.text
    assert 'JSON keys stay GUIDs' in caplog.text


def test_the_chart_is_looked_up_by_filter_not_by_direct_address(reader):
    """
    Прямой адрес отвечает на «нет такого» кодом 404, и отличить его от 404 веб-сервера можно
    только по телу — которое IIS подменяет (измерено на демо бухгалтерии). У отбора такого
    вопроса нет: 200 и пустая коллекция.
    """
    reader.chart_names = {KIND_1: "StatiZatrat"}

    _read_with_subconto(reader)

    lookup = next(unquote(u) for u in reader.requested_urls if CHART in unquote(u))
    assert "$filter=Ref_Key eq guid'" in lookup
    assert f"{CHART}(guid'" not in lookup, 'прямой адрес больше не используется'
