"""
Разбор классов регистров из `$metadata` (без сети — ответ 1С подставляется заглушкой).

Три случая, которые различаются только формой самого `$metadata`:
- независимый регистр сведений — `<Регистр>_RecordType` для него 1С не публикует вовсе;
- регистраторный регистр — `EntityType` без постфикса у него тоже есть, но описывает НАБОР записей
  регистратора, и ключ надо брать из `_RecordType`;
- регистр бухгалтерии — разбирается как регистраторный, но поля классифицируются по
  `_DrCrTurnover`, а не по `_Balance`/`_Turnover`.
"""

import pytest

from onecdc.metadata_reader import MetadataReader

INDEPENDENT = """
  <EntityType Name="InformationRegister_Prices">
    <Key><PropertyRef Name="Product_Key"/><PropertyRef Name="Period"/></Key>
    <Property Name="Product_Key" Type="Edm.Guid" Nullable="false"/>
    <Property Name="Period" Type="Edm.DateTime" Nullable="false"/>
    <Property Name="Price" Type="Edm.Double" Nullable="true"/>
  </EntityType>
"""

# Регистраторный регистр: EntityType без постфикса описывает набор записей (Recorder + RecordSet),
# поля и ключ движения лежат в _RecordType.
WITH_RECORDER = """
  <EntityType Name="AccumulationRegister_Sales">
    <Key><PropertyRef Name="Recorder"/><PropertyRef Name="Recorder_Type"/></Key>
    <Property Name="Recorder" Type="Edm.String" Nullable="false"/>
    <Property Name="Recorder_Type" Type="Edm.String" Nullable="false"/>
    <Property Name="RecordSet" Type="Collection(StandardODATA.AccumulationRegister_Sales_RowType)"
             Nullable="false"/>
  </EntityType>
  <EntityType Name="AccumulationRegister_Sales_RecordType">
    <Key><PropertyRef Name="Recorder"/><PropertyRef Name="LineNumber"/>
         <PropertyRef Name="Recorder_Type"/></Key>
    <Property Name="Recorder" Type="Edm.String" Nullable="false"/>
    <Property Name="Recorder_Type" Type="Edm.String" Nullable="false"/>
    <Property Name="LineNumber" Type="Edm.Int64" Nullable="false"/>
    <Property Name="Period" Type="Edm.DateTime" Nullable="true"/>
    <Property Name="Product_Key" Type="Edm.Guid" Nullable="true"/>
    <Property Name="Quantity" Type="Edm.Double" Nullable="true"/>
  </EntityType>
  <ComplexType Name="AccumulationRegister_Sales_Balance">
    <Property Name="Product_Key" Type="Edm.Guid" Nullable="true"/>
    <Property Name="QuantityBalance" Type="Edm.Double" Nullable="true"/>
  </ComplexType>
"""

# Регистр бухгалтерии: в движении пары Дт/Кт, в _Balance — свёрнутая одна сторона.
ACCOUNTING = """
  <EntityType Name="AccountingRegister_Main">
    <Key><PropertyRef Name="Recorder"/><PropertyRef Name="Recorder_Type"/></Key>
    <Property Name="Recorder" Type="Edm.String" Nullable="false"/>
    <Property Name="Recorder_Type" Type="Edm.String" Nullable="false"/>
    <Property Name="RecordSet" Type="Collection(StandardODATA.AccountingRegister_Main_RowType)"
             Nullable="false"/>
  </EntityType>
  <EntityType Name="AccountingRegister_Main_RecordType">
    <Key><PropertyRef Name="Recorder"/><PropertyRef Name="LineNumber"/>
         <PropertyRef Name="Recorder_Type"/></Key>
    <Property Name="Recorder" Type="Edm.String" Nullable="false"/>
    <Property Name="Recorder_Type" Type="Edm.String" Nullable="false"/>
    <Property Name="LineNumber" Type="Edm.Int64" Nullable="false"/>
    <Property Name="Period" Type="Edm.DateTime" Nullable="true"/>
    <Property Name="AccountDr_Key" Type="Edm.Guid" Nullable="true"/>
    <Property Name="AccountCr_Key" Type="Edm.Guid" Nullable="true"/>
    <Property Name="Company_Key" Type="Edm.Guid" Nullable="true"/>
    <Property Name="Summa" Type="Edm.Double" Nullable="true"/>
    <Property Name="CurrencySummaDr" Type="Edm.Double" Nullable="true"/>
    <Property Name="CurrencySummaCr" Type="Edm.Double" Nullable="true"/>
    <Property Name="Content" Type="Edm.String" Nullable="true"/>
  </EntityType>
  <ComplexType Name="AccountingRegister_Main_Balance">
    <Property Name="Account_Key" Type="Edm.Guid" Nullable="true"/>
    <Property Name="ExtDimension1" Type="Edm.String" Nullable="true"/>
    <Property Name="CurrencySummaBalance" Type="Edm.Double" Nullable="true"/>
  </ComplexType>
  <ComplexType Name="AccountingRegister_Main_DrCrTurnover">
    <Property Name="Period" Type="Edm.DateTime" Nullable="true"/>
    <Property Name="Recorder" Type="Edm.String" Nullable="true"/>
    <Property Name="AccountDr_Key" Type="Edm.Guid" Nullable="true"/>
    <Property Name="AccountCr_Key" Type="Edm.Guid" Nullable="true"/>
    <Property Name="ExtDimensionDr1" Type="Edm.String" Nullable="true"/>
    <Property Name="Company_Key" Type="Edm.Guid" Nullable="true"/>
    <Property Name="SummaTurnover" Type="Edm.Double" Nullable="true"/>
    <Property Name="CurrencySummaTurnoverDr" Type="Edm.Double" Nullable="true"/>
    <Property Name="CurrencySummaTurnoverCr" Type="Edm.Double" Nullable="true"/>
    <Property Name="Recorder_Type" Type="Edm.String" Nullable="true"/>
  </ComplexType>
"""


def _metadata(*blocks: str) -> MetadataReader:
    """MetadataReader с подставленным ответом `$metadata` (сети нет)."""
    xml = ('<?xml version="1.0" encoding="UTF-8"?>'
           '<edmx:Edmx xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx">'
           '<edmx:DataServices><Schema>' + ''.join(blocks) + '</Schema>'
           '</edmx:DataServices></edmx:Edmx>')

    class _Response:
        ok = True
        status_code = 200
        text = xml
        content = xml.encode()

    reader = MetadataReader(odata_url="http://fake")
    import onecdc.metadata_reader as module
    original = module.requests.get
    module.requests.get = lambda *a, **kw: _Response()
    try:
        reader.get_metadata()
    finally:
        module.requests.get = original
    return reader


def test_independent_information_register_is_read_without_record_type():
    # _RecordType для независимого регистра 1С не публикует — поля и ключ лежат в самом EntityType.
    metadata = _metadata(INDEPENDENT)

    obj = metadata["InformationRegister_Prices"]
    assert obj.primary_key == {"Product_Key": "Guid", "Period": "DateTime"}
    assert set(obj) == {"Product_Key", "Period", "Price"}
    # Регистратора нет — чистить группу не по чему, писаться будет upsert-ом без удаления.
    assert obj.object_key is None


def test_recorder_register_takes_key_from_record_type():
    # У регистраторного регистра EntityType без постфикса тоже есть, но это НАБОР записей:
    # ключ из него (Recorder + Recorder_Type) — не ключ движения, и RecordSet не поле записи.
    metadata = _metadata(WITH_RECORDER)

    obj = metadata["AccumulationRegister_Sales"]
    assert list(obj.primary_key) == ["Recorder", "LineNumber", "Recorder_Type"]
    assert "RecordSet" not in obj
    assert obj.object_key == ["Recorder", "Recorder_Type"]
    assert obj.resources == ["Quantity"]


def test_accounting_register_fields_are_classified_by_dr_cr_turnover():
    # По _Balance опознался бы только свёрнутый Account_Key/CurrencySummaBalance, которых в
    # движении нет: ресурсы остались бы непомеченными и не гасились бы при пометке удаления.
    metadata = _metadata(ACCOUNTING)

    obj = metadata["AccountingRegister_Main"]
    assert list(obj.primary_key) == ["Recorder", "LineNumber", "Recorder_Type"]
    assert obj.dimensions == ["AccountDr_Key", "AccountCr_Key", "Company_Key"]
    assert obj.resources == ["Summa", "CurrencySummaDr", "CurrencySummaCr"]
    # Recorder_Value — соседняя колонка для нессылочного значения составного типа
    # (COMPOSITE_VALUE_SUFFIX), к полям регистра отношения не имеет.
    # Recorder — составное поле, но соседней колонки у него нет: регистратором бывает только
    # ссылка на документ, примитива там не бывает никогда.
    assert obj.attributes == ["Content"]
