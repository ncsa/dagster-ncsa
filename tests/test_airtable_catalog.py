from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from pyairtable.formulas import match

from dagster_ncsa.airtable_catalog_resource import AirTableCatalogResource


@pytest.fixture
def mock_airtable_tables():
    """Fixture that provides mock Airtable tables for testing."""
    with patch("dagster_ncsa.airtable_catalog_resource.Api") as mock_api:
        mock_base = MagicMock()
        mock_api.return_value.base = mock_base

        mock_tables_table = MagicMock()
        mock_schemas_table = MagicMock()
        mock_catalogs_table = MagicMock()

        mock_base.return_value.table.side_effect = [
            mock_tables_table,
            mock_catalogs_table,
            mock_schemas_table,
        ]

        yield {
            "tables": mock_tables_table,
            "catalogs": mock_catalogs_table,
            "schemas": mock_schemas_table,
            "base": mock_base,
            "api": mock_api,
        }


def test_lookup_catalog(mock_airtable_tables):
    airtable = AirTableCatalogResource(
        api_key="123-567", base_id="baseID", table_id="table"
    )
    mock_airtable_tables["catalogs"].first.return_value = {
        "id": "recXZiwgbjGkelVoG",
        "createdTime": "2025-03-04T06:49:22.000Z",
        "fields": {
            "CatalogName": "PublicHealth",
            "Schemas": ["recc0Hl0AnR51Twn3"],
            "Tables": [
                "recAmy7mdcwVGIn3V",
                "recDHP4GlUHi5l2uG",
                "rec5JFBn6Iw8Nx0SJ",
                "recezGj2i7kJWk2Ky",
                "recV8M36m3tpC7ExP",
                "recbn7Mj2atT73lOV",
                "reclRQxUUdeR4vwGf",
            ],
            "CatalogID": 1,
        },
    }
    cat = airtable.lookup_catalog("test")
    mock_airtable_tables["base"].assert_called_with("baseID")
    mock_airtable_tables["api"].assert_called_once()

    mock_airtable_tables["catalogs"].first.assert_called_with(
        formula=match({"CatalogName": "test"})
    )

    assert cat["fields"]["CatalogName"] == "PublicHealth"


def test_lookup_schema(mock_airtable_tables):
    airtable = AirTableCatalogResource(
        api_key="123-567", base_id="baseID", table_id="table"
    )
    mock_airtable_tables["schemas"].first.return_value = {
        "id": "recc0Hl0AnR51Twn3",
        "createdTime": "2025-03-04T06:51:39.000Z",
        "fields": {
            "SchemaName": "sdoh",
            "CatalogName": ["recXZiwgbjGkelVoG"],
            "Tables": [
                "recAmy7mdcwVGIn3V",
                "recDHP4GlUHi5l2uG",
                "rec5JFBn6Iw8Nx0SJ",
                "recezGj2i7kJWk2Ky",
                "recV8M36m3tpC7ExP",
                "recbn7Mj2atT73lOV",
                "reclRQxUUdeR4vwGf",
            ],
            "SchemaID": 1,
        },
    }
    cat = {"fields": {"CatalogName": "PublicHealth", "CatalogID": "rec42"}}
    schema = airtable.lookup_schema(cat, "sdoh")
    mock_airtable_tables["base"].assert_called_with("baseID")
    mock_airtable_tables["api"].assert_called_once()

    mock_airtable_tables["schemas"].first.assert_called_with(
        formula=match({"CatalogName": "rec42", "SchemaName": "sdoh"})
    )

    assert schema["fields"]["SchemaName"] == "sdoh"


def test_create_table_record(mock_airtable_tables):
    airtable = AirTableCatalogResource(
        api_key="123-567", base_id="baseID", table_id="table"
    )

    mock_airtable_tables["schemas"].first.return_value = {
        "id": "recc0Hl0AnR51Twn3",
        "createdTime": "2025-03-04T06:51:39.000Z",
        "fields": {
            "SchemaName": "sdoh",
            "CatalogName": ["recXZiwgbjGkelVoG"],
            "Tables": [
                "recAmy7mdcwVGIn3V",
                "recDHP4GlUHi5l2uG",
                "rec5JFBn6Iw8Nx0SJ",
                "recezGj2i7kJWk2Ky",
                "recV8M36m3tpC7ExP",
                "recbn7Mj2atT73lOV",
                "reclRQxUUdeR4vwGf",
            ],
            "SchemaID": 1,
        },
    }
    mock_airtable_tables["catalogs"].first.return_value = {
        "id": "recXZiwgbjGkelVoG",
        "createdTime": "2025-03-04T06:49:22.000Z",
        "fields": {
            "CatalogName": "PublicHealth",
            "Schemas": ["recc0Hl0AnR51Twn3"],
            "Tables": [
                "recAmy7mdcwVGIn3V",
                "recDHP4GlUHi5l2uG",
                "rec5JFBn6Iw8Nx0SJ",
                "recezGj2i7kJWk2Ky",
                "recV8M36m3tpC7ExP",
                "recbn7Mj2atT73lOV",
                "reclRQxUUdeR4vwGf",
            ],
            "CatalogID": 1,
        },
    }

    airtable.create_table_record(
        catalog="PublicHealth",
        schema="sdoh",
        table="vdgb_f9s3",
        name="Table of Gross Cigarette Tax Revenue Per State (Orzechowski and Walker Tax Burden on Tobacco)",
        deltalake_path="s3://sdoh-public/delta/data.cdc.gov/vdgb-f9s3/",
        description="1970-2019. Orzechowski and Walker. Tax Burden on Tobacco",
        license_name="Open Data Commons Attribution License",
        pub_date=datetime.fromtimestamp(1616406567),
    )

    mock_airtable_tables["tables"].create.assert_called_with(
        {
            "Catalog": ["recXZiwgbjGkelVoG"],
            "Schema": ["recc0Hl0AnR51Twn3"],
            "TableName": "vdgb_f9s3",
            "Name": "Table of Gross Cigarette Tax Revenue Per State (Orzechowski and Walker Tax Burden on Tobacco)",
            "Description": "1970-2019. Orzechowski and Walker. Tax Burden on Tobacco",
            "DeltaTablePath": "s3://sdoh-public/delta/data.cdc.gov/vdgb-f9s3/",
            "License": "Open Data Commons Attribution License",
            "PublicationDate": datetime.fromtimestamp(1616406567).strftime("%Y-%m-%d"),
        }
    )
