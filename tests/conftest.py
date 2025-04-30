from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from dagster_ncsa.models import TableEntry
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


@pytest.fixture
def airtable_resource():
    return AirTableCatalogResource(
        api_key="123-567", base_id="baseID", table_id="table"
    )


@pytest.fixture
def table_entry():
    return TableEntry(
        catalog="PublicHealth",
        schema_name="sdoh",
        table="vdgb_f9s3",
        name="Table of Gross Cigarette Tax Revenue Per State (Orzechowski and Walker Tax Burden on Tobacco)",
        deltalake_path="s3://sdoh-public/delta/data.cdc.gov/vdgb-f9s3/",
        description="1970-2019. Orzechowski and Walker. Tax Burden on Tobacco",
        license_name="Open Data Commons Attribution License",
        pub_date=datetime.fromtimestamp(1616406567),
    )
