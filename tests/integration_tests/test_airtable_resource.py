from __future__ import annotations

from datetime import datetime

import dotenv
import pytest
from dagster import EnvVar

from dagster_ncsa.airtable_catalog_resource import AirTableCatalogResource


@pytest.fixture
def airtable_resource():
    dotenv.load_dotenv(".env")
    return AirTableCatalogResource(
        api_key=EnvVar("AIRTABLE_API_KEY").get_value(),
        base_id=EnvVar("AIRTABLE_BASE_ID").get_value(),
        table_id=EnvVar("AIRTABLE_TABLE_ID").get_value(),
    )


def test_lookup_catalog(airtable_resource):
    print(airtable_resource.lookup_catalog("PublicHealth"))


def test_lookup_schema(airtable_resource):
    catalog = airtable_resource.lookup_catalog("PublicHealth")
    print(airtable_resource.lookup_schema(catalog, "sdoh"))


def test_create_table():
    dotenv.load_dotenv(".env")

    bucket_name = "sdoh-public"
    delta_path = f"s3://{bucket_name}/delta/data.cdc.gov/vdgb-f9s3/"
    airtable = AirTableCatalogResource(
        api_key=EnvVar("AIRTABLE_API_KEY").get_value(),
        base_id=EnvVar("AIRTABLE_BASE_ID").get_value(),
        table_id=EnvVar("AIRTABLE_TABLE_ID").get_value(),
    )

    airtable.create_table_record(
        catalog="PublicHealth",
        schema="sdoh",
        name="Table of Gross Cigarette Tax Revenue Per State (Orzechowski and Walker Tax Burden on Tobacco)",
        table="vdgb_f9s3",
        description="1970-2019. Orzechowski and Walker. Tax Burden on Tobacco",
        deltalake_path=delta_path,
        license_name="Open Data Commons Attribution License",
        pub_date=datetime.fromtimestamp(1616406567),
    )
