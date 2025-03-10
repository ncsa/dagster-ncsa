from __future__ import annotations

from datetime import datetime

import dotenv
from dagster import EnvVar

from dagster_ncsa.airtable_catalog_resource import AirTableCatalogResource


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
