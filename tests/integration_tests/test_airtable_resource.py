from __future__ import annotations

from datetime import datetime

import dotenv
import pytest
from dagster import EnvVar

from dagster_ncsa.airtable_catalog_resource import AirTableCatalogResource
from dagster_ncsa.models import TableEntry


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


def test_create_table(airtable_resource):
    try:
        bucket_name = "sdoh-public"
        delta_path = f"s3://{bucket_name}/delta/data.cdc.gov/vdgb-f9s3/"

        # Print the results of the lookups
        print("Catalog lookup:")
        catalog_rec = airtable_resource.lookup_catalog("PublicHealth")
        print(catalog_rec)

        print("Schema lookup:")
        schema_rec = airtable_resource.lookup_schema(catalog_rec, "sdoh")
        print(schema_rec)

        tags = ["NIH", "Tobacco"]

        # Test tag lookup functionality
        print("Looking up existing tags:")
        for tag in tags:
            tag_record = airtable_resource.find_tag_by_name(tag)
            print(f"Tag '{tag}': {'Found' if tag_record else 'Not found'}")

        entry = TableEntry(
            catalog="PublicHealth",
            schema_name="sdoh",
            table="vdgb_f9s3",
            name="Table of Gross Cigarette Tax Revenue Per State (Orzechowski and Walker Tax Burden on Tobacco)",
            deltalake_path=delta_path,
            description="1970-2019. Orzechowski and Walker. Tax Burden on Tobacco",
            license_name="Open Data Commons Attribution License",
            pub_date=datetime.fromtimestamp(1616406567),
            tags=tags,
        )
        print("Testing retrieval by tag:")
        tables_with_tag = airtable_resource.get_tables_by_tag("NIH")

        print(
            f"Found {[item['fields']['TableName'] for item in tables_with_tag]} tables with the 'NIH' tag"
        )

        # Create the record using the TableEntry instance
        airtable_resource.create_table_record(entry)
    except Exception as e:
        print(f"Error occurred: {type(e).__name__}: {e}")
        raise
