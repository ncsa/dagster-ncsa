from pyairtable.formulas import match

from dagster_ncsa.airtable_catalog_resource import AirTableCatalogResource


def test_lookup_catalog(mock_airtable_tables, airtable_resource):
    # Mock the return value for the first method
    mock_airtable_tables["catalogs"].first.return_value = {
        "id": "recXZiwgbjGkelVoG",
        "createdTime": "2025-03-04T06:49:22.000Z",
        "fields": {
            "Catalog": "PublicHealth",
            "Schemas": ["recc0Hl0AnR51Twn3"],
            "Tables": ["recAmy7mdcwVGIn3V"],
            "CatalogID": 1,
        },
    }

    # Call the method under test
    airtable_resource.lookup_catalog("test")

    # Assert the expected calls
    mock_airtable_tables["catalogs"].first.assert_called_with(
        formula=match({"Catalog": "test"})
    )


def test_lookup_schema(mock_airtable_tables):
    airtable = AirTableCatalogResource(
        api_key="123-567", base_id="baseID", table_id="table"
    )
    mock_airtable_tables["schemas"].first.return_value = {
        "id": "recc0Hl0AnR51Twn3",
        "createdTime": "2025-03-04T06:51:39.000Z",
        "fields": {
            "Schema": "sdoh",
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
    cat = {"fields": {"Catalog": "PublicHealth", "CatalogID": "rec42"}}
    schema = airtable.lookup_schema(cat, "sdoh")
    mock_airtable_tables["base"].assert_called_with("baseID")
    mock_airtable_tables["api"].assert_called_once()

    mock_airtable_tables["schemas"].first.assert_called_with(
        formula=match({"CatalogID": "rec42", "Schema": "sdoh"})
    )

    assert schema["fields"]["Schema"] == "sdoh"


def test_create_table_record(mock_airtable_tables, table_entry):
    # Configure the mock to return the expected SchemaID
    mock_airtable_tables["schemas"].first.return_value = {
        "id": "recc0Hl0AnR51Twn3",
        "fields": {"Schema": "sdoh"},
    }

    airtable = AirTableCatalogResource(
        api_key="123-567", base_id="baseID", table_id="table"
    )

    airtable.create_table_record(table_entry)

    # Assert the correct call was made
    mock_airtable_tables["tables"].create.assert_called_with(
        {
            "SchemaID": ["recc0Hl0AnR51Twn3"],
            "TableName": "vdgb_f9s3",
            "Name": "Table of Gross Cigarette Tax Revenue Per State (Orzechowski and Walker Tax Burden on Tobacco)",
            "Description": "1970-2019. Orzechowski and Walker. Tax Burden on Tobacco",
            "DeltaTablePath": "s3://sdoh-public/delta/data.cdc.gov/vdgb-f9s3/",
            "License": "Open Data Commons Attribution License",
            "PublicationDate": "2021-03-22",
        }
    )
