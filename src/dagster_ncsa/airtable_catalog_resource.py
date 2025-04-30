from __future__ import annotations

from typing import Any

from dagster import ConfigurableResource
from pyairtable import Api
from pyairtable.formulas import match

from .models import TableEntry


class AirTableCatalogResource(ConfigurableResource):
    """
    Dagster resource for interacting with the Airtable-based Catalog API.

    NOTE: Due to the implementation of connecting to the tables, this resource
    won't work with EnvVar in the config. You need to use EnvVar.get_value()
    to load the env vars at instantiation time.
    """

    api_key: str = "XXXX"
    base_id: str = ""
    table_id: str = ""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._base = self.api.base(self.base_id)
        self._tables_table = self._base.table("Tables")
        self._catalogs_table = self._base.table("Catalogs")
        self._schemas_table = self._base.table("Schemas")

    def get_schema(self):
        """Get all tables from Airtable"""
        api = Api(self.api_key)
        table = api.table(self.base_id, self.table_id)
        return table.schema()

    @property
    def api(self):
        return Api(self.api_key)

    def lookup_catalog(self, catalog: str) -> dict[str, Any]:
        """Lookup a catalog in the table"""
        return self._catalogs_table.first(formula=match({"Catalog": catalog}))

    def lookup_schema(self, catalog: dict, schema: str) -> dict[str, Any]:
        return self._schemas_table.first(
            formula=match(
                {"CatalogID": catalog["fields"]["CatalogID"], "Schema": schema}
            )
        )

    def create_table_record(self, entry: TableEntry):
        """
        Create a record in the table using a TableEntry instance

        Args:
            entry: A TableEntry instance containing the table information
        """
        catalog_rec = self.lookup_catalog(entry.catalog)
        schema_rec = self.lookup_schema(catalog_rec, entry.schema_name)

        # Prepare record data
        record_data = {
            "SchemaID": [schema_rec["id"]],
            "TableName": entry.table,
            "Name": entry.name,
            "DeltaTablePath": entry.deltalake_path,
        }

        # Add optional fields if provided
        if entry.description is not None:
            record_data["Description"] = entry.description

        if entry.license_name is not None:
            record_data["License"] = entry.license_name

        if entry.pub_date is not None:
            record_data["PublicationDate"] = entry.pub_date.strftime("%Y-%m-%d")

        # Create the record
        self._tables_table.create(record_data)
