from __future__ import annotations

from datetime import datetime
from typing import Any

from dagster import ConfigurableResource
from pyairtable import Api
from pyairtable.formulas import match


class AirTableCatalogResource(ConfigurableResource):
    """Dagster resource for interacting Airtable-based Catalog API"""

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
        return self._catalogs_table.first(formula=match({"CatalogName": catalog}))

    def lookup_schema(self, catalog: dict, schema: str) -> dict[str, Any]:
        return self._schemas_table.first(
            formula=match(
                {"CatalogName": catalog["fields"]["CatalogID"], "SchemaName": schema}
            )
        )

    def create_table_record(
        self,
        catalog: str,
        schema: str,
        table: str,
        name: str,
        deltalake_path: str,
        description: str,
        license_name: str,
        pub_date: datetime,
    ):
        """Create a record in the table"""
        catalog_rec = self.lookup_catalog(catalog)
        schema_rec = self.lookup_schema(catalog_rec, schema)

        self._tables_table.create(
            {
                "Catalog": [catalog_rec["id"]],
                "Schema": [schema_rec["id"]],
                "TableName": table,
                "Name": name,
                "Description": description,
                "DeltaTablePath": deltalake_path,
                "License": license_name,
                "PublicationDate": pub_date.strftime("%Y-%m-%d"),
            }
        )
