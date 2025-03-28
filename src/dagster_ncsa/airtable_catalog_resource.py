from __future__ import annotations

from typing import Any, List, Optional

from dagster import ConfigurableResource
from pyairtable import Api
from pyairtable.formulas import match

from .models import TableEntry


class AirTableCatalogResource(ConfigurableResource):
    """
    Dagster resource for interacting Airtable-based Catalog API
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
        self._tags_table = self._base.table("Tags")

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

    def find_tag_by_name(self, tag_name: str) -> Optional[dict[str, Any]]:
        """
        Search for a tag by name in the Tags table
        Returns the tag record if found, None otherwise
        """
        return self._tags_table.first(formula=match({"TagName": tag_name}))

    def create_tag(self, tag_name: str) -> dict[str, Any]:
        """
        Create a new tag in the Tags table
        Returns the created tag record
        """
        return self._tags_table.create({"TagName": tag_name})

    def get_or_create_tag(self, tag_name: str) -> dict[str, Any]:
        """
        Find a tag by name or create it if it doesn't exist
        Returns the tag record (either existing or newly created)
        """
        existing_tag = self.find_tag_by_name(tag_name)
        if existing_tag:
            return existing_tag

        # Create new tag
        return self.create_tag(tag_name)

    def process_tags(self, tag_names: List[str]) -> List[str]:
        """
        Process a list of tag names and return a list of tag record IDs
        For each tag name, find or create the tag

        Args:
            tag_names: List of tag names to process

        Returns:
            List of tag record IDs
        """
        tag_ids = []
        for tag_name in tag_names:
            tag_record = self.get_or_create_tag(tag_name)
            tag_ids.append(tag_record["id"])
        return tag_ids

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

        if hasattr(entry, "tags") and entry.tags:
            tag_ids = self.process_tags(entry.tags)
            record_data["Tags"] = tag_ids

        # Create the record
        self._tables_table.create(record_data)

    def get_tables_by_tag(self, tag_name: str) -> List[dict[str, Any]]:
        """
        Get all tables associated with a specific tag

        Args:
            tag_name: The name of the tag to filter by

        Returns:
            List of table records that have the specified tag
        """
        # First, find the tag record

        try:
            # Get all tables linked to this tag
            tables = self._tables_table.all(
                formula=f"SEARCH('{tag_name}', ARRAYJOIN(Tags, ','))"
            )

            # print(f"Found {  tables} tables with tag '{tag_name}'")
            return tables

        except Exception as e:
            print(f"Error retrieving tables by tag: {type(e).__name__}: {e}")
            return []

        # Create the record
        self._tables_table.create(record_data)