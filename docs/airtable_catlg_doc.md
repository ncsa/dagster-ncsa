# AirTable Catalog Resource

This module provides integration between Dagster and Airtable, allowing you to manage data catalog metadata in Airtable.

## Models

### TableEntry

`TableEntry` is a Pydantic model that represents a table entry in the catalog system with enforced required fields and types.

```python
class TableEntry(BaseModel):
    catalog: str                     # The catalog name
    schema_name: str                 # The schema name
    table: str                       # The table name
    name: str                        # Display name for the table
    deltalake_path: str              # Path to the Delta Lake table
    description: Optional[str] = None  # Optional description of the table
    license_name: Optional[str] = None # Optional license name
    pub_date: Optional[datetime] = None # Optional publication date
```

The model configuration ensures:
- Field population by name
- Validation during assignment
- Descriptive error messages
- Protection against extra fields

## Resources

### AirTableCatalogResource

`AirTableCatalogResource` is a Dagster resource for interacting with an Airtable-based catalog system.

```python
class AirTableCatalogResource(ConfigurableResource):
    api_key: str = "XXXX"  # Airtable API key
    base_id: str = ""      # Airtable base ID
    table_id: str = ""     # Airtable table ID
```

#### Usage Notes

**Important**: Due to the implementation of connecting to the tables, this resource won't work with `EnvVar` in the config. You need to use `EnvVar.get_value()` to load the environment variables at instantiation time.

#### Methods

- `get_schema()`: Get all tables from Airtable
- `lookup_catalog(catalog: str)`: Lookup a catalog in the table
- `lookup_schema(catalog: dict, schema: str)`: Lookup a schema in the table
- `create_table_record(entry: TableEntry)`: Create a record in the table using a TableEntry instance

#### Example

```python
from dagster import EnvVar
from dagster_ncsa.airtable_catalog_resource import AirTableCatalogResource
from dagster_ncsa.models import TableEntry
from datetime import datetime

# Configure the resource
airtable_resource = AirTableCatalogResource(
    api_key=EnvVar.get_value("AIRTABLE_API_KEY"),
    base_id="app12345abcde",
    table_id="tbl67890fghij"
)

# Create a table entry
entry = TableEntry(
    catalog="my_catalog",
    schema_name="my_schema",
    table="my_table",
    name="My Table",
    deltalake_path="s3://my-bucket/my-table",
    description="This is my table",
    license_name="MIT",
    pub_date=datetime.now()
)

# Create the record in Airtable
airtable_resource.create_table_record(entry)
```