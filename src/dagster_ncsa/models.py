from __future__ import annotations

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class TableEntry(BaseModel):
    """
    Pydantic model representing a table entry with enforced required fields and types.

    Attributes:
        catalog: The catalog name
        schema: The schema name
        table: The table name
        name: Display name for the table
        deltalake_path: Path to the Delta Lake table
        description: Optional description of the table
        license_name: Optional license name
        pub_date: Optional publication date
    """
    catalog: str
    schema_name: str
    table: str
    name: str
    deltalake_path: str
    description: Optional[str] = None
    license_name: Optional[str] = None
    pub_date: Optional[datetime] = None

    model_config = {
        # Allow population by field name
        "populate_by_name": True,
        # Generate schema that includes all validators
        "validate_assignment": True,
        # More descriptive errors
        "extra": "forbid",
    }