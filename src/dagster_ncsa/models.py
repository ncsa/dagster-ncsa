from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class TableEntry(BaseModel):
    """
    Represents a table entry in the catalog.
    """

    catalog: str
    """The catalog name. :no-index:"""

    deltalake_path: str
    """The Delta Lake path. :no-index:"""

    description: str
    """The table description. :no-index:"""

    license_name: str
    """The license name. :no-index:"""

    name: str
    """The table name. :no-index:"""

    pub_date: datetime
    """The publication date. :no-index:"""

    schema_name: str
    """The schema name. :no-index:"""

    table: str
    """The table identifier. :no-index:"""
    model_config = {
        # Allow population by field name
        "populate_by_name": True,
        # Generate schema that includes all validators
        "validate_assignment": True,
        # More descriptive errors
        "extra": "forbid",
    }
