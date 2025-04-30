from unittest.mock import MagicMock
import pytest
from dagster_ncsa.redcap_dagster_resource import RedCapResource


@pytest.fixture
def mock_redcap_resource():
    """Fixture to mock RedCapResource."""
    mock_resource = MagicMock(spec=RedCapResource)
    mock_resource.fetch_metadata.return_value = [
        {"field_name": "specify_days", "form_name": "exposure_logging"},
        {"field_name": "ppe_type", "form_name": "exposure_logging"},
    ]
    mock_resource.export_records.return_value = [
        {"record_id": "10", "specify_days": "Monday", "ppe_type": "Gloves"}
    ]
    return mock_resource


def test_fetch_metadata_with_mock(mock_redcap_resource):  # pylint: disable=redefined-outer-name
    """Test fetching metadata using a mocked RedCapResource."""
    forms = ["exposure_logging"]
    fields = ["specify_days", "ppe_type"]
    metadata = mock_redcap_resource.fetch_metadata(
        content="metadata", forms=forms, fields=fields
    )
    assert len(metadata) == 2
    assert metadata[0]["field_name"] == "specify_days"


def test_export_records_with_mock(mock_redcap_resource):  # pylint: disable=redefined-outer-name
    """Test exporting records using a mocked RedCapResource."""
    records = ["10"]
    fields = ["specify_days", "ppe_type"]
    forms = ["exposure_logging"]
    records_data = mock_redcap_resource.export_records(
        records=records, fields=fields, forms=forms, format_type="json"
    )
    assert len(records_data) == 1
    assert records_data[0]["record_id"] == "10"
