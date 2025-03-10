from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from dagster_ncsa import S3ResourceNCSA


@patch("dagster_ncsa.s3_resource_ncsa.S3Resource.get_client")
def test_read_s3_object(mock_client):
    doc = {"hey": "there"}
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(doc)
    mock_client.return_value.get_object.return_value = {"Body": mock_body}
    s3 = S3ResourceNCSA()
    doc2 = s3.read_json_object("bucket", "key")
    assert doc == doc2
    mock_client.return_value.get_object.assert_called_with(Bucket="bucket", Key="key")
    mock_body.read.assert_called_once()


@patch("dagster_ncsa.s3_resource_ncsa.S3Resource.get_client")
def test_save_json_object(mock_client):
    doc = {"hey": "there"}
    s3 = S3ResourceNCSA()
    s3.save_json_object("bucket", "key", doc)
    mock_client.return_value.put_object.assert_called_with(
        Bucket="bucket", Key="key", Body=json.dumps(doc)
    )


@patch("dagster_ncsa.s3_resource_ncsa.S3Resource.get_client")
def test_delete_directory(mock_get_client):
    client = mock_get_client.return_value
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {"Contents": [{"Key": "key1"}, {"Key": "key2"}]},
        {"Contents": [{"Key": "key3"}, {"Key": "key4"}]},
    ]

    client.get_paginator.return_value = paginator
    s3 = S3ResourceNCSA()
    s3.delete_directory("bucket", "/directory/path")

    paginator.paginate.assert_called_with(Bucket="bucket", Prefix="/directory/path/")
    client.delete_objects.assert_called_with(
        Bucket="bucket",
        Delete={
            "Objects": [
                {"Key": "key1"},
                {"Key": "key2"},
                {"Key": "key3"},
                {"Key": "key4"},
            ],
            "Quiet": False,
        },
    )


@patch("dagster_ncsa.s3_resource_ncsa.S3Resource.get_client")
def test_list_csv_files(mock_get_client):
    client = mock_get_client.return_value

    paginator = MagicMock()
    paginator.paginate.return_value = [
        {"Contents": [{"Key": "key1.csv"}, {"Key": "key2"}]},
        {"Contents": [{"Key": "key3"}, {"Key": "key4.csv"}]},
    ]
    client.get_paginator.return_value = paginator

    s3 = S3ResourceNCSA()
    csv_files = s3.list_files("bucket", "/directory/path", ".csv")

    paginator.paginate.assert_called_with(Bucket="bucket", Prefix="/directory/path/")
    assert len(csv_files) == 2
    assert csv_files == ["key1.csv", "key4.csv"]
