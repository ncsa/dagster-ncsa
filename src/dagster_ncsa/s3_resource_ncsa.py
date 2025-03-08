from __future__ import annotations

import json
from typing import Any

from dagster_aws.s3 import S3Resource


class S3ResourceNCSA(S3Resource):
    """
    Dagster resource extending S3Resource with additional utility methods.

    This class provides convenient methods for interacting with S3 storage,
    specifically focusing on JSON data operations.
    """

    def read_json_object(self, bucket: str, key: str) -> dict[str, Any]:
        response = self.get_client().get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read())
