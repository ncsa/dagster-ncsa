from __future__ import annotations

import json
from typing import Any

from botocore.exceptions import ClientError
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

    def save_json_object(self, bucket: str, key: str, data: dict[str, Any]) -> None:
        self.get_client().put_object(Bucket=bucket, Key=key, Body=json.dumps(data))

    def delete_directory(self, bucket_name: str, directory_path: str) -> dict[str, Any]:
        """
        Delete all files in a directory in an S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket.
            directory_path (str): The directory path in the bucket (without leading slash).
                                  Make sure it ends with a '/' if it's a directory.

        Returns:
            dict: Information about the operation result.
        """
        # Ensure directory path ends with a slash
        if not directory_path.endswith("/"):
            directory_path += "/"

        # Initialize S3 client
        s3_client = self.get_client()

        try:
            # List all objects in the directory
            paginator = s3_client.get_paginator("list_objects_v2")
            objects_to_delete = []

            # Use pagination to handle large directories
            for page in paginator.paginate(Bucket=bucket_name, Prefix=directory_path):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        objects_to_delete.append({"Key": obj["Key"]})

            # If no objects found
            if not objects_to_delete:
                return {
                    "success": True,
                    "message": f"No objects found in {directory_path}",
                    "deleted_count": 0,
                }

            # Delete the objects
            response = s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": objects_to_delete, "Quiet": False},
            )

            return {
                "success": True,
                "message": f"Successfully deleted {len(objects_to_delete)} objects from {directory_path}",
                "deleted_count": len(objects_to_delete),
                "deleted_objects": [obj["Key"] for obj in objects_to_delete],
                "response": response,
            }

        except ClientError as e:
            return {"success": False, "message": f"Error: {e!s}", "error": e}

    def list_files(
        self, bucket_name, directory_path: str, extension: str = ".csv"
    ) -> list[str]:
        """
        List all files in a directory in an S3 bucket with a specific extension.
        :param bucket_name:
        :param directory_path:
        :param extension: Should start with a period. Default is '.csv'
        :return:
        """
        if not directory_path.endswith("/"):
            directory_path += "/"

        # Initialize S3 client
        s3_client = self.get_client()

        try:
            # List all objects in the directory
            paginator = s3_client.get_paginator("list_objects_v2")
            objects = []

            # Use pagination to handle large directories
            for page in paginator.paginate(Bucket=bucket_name, Prefix=directory_path):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        if obj["Key"].endswith(extension):
                            objects.append(obj["Key"])
            return objects

        except ClientError as e:
            raise e
