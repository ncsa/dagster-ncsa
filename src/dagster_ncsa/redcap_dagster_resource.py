from typing import Any, Generator
import contextlib
from dagster import ConfigurableResource
from redcap import Project


class RedCapClientWrapper:
    """A wrapper for interacting with the RedCap API using pyCap."""

    def __init__(self, token: str, url: str):
        """
        Initialize the RedCapClientWrapper.

        :param token: The API token for authentication.
        :param url: The base URL of the RedCap API.
        """
        self.project = Project(url, token)

    def fetch_metadata(self, content: str, **kwargs) -> dict[str, Any]:
        """
        Fetch metadata from the RedCap API.

        :param content: The type of content to fetch.
        :param kwargs: Additional parameters for the API request.
        :return: The response from the API as a dictionary.
        """
        if content == "metadata":
            return self.project.export_metadata(**kwargs)
        raise ValueError(f"Unsupported content type: {content}")

    def export_records(self, **kwargs) -> list[dict[str, Any]]:
        """
        Export records from the RedCap API.

        :param kwargs: Additional parameters for the API request.
        :return: A list of records as dictionaries.
        """
        return self.project.export_records(**kwargs)

    def import_records(self, records: list[dict[str, Any]], **kwargs) -> dict[str, Any]:
        """
        Import records into the RedCap API.

        :param records: A list of records to import.
        :param kwargs: Additional parameters for the API request.
        :return: The response from the API as a dictionary.
        """
        return self.project.import_records(records, **kwargs)


class RedCapResource(ConfigurableResource):
    """A resource for managing interactions with the RedCap API using pyCap."""

    token: str
    url: str

    @contextlib.contextmanager
    def get_client(self) -> Generator[RedCapClientWrapper, None, None]:
        """
        Get a RedCapClientWrapper instance.

        :yield: A RedCapClientWrapper instance.
        """
        client = RedCapClientWrapper(self.token, self.url)
        try:
            yield client
        finally:
            pass

    def fetch_metadata(self, content: str, **kwargs) -> dict[str, Any]:
        """
        Fetch metadata using the RedCap client.

        :param content: The type of content to fetch.
        :param kwargs: Additional parameters for the API request.
        :return: The response from the API as a dictionary.
        """
        with self.get_client() as client:
            return client.fetch_metadata(content, **kwargs)

    def export_records(self, **kwargs) -> list[dict[str, Any]]:
        """
        Export records using the RedCap client.

        :param kwargs: Additional parameters for the API request.
        :return: A list of records as dictionaries.
        """
        with self.get_client() as client:
            return client.export_records(**kwargs)

    def import_records(self, records: list[dict[str, Any]], **kwargs) -> dict[str, Any]:
        """
        Import records using the RedCap client.

        :param records: A list of records to import.
        :param kwargs: Additional parameters for the API request.
        :return: The response from the API as a dictionary.
        """
        with self.get_client() as client:
            return client.import_records(records, **kwargs)
