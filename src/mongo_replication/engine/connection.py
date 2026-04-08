"""Connection management for MongoDB with retry logic.

This module provides connection pooling and automatic retry logic for both source
and destination MongoDB instances using exponential backoff.
"""

import logging
from typing import Optional

from pymongo import MongoClient
from pymongo.database import Database
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages MongoDB connections with retry logic and connection pooling.

    Uses tenacity for exponential backoff on connection failures:
    - 3 retry attempts
    - Delays: 2s, 4s, 8s (exponential backoff)
    - Retries only on transient connection errors
    """

    def __init__(
        self,
        source_uri: str,
        dest_uri: str,
        source_db_name: str,
        dest_db_name: str,
    ):
        """Initialize connection manager.

        Args:
            source_uri: MongoDB connection URI for source
            dest_uri: MongoDB connection URI for destination
            source_db_name: Source database name
            dest_db_name: Destination database name

        Raises:
            ValueError: If source and destination point to the same database
        """
        self.source_uri = source_uri
        self.dest_uri = dest_uri
        self.source_db_name = source_db_name
        self.dest_db_name = dest_db_name

        # Validate that source and destination are different
        self._validate_different_databases()

        # Connection instances (lazy initialization)
        self._source_client: Optional[MongoClient] = None
        self._dest_client: Optional[MongoClient] = None

    def _validate_different_databases(self) -> None:
        """Validate that source and destination point to different databases.

        Raises:
            ValueError: If source and destination URIs point to the same database
        """
        # Parse host/port from URIs
        source_normalized = self._normalize_uri(self.source_uri)
        dest_normalized = self._normalize_uri(self.dest_uri)

        # Check if same host and same database name
        if source_normalized == dest_normalized and self.source_db_name == self.dest_db_name:
            raise ValueError(
                f"Source and destination cannot point to the same database. "
                f"Both are configured to use '{self.source_db_name}' on '{source_normalized}'. "
                f"Please use different databases or hosts to prevent data corruption."
            )

    @staticmethod
    def _normalize_uri(uri: str) -> str:
        """Normalize MongoDB URI for comparison (extract host/port).

        Args:
            uri: MongoDB connection URI

        Returns:
            Normalized host:port string
        """
        # Remove mongodb:// or mongodb+srv:// prefix
        if uri.startswith("mongodb+srv://"):
            uri = uri[14:]
        elif uri.startswith("mongodb://"):
            uri = uri[10:]

        # Remove credentials (user:pass@)
        if "@" in uri:
            uri = uri.split("@", 1)[1]

        # Remove database name and query parameters
        if "/" in uri:
            uri = uri.split("/", 1)[0]
        if "?" in uri:
            uri = uri.split("?", 1)[0]

        return uri.strip()

    @retry(
        retry=retry_if_exception_type((ConnectionFailure, ServerSelectionTimeoutError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _connect_source(self) -> MongoClient:
        """Connect to source MongoDB with retry logic.

        Returns:
            Connected MongoClient instance

        Raises:
            ConnectionFailure: After 3 failed attempts
        """
        logger.info("Connecting to source MongoDB...")
        client = MongoClient(self.source_uri, serverSelectionTimeoutMS=5000)

        # Test connection with ping
        client.admin.command("ping")
        logger.info("✅ Connected to source MongoDB")

        return client

    @retry(
        retry=retry_if_exception_type((ConnectionFailure, ServerSelectionTimeoutError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _connect_dest(self) -> MongoClient:
        """Connect to destination MongoDB with retry logic.

        Returns:
            Connected MongoClient instance

        Raises:
            ConnectionFailure: After 3 failed attempts
        """
        logger.info("Connecting to destination MongoDB...")
        client = MongoClient(self.dest_uri, serverSelectionTimeoutMS=5000)

        # Test connection with ping
        client.admin.command("ping")
        logger.info("✅ Connected to destination MongoDB")

        return client

    def get_source_client(self) -> MongoClient:
        """Get source MongoDB client (lazy initialization).

        Returns:
            Connected MongoClient instance
        """
        if self._source_client is None:
            self._source_client = self._connect_source()
        return self._source_client

    def get_dest_client(self) -> MongoClient:
        """Get destination MongoDB client (lazy initialization).

        Returns:
            Connected MongoClient instance
        """
        if self._dest_client is None:
            self._dest_client = self._connect_dest()
        return self._dest_client

    def get_source_db(self) -> Database:
        """Get source database instance.

        Returns:
            PyMongo Database instance
        """
        return self.get_source_client()[self.source_db_name]

    def get_dest_db(self) -> Database:
        """Get destination database instance.

        Returns:
            PyMongo Database instance
        """
        return self.get_dest_client()[self.dest_db_name]

    def close_all(self) -> None:
        """Close all MongoDB connections."""
        if self._source_client is not None:
            logger.info("Closing source MongoDB connection")
            self._source_client.close()
            self._source_client = None

        if self._dest_client is not None:
            logger.info("Closing destination MongoDB connection")
            self._dest_client.close()
            self._dest_client = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensure connections are closed."""
        self.close_all()
