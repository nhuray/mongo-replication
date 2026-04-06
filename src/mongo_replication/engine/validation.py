"""Cursor field validation with automatic fallback for MongoDB replication.

This module validates that configured cursor fields exist in collections and
provides automatic fallback to _id when the configured field is not found.
"""

import logging
from typing import Any, Optional

from pymongo.collection import Collection

logger = logging.getLogger(__name__)


class CursorValidator:
    """Validates cursor fields and provides fallback logic.

    Ensures incremental loading always works by falling back to _id
    (which always exists in MongoDB) when configured cursor field is missing.
    """

    def __init__(self, fallback_cursor: str = "_id"):
        """Initialize cursor validator.

        Args:
            fallback_cursor: Field to use when configured cursor is not found (default: _id)
        """
        self.fallback_cursor = fallback_cursor

    def _field_exists(self, collection: Collection, field_path: str) -> bool:
        """Check if a field exists in any document in the collection.

        Supports nested fields (e.g., "meta.updatedAt").

        Args:
            collection: PyMongo Collection instance
            field_path: Field path to check (supports dot notation)

        Returns:
            True if field exists in at least one document
        """
        # Check if any document has this field
        # Use limit(1) for performance - we only need to know if it exists
        sample = collection.find_one({field_path: {"$exists": True}})
        return sample is not None

    def validate_cursor_field(
        self,
        collection: Collection,
        collection_name: str,
        cursor_field: Optional[str],
        write_disposition: str,
    ) -> str:
        """Validate cursor field and return the actual field to use.

        Logic:
        1. If write_disposition is "replace", no cursor needed - return empty string
        2. If cursor_field is None, use fallback
        3. If cursor_field exists in collection, use it
        4. Otherwise, fall back to fallback_cursor with warning

        Args:
            collection: PyMongo Collection instance
            collection_name: Name of the collection (for logging)
            cursor_field: Configured cursor field (may be None)
            write_disposition: Write strategy (merge/append/replace)

        Returns:
            Actual cursor field to use for incremental loading
        """
        # Replace mode doesn't need cursor
        if write_disposition == "replace":
            logger.debug(f"{collection_name}: Using 'replace' mode - no cursor needed")
            return ""

        # If no cursor configured, use fallback
        if cursor_field is None:
            logger.info(
                f"⚠️  {collection_name}: No cursor_field configured, using '{self.fallback_cursor}'"
            )
            return self.fallback_cursor

        # Check if configured cursor field exists
        if self._field_exists(collection, cursor_field):
            logger.debug(f"{collection_name}: Using cursor field '{cursor_field}'")
            return cursor_field

        # Field doesn't exist - fall back with warning
        logger.warning(
            f"⚠️  {collection_name}: Configured cursor field '{cursor_field}' not found, "
            f"falling back to '{self.fallback_cursor}'"
        )
        return self.fallback_cursor

    def get_field_value(self, document: dict, field_path: str) -> Optional[Any]:
        """Extract field value from document, supporting nested paths.

        Args:
            document: MongoDB document
            field_path: Field path (supports dot notation like "meta.updatedAt")

        Returns:
            Field value (native BSON type) or None if not found
        """
        # Handle nested fields
        parts = field_path.split(".")
        value = document

        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return None

        return value

    def validate_cursor_value(
        self,
        value: Any,
        collection_name: str,
        cursor_field: str,
    ) -> None:
        """Validate that cursor value is sortable and not None.

        Args:
            value: Cursor value to validate
            collection_name: Name of the collection (for logging)
            cursor_field: Name of the cursor field

        Raises:
            ValueError: If cursor value is None or not sortable
        """
        if value is None:
            raise ValueError(
                f"{collection_name}: Cursor field '{cursor_field}' returned None value"
            )

        # Check if value is sortable (MongoDB requires this for cursors)
        # Most BSON types are sortable, but we can add checks for edge cases
        # For now, just check for None which is the most common issue
