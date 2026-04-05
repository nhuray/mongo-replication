"""Configuration management for MongoDB replication."""

from .loader import (
    CollectionConfig,
    ReplicationConfig,
    get_collection_config,
    get_mongodb_connection_string,
)

__all__ = [
    "CollectionConfig",
    "ReplicationConfig",
    "get_collection_config",
    "get_mongodb_connection_string",
]
