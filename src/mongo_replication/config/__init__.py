"""Configuration management for MongoDB replication."""

from .manager import (
    get_collection_config,
    get_mongodb_connection_string,
)
from .models import (
    CollectionConfig,
    ReplicationConfig,
    FieldTransformConfig,
    ReplicationDefaultsConfig,
)

__all__ = [
    "CollectionConfig",
    "ReplicationConfig",
    "FieldTransformConfig",
    "ReplicationDefaultsConfig",
    "get_collection_config",
    "get_mongodb_connection_string",
]
