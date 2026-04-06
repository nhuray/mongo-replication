"""Configuration management for MongoDB replication."""

from .manager import (
    get_collection_config,
    get_mongodb_connection_string,
)
from .models import (
    CollectionConfig,
    ReplicationConfig,
    FieldTransformConfig,
    DefaultsReplicationConfig,
)

__all__ = [
    "CollectionConfig",
    "ReplicationConfig",
    "FieldTransformConfig",
    "DefaultsReplicationConfig",
    "get_collection_config",
    "get_mongodb_connection_string",
]
