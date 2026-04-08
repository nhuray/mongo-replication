"""Configuration models for MongoDB replication.

This module defines the configuration schema with two main sections:
1. scan: Configuration for PII discovery and collection analysis
2. replication: Configuration for the actual replication process
"""

import re
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator, RootModel


# =============================================================================
# SCAN CONFIG MODELS
# =============================================================================


class ScanDiscoveryConfig(BaseModel):
    """Configuration for collection discovery during scan."""

    include_patterns: List[str] = Field(default_factory=list)
    """Regex patterns for collections to include (empty = include all)."""

    exclude_patterns: List[str] = Field(default_factory=list)
    """Regex patterns for collections to exclude."""


class ScanSamplingConfig(BaseModel):
    """Configuration for document sampling during scan."""

    sample_size: int = 1000
    """Number of documents to sample per collection."""

    sample_strategy: Literal["random", "stratified"] = "stratified"
    """Sampling strategy: 'random' or 'stratified'."""

    @field_validator("sample_size")
    @classmethod
    def validate_sample_size(cls, v: int) -> int:
        """Validate sample_size is at least 1."""
        if v < 1:
            raise ValueError(f"sample_size must be >= 1, got {v}")
        return v


class ScanPIIAnalysisConfig(BaseModel):
    """Configuration for PII analysis during scan."""

    enabled: bool = True
    """Whether to run PII detection."""

    confidence_threshold: float = 0.85
    """Minimum confidence score for PII detection (0.0-1.0)."""

    entity_types: List[str] = Field(
        default_factory=lambda: [
            "EMAIL_ADDRESS",
            "PHONE_NUMBER",
            "PERSON",
            "US_SSN",
            "CREDIT_CARD",
            "IBAN_CODE",
            "IP_ADDRESS",
            "URL",
        ]
    )
    """PII entity types to detect."""

    default_strategies: Dict[str, str] = Field(
        default_factory=lambda: {
            "EMAIL_ADDRESS": "fake",
            "PHONE_NUMBER": "fake",
            "PERSON": "hash",
            "US_SSN": "redact",
            "CREDIT_CARD": "redact",
            "IBAN_CODE": "redact",
            "IP_ADDRESS": "hash",
            "URL": "hash",
        }
    )
    """Default anonymization strategy per entity type."""

    allowlist: List[str] = Field(default_factory=list)
    """Field patterns to exclude from PII detection (e.g., 'metadata.*', '*.created_at')."""

    presidio_config: Optional[str] = None
    """Path to Presidio YAML configuration file for custom PII recognizers.

    This allows you to:
    - Define custom PII recognizers using regex patterns or deny-lists
    - Override default Presidio recognizers with custom settings
    - Configure NLP models and language support
    - Add domain-specific PII patterns (e.g., employee IDs, patient IDs)

    Path resolution:
    - Absolute paths: Used as-is (e.g., '/path/to/presidio.yaml')
    - Relative paths: Resolved in this order:
      1. Relative to current working directory
      2. Relative to config/ directory
      3. Default: src/mongo_replication/config/presidio.yaml

    Examples:
    - None: Use default Presidio configuration (built-in recognizers)
    - "config/custom_presidio.yaml": Use custom config in config/ directory
    - "/absolute/path/presidio.yaml": Use specific absolute path

    See docs/configuration.md for detailed examples and guidance.
    """

    @field_validator("confidence_threshold")
    @classmethod
    def validate_confidence_threshold(cls, v: float) -> float:
        """Validate confidence_threshold is between 0.0 and 1.0."""
        if not 0.0 <= v <= 1.0:
            raise ValueError(f"confidence_threshold must be between 0.0 and 1.0, got {v}")
        return v


class ScanCursorDetectionConfig(BaseModel):
    """Configuration for cursor field detection during scan."""

    cursor_fields: List[str] = Field(
        default_factory=lambda: ["updated_at", "updatedAt", "meta.updated_at", "meta.updatedAt"]
    )
    """List of cursor field candidates to try (checked in priority order during scan)."""


class ScanConfig(BaseModel):
    """Configuration for the scan command."""

    discovery: ScanDiscoveryConfig = Field(default_factory=ScanDiscoveryConfig)
    """Collection discovery configuration."""

    sampling: ScanSamplingConfig = Field(default_factory=ScanSamplingConfig)
    """Document sampling configuration."""

    pii_analysis: ScanPIIAnalysisConfig = Field(default_factory=ScanPIIAnalysisConfig)
    """PII analysis configuration."""

    cursor_detection: ScanCursorDetectionConfig = Field(default_factory=ScanCursorDetectionConfig)
    """Cursor field detection configuration."""


# =============================================================================
# REPLICATION CONFIG MODELS
# =============================================================================


class FieldTransformConfig(BaseModel):
    """Configuration for a single field transformation."""

    field: str
    """Field path (supports dot notation for nested fields)."""

    type: str
    """Transformation type (currently only "regex_replace")."""

    pattern: str
    """Regex pattern to match."""

    replacement: str
    """Replacement string."""

    @model_validator(mode="after")
    def validate_transformation(self) -> "FieldTransformConfig":
        """Validate transformation configuration."""
        if self.type != "regex_replace":
            raise ValueError(
                f"Invalid transformation type '{self.type}' for field '{self.field}'. "
                f"Currently only 'regex_replace' is supported."
            )

        # Validate regex pattern at config load time (fail-fast)
        try:
            re.compile(self.pattern)
        except re.error as e:
            raise ValueError(
                f"Invalid regex pattern '{self.pattern}' for field '{self.field}': {str(e)}"
            )

        return self


class ReplicationDiscoveryConfig(BaseModel):
    """Configuration for collection discovery during replication."""

    replicate_all: bool = True
    """If true, auto-discover and replicate all collections not explicitly excluded."""

    include_patterns: List[str] = Field(default_factory=list)
    """Regex patterns for collections to include (empty = include all)."""

    exclude_patterns: List[str] = Field(default_factory=list)
    """Regex patterns for collections to exclude."""


class ReplicationStateManagementConfig(BaseModel):
    """Configuration for replication state tracking."""

    runs_collection: str = "_rep_runs"
    """Collection name for storing job run history."""

    state_collection: str = "_rep_state"
    """Collection name for storing per-collection replication state."""


class ReplicationPerformanceConfig(BaseModel):
    """Configuration for replication performance settings."""

    max_parallel_collections: int = 5
    """Maximum number of collections to replicate concurrently."""

    batch_size: int = 1000
    """Number of documents to process in each batch."""

    @field_validator("max_parallel_collections")
    @classmethod
    def validate_max_parallel_collections(cls, v: int) -> int:
        """Validate max_parallel_collections is at least 1."""
        if v < 1:
            raise ValueError(f"max_parallel_collections must be >= 1, got {v}")
        return v

    @field_validator("batch_size")
    @classmethod
    def validate_batch_size(cls, v: int) -> int:
        """Validate batch_size is at least 1."""
        if v < 1:
            raise ValueError(f"batch_size must be >= 1, got {v}")
        return v


class ReplicationDefaultsConfig(BaseModel):
    """Default replication settings for all collections."""

    cursor_field: Optional[str] = None
    """Default cursor field to use for incremental loading."""

    cursor_fallback_field: str = "_id"
    """Field to use when cursor_field doesn't exist or no cursor_fields match."""

    cursor_initial_value: str = "2020-01-01T00:00:00Z"
    """Initial cursor value for first-time replication."""

    primary_key: str = "_id"
    """Default primary key field to use for upsert."""

    write_disposition: Literal["merge", "append", "replace"] = "merge"
    """Default write strategy: merge (upsert), append (insert), replace (drop/recreate)."""

    transform_error_mode: Literal["skip", "fail"] = "skip"
    """How to handle errors during field transformations."""


class CollectionConfig(ReplicationDefaultsConfig):
    """Configuration for a single collection.

    Inherits all default settings from ReplicationDefaultsConfig, allowing
    per-collection overrides of any default setting.
    """

    name: str
    """Collection name."""

    cursor_field: Optional[str] = None
    """Field to use for incremental loading (overrides defaults)."""

    cursor_initial_value: Optional[str] = None
    """Initial cursor value for first-time replication (overrides defaults)."""

    match: Optional[Dict[str, Any]] = None
    """MongoDB match filter to apply during replication."""

    field_transforms: List[FieldTransformConfig] = Field(default_factory=list)
    """Field transformations to apply."""

    fields_exclude: List[str] = Field(default_factory=list)
    """Fields to exclude from replication."""

    pii_anonymized_fields: Dict[str, str] = Field(default_factory=dict)
    """Mapping of field paths to anonymization strategies."""

    @field_validator("write_disposition")
    @classmethod
    def validate_write_disposition(cls, v: str, info) -> str:
        """Validate write_disposition is one of the valid values."""
        valid_dispositions = ["merge", "append", "replace"]
        if v not in valid_dispositions:
            name = info.data.get("name", "unknown")
            raise ValueError(
                f"Invalid write_disposition '{v}' for collection '{name}'. "
                f"Must be one of: {', '.join(valid_dispositions)}"
            )
        return v

    @field_validator("transform_error_mode")
    @classmethod
    def validate_transform_error_mode(cls, v: str, info) -> str:
        """Validate transform_error_mode is one of the valid values."""
        valid_error_modes = ["skip", "fail"]
        if v not in valid_error_modes:
            name = info.data.get("name", "unknown")
            raise ValueError(
                f"Invalid transform_error_mode '{v}' for collection '{name}'. "
                f"Must be one of: {', '.join(valid_error_modes)}"
            )
        return v


class CollectionsConfig(RootModel):
    """Configuration for collections to replicate."""

    root: Dict[str, CollectionConfig] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def set_name(cls, data):
        """Set the name of the collection config."""
        result: Dict[str, CollectionConfig] = {}
        if isinstance(data, dict):
            # Move the dictionary key into the 'name' field of the value dict
            for k, v in data.items():
                result[k] = CollectionConfig(name=k, **v)
            return result

        return data

    def __getitem__(self, key: str) -> CollectionConfig:
        """Allow dictionary-style access to collections."""
        return self.root[key]

    def __contains__(self, key: str) -> bool:
        """Check if a collection exists."""
        return key in self.root

    def get(self, key: str, default=None) -> Optional[CollectionConfig]:
        """Get a collection by name with optional default."""
        return self.root.get(key, default)

    def items(self):
        """Return items from the underlying dict."""
        return self.root.items()

    def keys(self):
        """Return keys from the underlying dict."""
        return self.root.keys()

    def values(self):
        """Return values from the underlying dict."""
        return self.root.values()


class SchemaRelationshipConfig(BaseModel):
    """Defines parent-child relationship between collections for cascading replication."""

    parent: str
    """Parent collection name (e.g., 'customers')."""

    child: str
    """Child collection name (e.g., 'orders')."""

    parent_field: str
    """Field in parent collection (usually '_id')."""

    child_field: str
    """Field in child collection that references parent (e.g., 'customerId')."""

    @model_validator(mode="after")
    def validate_relationship(self) -> "SchemaRelationshipConfig":
        """Validate relationship configuration."""
        if not self.parent or not self.child:
            raise ValueError("Relationship must specify both parent and child collections")
        if not self.parent_field or not self.child_field:
            raise ValueError("Relationship must specify both parent_field and child_field")
        if self.parent == self.child:
            raise ValueError(f"Collection '{self.parent}' cannot have a relationship with itself")
        return self


class ReplicationConfig(BaseModel):
    """Complete replication configuration."""

    model_config = {"protected_namespaces": ()}

    discovery: ReplicationDiscoveryConfig = Field(default_factory=ReplicationDiscoveryConfig)
    """Collection discovery configuration."""

    state_management: ReplicationStateManagementConfig = Field(
        default_factory=ReplicationStateManagementConfig
    )
    """State tracking configuration."""

    performance: ReplicationPerformanceConfig = Field(default_factory=ReplicationPerformanceConfig)
    """Performance settings."""

    defaults: ReplicationDefaultsConfig = Field(default_factory=ReplicationDefaultsConfig)
    """Default settings for all collections."""

    collections: CollectionsConfig = Field(default_factory=CollectionsConfig)
    """Per-collection configuration."""


# =============================================================================
# ROOT CONFIG
# =============================================================================


class Config(BaseModel):
    """Root configuration object with scan and replication sections."""

    scan: Optional[ScanConfig] = None
    """Configuration for PII scanning (optional)."""

    replication: Optional["ReplicationConfig"] = None
    """Configuration for replication (optional)."""

    schema_relationships: List[SchemaRelationshipConfig] = Field(default_factory=list)
    """Collection relationships for cascading replication (optional)."""

    @model_validator(mode="after")
    def validate_config(self) -> "Config":
        """Validate that at least one section is present."""
        if self.scan is None and self.replication is None:
            raise ValueError(
                "Configuration must have at least one of 'scan' or 'replication' sections"
            )
        return self
