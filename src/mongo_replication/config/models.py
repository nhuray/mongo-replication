"""Configuration models for MongoDB replication.

This module defines the configuration schema with two main sections:
1. scan: Configuration for PII discovery and collection analysis
2. replication: Configuration for the actual replication process
"""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional


# =============================================================================
# SCAN CONFIG MODELS
# =============================================================================


@dataclass
class ScanDiscoveryConfig:
    """Configuration for collection discovery during scan."""

    include_patterns: List[str] = field(default_factory=list)
    """Regex patterns for collections to include (empty = include all)."""

    exclude_patterns: List[str] = field(default_factory=list)
    """Regex patterns for collections to exclude."""


@dataclass
class ScanPIIConfig:
    """Configuration for PII detection during scan."""

    enabled: bool = True
    """Whether to run PII detection."""

    confidence_threshold: float = 0.85
    """Minimum confidence score for PII detection (0.0-1.0)."""

    entity_types: List[str] = field(
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

    sample_size: int = 1000
    """Number of documents to sample per collection."""

    sample_strategy: Literal["random", "stratified"] = "stratified"
    """Sampling strategy: 'random' or 'stratified'."""

    default_strategies: Dict[str, str] = field(
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

    allowlist: List[str] = field(default_factory=list)
    """Field patterns to exclude from PII detection (e.g., 'metadata.*', '*.created_at')."""

    def __post_init__(self):
        """Validate configuration."""
        if not 0.0 <= self.confidence_threshold <= 1.0:
            raise ValueError(
                f"confidence_threshold must be between 0.0 and 1.0, got {self.confidence_threshold}"
            )

        if self.sample_size < 1:
            raise ValueError(f"sample_size must be >= 1, got {self.sample_size}")

        if self.sample_strategy not in ("random", "stratified"):
            raise ValueError(
                f"sample_strategy must be 'random' or 'stratified', got {self.sample_strategy}"
            )


@dataclass
class ScanConfig:
    """Configuration for the scan command."""

    discovery: ScanDiscoveryConfig = field(default_factory=ScanDiscoveryConfig)
    """Collection discovery configuration."""

    pii: ScanPIIConfig = field(default_factory=ScanPIIConfig)
    """PII detection configuration."""


# =============================================================================
# REPLICATION CONFIG MODELS
# =============================================================================


@dataclass
class FieldTransformConfig:
    """Configuration for a single field transformation."""

    field: str
    """Field path (supports dot notation for nested fields)."""

    type: str
    """Transformation type (currently only "regex_replace")."""

    pattern: str
    """Regex pattern to match."""

    replacement: str
    """Replacement string."""

    def __post_init__(self):
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


@dataclass
class StateConfig:
    """Configuration for replication state tracking."""

    runs_collection: str = "_rep_runs"
    """Collection name for storing job run history."""

    state_collection: str = "_rep_state"
    """Collection name for storing per-collection replication state."""


@dataclass
class DefaultsReplicationConfig:
    """Default replication settings for all collections."""

    replicate_all: bool = True
    """If true, auto-discover all collections not explicitly configured."""

    include_patterns: List[str] = field(default_factory=list)
    """Regex patterns for collections to include (empty = include all)."""

    exclude_patterns: List[str] = field(default_factory=list)
    """Regex patterns for collections to exclude."""

    write_disposition: Literal["merge", "append", "replace"] = "merge"
    """Default write strategy: merge (upsert), append (insert), replace (drop/recreate)."""

    cursor_field: Optional[str] = None
    """Field to use for incremental loading (e.g., updated_at, _id)."""

    fallback_cursor: str = "_id"
    """Field to use when cursor_field is not set or doesn't exist."""

    initial_value: str = "2020-01-01T00:00:00Z"
    """Initial cursor value for first-time replication."""

    max_parallel_collections: int = 5
    """Maximum number of collections to replicate concurrently."""

    batch_size: int = 1000
    """Number of documents to process in each batch."""

    transform_error_mode: Literal["skip", "fail"] = "skip"
    """How to handle errors during field transformations."""

    state: StateConfig = field(default_factory=StateConfig)
    """Configuration for replication state tracking."""

    def __post_init__(self):
        """Validate configuration."""
        if self.max_parallel_collections < 1:
            raise ValueError(
                f"max_parallel_collections must be >= 1, got {self.max_parallel_collections}"
            )

        if self.batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {self.batch_size}")


@dataclass
class CollectionConfig:
    """Configuration for a single collection."""

    name: str
    """Collection name."""

    cursor_field: Optional[str]
    """Field to use for incremental loading (overrides defaults)."""

    write_disposition: Literal["merge", "append", "replace"]
    """Write strategy for this collection."""

    primary_key: str
    """Primary key field (usually '_id')."""

    pii_fields: Dict[str, str]
    """Mapping of field paths to anonymization strategies."""

    match: Optional[Dict[str, Any]] = None
    """MongoDB match filter to apply during replication."""

    field_transforms: List[FieldTransformConfig] = field(default_factory=list)
    """Field transformations to apply."""

    fields_exclude: List[str] = field(default_factory=list)
    """Fields to exclude from replication."""

    transform_error_mode: Literal["skip", "fail"] = "skip"
    """Error handling mode: skip or fail."""

    def __post_init__(self):
        """Validate configuration after initialization."""
        valid_dispositions = ["merge", "append", "replace"]
        if self.write_disposition not in valid_dispositions:
            raise ValueError(
                f"Invalid write_disposition '{self.write_disposition}' for collection '{self.name}'. "
                f"Must be one of: {', '.join(valid_dispositions)}"
            )

        # Validate transform_error_mode
        valid_error_modes = ["skip", "fail"]
        if self.transform_error_mode not in valid_error_modes:
            raise ValueError(
                f"Invalid transform_error_mode '{self.transform_error_mode}' for collection '{self.name}'. "
                f"Must be one of: {', '.join(valid_error_modes)}"
            )


@dataclass
class RelationshipConfig:
    """Defines parent-child relationship between collections for cascading replication."""

    parent: str
    """Parent collection name (e.g., 'customers')."""

    child: str
    """Child collection name (e.g., 'orders')."""

    parent_field: str
    """Field in parent collection (usually '_id')."""

    child_field: str
    """Field in child collection that references parent (e.g., 'customerId')."""

    def __post_init__(self):
        """Validate relationship configuration."""
        if not self.parent or not self.child:
            raise ValueError("Relationship must specify both parent and child collections")
        if not self.parent_field or not self.child_field:
            raise ValueError("Relationship must specify both parent_field and child_field")
        if self.parent == self.child:
            raise ValueError(f"Collection '{self.parent}' cannot have a relationship with itself")


@dataclass
class ReplicationConfig:
    """Complete replication configuration."""

    collections: Dict[str, CollectionConfig]
    """Per-collection configuration."""

    defaults: Dict[str, Any]
    """Default settings for all collections (raw dict for flexibility)."""

    schema: List[RelationshipConfig] = field(default_factory=list)
    """Collection relationships for cascading replication (optional)."""

    @property
    def fallback_cursor(self) -> str:
        """Get the fallback cursor field name."""
        return self.defaults.get("fallback_cursor", "_id")

    @property
    def initial_value(self) -> str:
        """Get the initial cursor value for incremental loading."""
        return self.defaults.get("initial_value", "2020-01-01T00:00:00Z")

    @property
    def replicate_all(self) -> bool:
        """Get the replicate_all flag."""
        return self.defaults.get("replicate_all", True)

    @property
    def include_patterns(self) -> list:
        """Get include patterns for collection filtering."""
        return self.defaults.get("include_patterns", [])

    @property
    def exclude_patterns(self) -> list:
        """Get exclude patterns for collection filtering."""
        return self.defaults.get("exclude_patterns", [])

    @property
    def batch_size(self) -> int:
        """Get default batch size."""
        return self.defaults.get("batch_size", 1000)

    @property
    def max_parallel_collections(self) -> int:
        """Get max parallel collections."""
        return self.defaults.get("max_parallel_collections", 5)

    @property
    def transform_error_mode(self) -> str:
        """Get default transform error mode."""
        return self.defaults.get("transform_error_mode", "skip")


# =============================================================================
# ROOT CONFIG
# =============================================================================


# =============================================================================
# ROOT CONFIG
# =============================================================================


@dataclass
class Config:
    """Root configuration object with scan and replication sections."""

    scan: Optional[ScanConfig] = None
    """Configuration for PII scanning (optional)."""

    replication: Optional["ReplicationConfig"] = None
    """Configuration for replication (optional)."""

    def __post_init__(self):
        """Validate that at least one section is present."""
        if self.scan is None and self.replication is None:
            raise ValueError(
                "Configuration must have at least one of 'scan' or 'replication' sections"
            )
