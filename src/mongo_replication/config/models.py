"""Configuration models for MongoDB replication.

This module defines the configuration schema with two main sections:
1. scan: Configuration for PII discovery and collection analysis
2. replication: Configuration for the actual replication process
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional


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
class Config:
    """Root configuration object with scan and replication sections."""

    scan: Optional[ScanConfig] = None
    """Configuration for PII scanning (optional)."""

    replication: Optional[Any] = None  # Will be ReplicationConfig from loader.py
    """Configuration for replication (optional)."""

    def __post_init__(self):
        """Validate that at least one section is present."""
        if self.scan is None and self.replication is None:
            raise ValueError(
                "Configuration must have at least one of 'scan' or 'replication' sections"
            )
