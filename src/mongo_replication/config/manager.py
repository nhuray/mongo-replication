"""Configuration management for MongoDB replication.

This module provides functions for loading, saving, and managing configuration
files for MongoDB replication jobs. It handles both scan and replication configs,
with support for defaults merging and validation.
"""

import logging
import os
import warnings
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from mongo_replication.config.models import (
    Config,
    ScanConfig,
    ScanDiscoveryConfig,
    ScanPIIConfig,
    RelationshipConfig,
    FieldTransformConfig,
    CollectionConfig,
    ReplicationConfig,
)

logger = logging.getLogger(__name__)


def get_collection_config(
    config: ReplicationConfig, collection_name: str
) -> Optional[CollectionConfig]:
    """
    Get configuration for a specific collection.

    Args:
        config: The replication configuration
        collection_name: Name of the collection

    Returns:
        CollectionConfig if found, None otherwise
    """
    return config.collections.get(collection_name)


def get_mongodb_connection_string(env_var: str = "MONGODB_SOURCE_URI") -> str:
    """
    Get MongoDB connection string from environment variable.

    DEPRECATED: Use JobManager.get_job() instead for multi-job support.

    Args:
        env_var: Name of the environment variable containing the connection string

    Returns:
        MongoDB connection string

    Raises:
        ValueError: If environment variable is not set
    """
    connection_string = os.getenv(env_var)
    if not connection_string:
        raise ValueError(f"Environment variable {env_var} is not set")
    return connection_string


def load_config(config_path: Path) -> Config:
    """
    Load complete configuration with scan and/or replication sections.

    Configuration loading follows this precedence (later overrides earlier):
    1. Default values from defaults.yaml
    2. User configuration from the specified config file
    3. CLI arguments (handled by caller)

    This is the config format:
        scan:
          discovery: ...
          pii: ...
        replication:
          defaults: ...
          collections: ...

    Args:
        config_path: Path to YAML configuration file

    Returns:
        Config object with scan and/or replication sections

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If configuration is invalid
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    # Load system defaults
    system_defaults = load_defaults()

    with open(config_path, "r") as f:
        raw_config = yaml.safe_load(f)

    if not raw_config:
        raise ValueError(f"Configuration file is empty: {config_path}")

    # Check if this is old format (has 'defaults' or 'collections' at root level)
    is_old_format = "defaults" in raw_config or "collections" in raw_config
    has_new_sections = "scan" in raw_config or "replication" in raw_config

    if is_old_format and not has_new_sections:
        # Old format detected - migrate and warn
        warnings.warn(
            f"Configuration file {config_path} uses deprecated format. "
            "Please wrap your configuration in a 'replication:' section. "
            "See migration guide for details.",
            DeprecationWarning,
            stacklevel=2,
        )
        logger.warning(f"Auto-migrating old config format in {config_path}")

        # Migrate: wrap entire config in 'replication' section
        raw_config = {"replication": raw_config}

    # Parse scan section
    scan_config = None
    if "scan" in raw_config:
        scan_data = raw_config["scan"]
        scan_defaults = system_defaults.get("scan", {})

        # Merge defaults for scan section
        merged_scan = deep_merge(scan_defaults, scan_data)

        # Parse discovery config
        discovery_data = merged_scan.get("discovery", {})
        discovery = ScanDiscoveryConfig(
            include_patterns=discovery_data.get("include_patterns", []),
            exclude_patterns=discovery_data.get("exclude_patterns", []),
        )

        # Parse PII config
        pii_data = merged_scan.get("pii", {})
        pii_defaults = scan_defaults.get("pii", {})

        pii = ScanPIIConfig(
            enabled=pii_data.get("enabled", pii_defaults.get("enabled", True)),
            confidence_threshold=pii_data.get(
                "confidence_threshold", pii_defaults.get("confidence_threshold", 0.85)
            ),
            entity_types=pii_data.get(
                "entity_types", pii_defaults.get("entity_types", ScanPIIConfig().entity_types)
            ),
            sample_size=pii_data.get("sample_size", pii_defaults.get("sample_size", 1000)),
            sample_strategy=pii_data.get(
                "sample_strategy", pii_defaults.get("sample_strategy", "stratified")
            ),
            default_strategies=pii_data.get(
                "default_strategies",
                pii_defaults.get("default_strategies", ScanPIIConfig().default_strategies),
            ),
            allowlist=pii_data.get("allowlist", pii_defaults.get("allowlist", [])),
        )

        scan_config = ScanConfig(discovery=discovery, pii=pii)

    # Parse replication section (use existing load_config logic)
    replication_config = None
    if "replication" in raw_config:
        # Get system defaults for replication
        replication_defaults = system_defaults.get("replication", {})

        replication_data = raw_config["replication"]

        # Merge defaults: system defaults < user defaults
        user_defaults = replication_data.get("defaults", {})
        merged_defaults = deep_merge(replication_defaults, user_defaults)

        collections_dict = {}
        raw_collections = replication_data.get("collections", {})

        for collection_name, collection_data in raw_collections.items():
            # Parse field transformations
            field_transforms = []
            raw_transforms = collection_data.get("field_transforms", [])
            for transform_data in raw_transforms:
                field_transforms.append(
                    FieldTransformConfig(
                        field=transform_data["field"],
                        type=transform_data["type"],
                        pattern=transform_data["pattern"],
                        replacement=transform_data["replacement"],
                    )
                )

            collections_dict[collection_name] = CollectionConfig(
                name=collection_name,
                cursor_field=collection_data.get("cursor_field"),
                write_disposition=collection_data.get(
                    "write_disposition", merged_defaults.get("write_disposition", "merge")
                ),
                primary_key=collection_data.get("primary_key", "_id"),
                pii_fields=collection_data.get("pii_fields", {}),
                match=collection_data.get("match"),
                field_transforms=field_transforms,
                fields_exclude=collection_data.get("fields_exclude", []),
                transform_error_mode=collection_data.get(
                    "transform_error_mode", merged_defaults.get("transform_error_mode", "skip")
                ),
            )

        replication_config = ReplicationConfig(
            collections=collections_dict, defaults=merged_defaults
        )

        # Parse schema (relationships) from replication section
        schema = []
        if "schema" in replication_data:
            for rel_data in replication_data["schema"]:
                rel = RelationshipConfig(
                    parent=rel_data["parent"],
                    child=rel_data["child"],
                    parent_field=rel_data.get("parent_field", "_id"),  # Default to _id
                    child_field=rel_data["child_field"],
                )
                schema.append(rel)
        replication_config.schema = schema

    return Config(scan=scan_config, replication=replication_config)


def load_scan_config(config_path: Path) -> ScanConfig:
    """
    Load only the scan configuration section.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        ScanConfig object

    Raises:
        ValueError: If config file doesn't have scan section
    """
    rep_config = load_config(config_path)

    if rep_config.scan is None:
        raise ValueError(f"Configuration file {config_path} has no 'scan' section")

    return rep_config.scan


def load_replication_config(config_path: Path) -> ReplicationConfig:
    """
    Load only the replication configuration section.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        ReplicationConfig object

    Raises:
        ValueError: If config file doesn't have replication section
    """
    rep_config = load_config(config_path)

    if rep_config.replication is None:
        raise ValueError(f"Configuration file {config_path} has no 'replication' section")

    return rep_config.replication


def _format_yaml_value(value: Any) -> str:
    """
    Format a value for YAML output without document separators.

    Args:
        value: Value to format

    Returns:
        Formatted string representation
    """
    if value is None:
        return "null"
    elif isinstance(value, bool):
        return str(value)
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, str):
        # Quote strings that contain special characters
        if any(
            char in value
            for char in [
                '"',
                "'",
                ":",
                "#",
                "[",
                "]",
                "{",
                "}",
                ",",
                "&",
                "*",
                "!",
                "|",
                ">",
                "@",
                "`",
            ]
        ):
            # Use single quotes and escape any single quotes in the string
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        else:
            return value
    else:
        # For complex types, use yaml.dump but remove document separator
        dumped = yaml.dump(value, default_flow_style=True).strip()
        if dumped.endswith("..."):
            dumped = dumped[:-3].strip()
        return dumped


def _write_yaml_with_comments(data: Dict[str, Any], file_path: Path) -> None:
    """
    Write YAML file with helpful comments explaining each section.

    Args:
        data: Configuration data to write
        file_path: Path to write YAML file
    """
    with open(file_path, "w") as f:
        # Header comments
        f.write("# MongoDB Replication Tool - Job Configuration\n")
        f.write("#\n")
        f.write("# This configuration was generated by 'mongorep init' command.\n")
        f.write("# You can edit this file to customize your replication settings.\n")
        f.write("#\n")
        f.write("# For full documentation, see: src/rep/config/defaults.yaml\n")
        f.write("#\n")
        f.write("# Configuration precedence:\n")
        f.write("#   1. System defaults (defaults.yaml)\n")
        f.write("#   2. This file (job-specific overrides)\n")
        f.write("#   3. CLI arguments (highest priority)\n")
        f.write("\n")

        # Scan section
        if "scan" in data:
            f.write(
                "# =============================================================================\n"
            )
            f.write("# SCAN CONFIGURATION\n")
            f.write(
                "# =============================================================================\n"
            )
            f.write("# Controls which collections are scanned for PII and how PII is detected.\n")
            f.write("\n")
            f.write("scan:\n")

            scan = data["scan"]

            # Discovery section
            if "discovery" in scan:
                f.write("  # Collection Discovery\n")
                f.write("  # ---------------------\n")
                f.write("  # Use regex patterns to filter which collections are scanned\n")
                f.write("  discovery:\n")

                discovery = scan["discovery"]

                # Include patterns
                f.write("    # Include patterns: Only scan collections matching these patterns\n")
                f.write("    # Empty list = scan all collections\n")
                f.write("    include_patterns:\n")
                if discovery.get("include_patterns"):
                    for pattern in discovery["include_patterns"]:
                        # Quote strings that need it, otherwise write as-is
                        if isinstance(pattern, str) and (
                            '"' in pattern or "'" in pattern or ":" in pattern
                        ):
                            f.write(f"      - '{pattern}'\n")
                        else:
                            f.write(f"      - {pattern}\n")
                else:
                    f.write("      []  # Scan all collections\n")

                f.write("\n")

                # Exclude patterns
                f.write("    # Exclude patterns: Skip collections matching these patterns\n")
                f.write("    # Applied after include_patterns\n")
                f.write("    exclude_patterns:\n")
                if discovery.get("exclude_patterns"):
                    for pattern in discovery["exclude_patterns"]:
                        # Quote strings that need it, otherwise write as-is
                        if isinstance(pattern, str) and (
                            '"' in pattern or "'" in pattern or ":" in pattern
                        ):
                            f.write(f"      - '{pattern}'\n")
                        else:
                            f.write(f"      - {pattern}\n")
                else:
                    f.write("      []  # Don't exclude any collections\n")

                f.write("\n")

            # PII section
            if "pii" in scan:
                f.write("  # PII Detection Settings\n")
                f.write("  # ----------------------\n")
                f.write("  # Configure automatic PII detection using Microsoft Presidio\n")
                f.write("  pii:\n")

                pii = scan["pii"]

                # Enabled
                f.write(f"    enabled: {pii['enabled']}\n")
                f.write("\n")

                # Confidence threshold
                f.write("    # Confidence threshold (0.0-1.0)\n")
                f.write("    # Higher = fewer false positives, Lower = more sensitive\n")
                f.write(f"    confidence_threshold: {pii['confidence_threshold']}\n")
                f.write("\n")

                # Entity types
                f.write("    # PII entity types to detect\n")
                f.write("    # Common types: EMAIL_ADDRESS, PHONE_NUMBER, PERSON, CREDIT_CARD,\n")
                f.write("    #                US_SSN, IBAN_CODE, IP_ADDRESS, URL\n")
                f.write("    entity_types:\n")
                for entity_type in pii["entity_types"]:
                    f.write(f"      - {entity_type}\n")
                f.write("\n")

                # Sample size
                f.write("    # Number of documents to analyze per collection\n")
                f.write("    # Larger sample = more accurate but slower\n")
                f.write(f"    sample_size: {pii['sample_size']}\n")
                f.write("\n")

                # Sample strategy
                f.write("    # Sampling strategy: 'stratified' or 'random'\n")
                f.write(
                    "    # stratified = distributed across collection, random = random selection\n"
                )
                f.write(f"    sample_strategy: {pii['sample_strategy']}\n")
                f.write("\n")

                # Default strategies
                f.write("    # Anonymization strategies per entity type\n")
                f.write("    # Options:\n")
                f.write("    #   - redact: Replace with *** (best for sensitive data like SSN)\n")
                f.write("    #   - hash: SHA-256 hash (best for IDs, maintains uniqueness)\n")
                f.write("    #   - fake: Generate realistic fake data (best for emails, names)\n")
                f.write("    default_strategies:\n")
                for entity_type, strategy in pii["default_strategies"].items():
                    f.write(f"      {entity_type}: {strategy}\n")
                f.write("\n")

                # Allowlist
                f.write("    # Allowlist: Fields to skip PII detection (false positives)\n")
                f.write("    # Format: collection.field (e.g., users.user_id)\n")
                f.write("    allowlist:\n")
                if pii.get("allowlist"):
                    for item in pii["allowlist"]:
                        f.write(f"      - {item}\n")
                else:
                    f.write("      []  # No allowlist entries\n")
                f.write("\n")

        # Replication section
        if "replication" in data:
            f.write(
                "# =============================================================================\n"
            )
            f.write("# REPLICATION CONFIGURATION\n")
            f.write(
                "# =============================================================================\n"
            )
            f.write("# Controls how collections are replicated from source to destination.\n")
            f.write("# This section is typically generated after running 'mongorep scan'.\n")
            f.write("\n")
            f.write("replication:\n")

            replication = data["replication"]

            # Defaults
            if "defaults" in replication:
                f.write("  # Default settings for all collections\n")
                f.write("  # These can be overridden per collection below\n")
                f.write("  defaults:\n")
                for key, value in replication["defaults"].items():
                    if key == "write_disposition":
                        f.write(
                            "    # Write strategy: merge (upsert), append (insert), replace (drop/recreate)\n"
                        )
                    elif key == "fallback_cursor":
                        f.write("    # Fallback cursor field when cursor_field doesn't exist\n")
                    elif key == "initial_value":
                        f.write("    # Initial cursor value for first-time replication\n")
                    elif key == "batch_size":
                        f.write("    # Documents per batch (higher = faster but more memory)\n")
                    elif key == "max_parallel_collections":
                        f.write("    # Collections to replicate concurrently\n")
                    elif key == "transform_error_mode":
                        f.write(
                            "    # Error handling: skip (log and continue) or fail (stop replication)\n"
                        )

                    f.write(f"    {key}: {_format_yaml_value(value)}\n")
                f.write("\n")

            # Collections
            if "collections" in replication:
                f.write("  # Collection-specific configuration\n")
                f.write("  # Override defaults and specify PII fields for each collection\n")
                f.write("  collections:\n")
                for coll_name, coll_config in replication["collections"].items():
                    f.write(f"    {coll_name}:\n")

                    # Cursor field
                    f.write("      # Field to track incremental changes (e.g., updated_at, _id)\n")
                    f.write(
                        f"      cursor_field: {_format_yaml_value(coll_config.get('cursor_field'))}\n"
                    )

                    # Write disposition
                    f.write("      # Write strategy: merge, append, or replace\n")
                    f.write(f"      write_disposition: {coll_config.get('write_disposition')}\n")

                    # Primary key
                    f.write("      # Primary key field for merges and deduplication\n")
                    f.write(f"      primary_key: {coll_config.get('primary_key')}\n")

                    # PII fields
                    if coll_config.get("pii_fields"):
                        f.write("      # PII fields and their anonymization strategies\n")
                        f.write("      pii_fields:\n")
                        for field, strategy in coll_config["pii_fields"].items():
                            f.write(f"        {field}: {strategy}\n")

                    # Match filter
                    if coll_config.get("match"):
                        f.write("      # MongoDB filter to select specific documents\n")
                        f.write("      match:\n")
                        match_yaml = yaml.dump(
                            coll_config["match"], default_flow_style=False, indent=8
                        )
                        for line in match_yaml.split("\n"):
                            if line.strip():
                                f.write(f"      {line}\n")

                    # Field transforms
                    if coll_config.get("field_transforms"):
                        f.write("      # Field transformations (regex replace, etc.)\n")
                        f.write("      field_transforms:\n")
                        transforms_yaml = yaml.dump(
                            coll_config["field_transforms"], default_flow_style=False, indent=10
                        )
                        for line in transforms_yaml.split("\n"):
                            if line.strip():
                                f.write(f"      {line}\n")

                    # Fields to exclude
                    if coll_config.get("fields_exclude"):
                        f.write("      # Fields to exclude from replication\n")
                        f.write("      fields_exclude:\n")
                        for field in coll_config["fields_exclude"]:
                            f.write(f"        - {field}\n")

                    f.write("\n")

        # Relationships section
        if "relationships" in data:
            f.write(
                "# =============================================================================\n"
            )
            f.write("# RELATIONSHIPS\n")
            f.write(
                "# =============================================================================\n"
            )
            f.write("# Define parent-child relationships for cascading replication.\n")
            f.write("# Used with --select option to replicate related data across collections.\n")
            f.write("#\n")
            f.write("# Example: mongorep run job --select customers=id1,id2\n")
            f.write("# This will replicate specified customers AND their related orders/items.\n")
            f.write("\n")
            f.write("relationships:\n")

            for rel in data["relationships"]:
                f.write(f"  - parent: {rel['parent']}\n")
                f.write(f"    child: {rel['child']}\n")
                f.write("    # Field in parent collection (usually _id)\n")
                f.write(f"    parent_field: {rel['parent_field']}\n")
                f.write("    # Field in child that references parent\n")
                f.write(f"    child_field: {rel['child_field']}\n")
                f.write("\n")


def save_config(config: Config, output_path: Path) -> None:
    """
    Save Config to YAML file with helpful comments.

    Args:
        config: Config object to save
        output_path: Path to save YAML file
    """
    data = {}

    # Save scan section
    if config.scan:
        scan_data = {
            "discovery": {
                "include_patterns": config.scan.discovery.include_patterns,
                "exclude_patterns": config.scan.discovery.exclude_patterns,
            }
        }

        # Only include PII section if it exists
        if config.scan.pii:
            scan_data["pii"] = {
                "enabled": config.scan.pii.enabled,
                "confidence_threshold": config.scan.pii.confidence_threshold,
                "entity_types": config.scan.pii.entity_types,
                "sample_size": config.scan.pii.sample_size,
                "sample_strategy": config.scan.pii.sample_strategy,
                "default_strategies": config.scan.pii.default_strategies,
                "allowlist": config.scan.pii.allowlist,
            }

        data["scan"] = scan_data

    # Save replication section
    if config.replication:
        rep_data = {"defaults": config.replication.defaults, "collections": {}}

        for coll_name, coll_config in config.replication.collections.items():
            coll_data = {
                "cursor_field": coll_config.cursor_field,
                "write_disposition": coll_config.write_disposition,
                "primary_key": coll_config.primary_key,
            }

            if coll_config.pii_fields:
                coll_data["pii_fields"] = coll_config.pii_fields

            if coll_config.match:
                coll_data["match"] = coll_config.match

            if coll_config.field_transforms:
                coll_data["field_transforms"] = [
                    {
                        "field": t.field,
                        "type": t.type,
                        "pattern": t.pattern,
                        "replacement": t.replacement,
                    }
                    for t in coll_config.field_transforms
                ]

            if coll_config.fields_exclude:
                coll_data["fields_exclude"] = coll_config.fields_exclude

            rep_data["collections"][coll_name] = coll_data

        # Add schema section if it exists
        if config.replication.schema:
            rep_data["schema"] = [
                {
                    "parent": rel.parent,
                    "child": rel.child,
                    "parent_field": rel.parent_field,
                    "child_field": rel.child_field,
                }
                for rel in config.replication.schema
            ]

        data["replication"] = rep_data

    # Write to file with comments
    _write_yaml_with_comments(data, output_path)

    logger.info(f"Saved configuration to {output_path}")


def load_defaults() -> Dict[str, Any]:
    """
    Load default configuration from defaults.yaml.

    Returns:
        Dictionary with default configuration values
    """
    defaults_path = Path(__file__).parent / "defaults.yaml"

    if not defaults_path.exists():
        logger.warning(f"Defaults file not found: {defaults_path}. Using hardcoded defaults.")
        return {}

    with open(defaults_path, "r") as f:
        defaults = yaml.safe_load(f)

    return defaults or {}


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep merge two dictionaries, with override taking precedence.

    Args:
        base: Base dictionary (defaults)
        override: Override dictionary (user config)

    Returns:
        Merged dictionary
    """
    result = base.copy()

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            # Recursively merge nested dictionaries
            result[key] = deep_merge(result[key], value)
        else:
            # Override value
            result[key] = value

    return result
