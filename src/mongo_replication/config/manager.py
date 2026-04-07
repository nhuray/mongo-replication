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
from jinja2 import Environment, FileSystemLoader, select_autoescape

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


# Initialize Jinja2 environment for template rendering
def _get_jinja_env() -> Environment:
    """Get Jinja2 environment for rendering config templates."""
    template_dir = Path(__file__).parent
    env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=select_autoescape(["yaml", "yml"]),
        trim_blocks=False,
        lstrip_blocks=False,
        keep_trailing_newline=True,
    )

    # Add custom filters
    def toyaml(value, indent=2):
        """Convert value to YAML string."""
        result = yaml.dump(value, default_flow_style=False, indent=indent).strip()
        # Remove YAML document separators
        if result.endswith("..."):
            result = result[:-3].strip()
        return result

    def tojson_yaml(value):
        """Convert value to YAML JSON-style string (inline)."""
        result = yaml.dump(value, default_flow_style=True).strip()
        # Remove YAML document separators
        if result.endswith("..."):
            result = result[:-3].strip()
        return result

    env.filters["toyaml"] = toyaml
    env.filters["tojson"] = tojson_yaml

    return env


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
            presidio_config=pii_data.get(
                "presidio_config", pii_defaults.get("presidio_config", None)
            ),
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


def _render_config_template(data: Dict[str, Any]) -> str:
    """
    Render configuration to YAML string using Jinja2 template.

    Args:
        data: Configuration data to render

    Returns:
        Rendered YAML string with comments
    """
    env = _get_jinja_env()
    template = env.get_template("config_template.yaml.j2")
    return template.render(**data)


def save_config(config: Config, output_path: Path) -> None:
    """
    Save Config to YAML file with helpful comments using Jinja2 template.

    Args:
        config: Config object to save
        output_path: Path to save YAML file
    """
    data = {}

    # Prepare scan section data
    if config.scan:
        scan_data = {
            "discovery": {
                "include_patterns": config.scan.discovery.include_patterns,
                "exclude_patterns": config.scan.discovery.exclude_patterns,
            },
            "pii": {
                "enabled": config.scan.pii.enabled,
                "confidence_threshold": config.scan.pii.confidence_threshold,
                "entity_types": config.scan.pii.entity_types,
                "sample_size": config.scan.pii.sample_size,
                "sample_strategy": config.scan.pii.sample_strategy,
                "default_strategies": config.scan.pii.default_strategies,
                "allowlist": config.scan.pii.allowlist,
                "presidio_config": config.scan.pii.presidio_config,
            },
        }
        data["scan"] = scan_data

    # Prepare replication section data
    if config.replication:
        collections_data = {}
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

            collections_data[coll_name] = coll_data

        rep_data = {
            "defaults": config.replication.defaults,
            "collections": collections_data,
        }

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

    # Render template and write to file
    rendered = _render_config_template(data)
    with open(output_path, "w") as f:
        f.write(rendered)

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
