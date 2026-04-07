"""Configuration management for MongoDB replication.

This module provides functions for loading, saving, and managing configuration
files for MongoDB replication jobs. It handles both scan and replication configs,
with support for defaults merging and validation.
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape

from mongo_replication.config.models import (
    Config,
    ScanConfig,
    CollectionConfig,
    DefaultsReplicationConfig,
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

    # Build merged config - only merge sections that exist in raw_config
    merged_config = {}

    if "scan" in raw_config:
        # Merge scan section with defaults
        scan_defaults = system_defaults.get("scan", {})
        merged_config["scan"] = deep_merge(scan_defaults, raw_config["scan"])

    if "replication" in raw_config:
        # Merge replication section with defaults
        replication_defaults = system_defaults.get("replication", {})
        merged_replication = deep_merge(replication_defaults, raw_config["replication"])

        # Add collection names to each collection config
        if "collections" in merged_replication:
            for coll_name, coll_data in merged_replication["collections"].items():
                coll_data["name"] = coll_name

        merged_config["replication"] = merged_replication

    # Use Pydantic validation to parse the merged config
    try:
        config = Config.model_validate(merged_config)
    except Exception as e:
        raise ValueError(f"Invalid configuration in {config_path}: {str(e)}")

    return config


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


def _save_defaults(output_path: Path) -> None:
    """
    Save default configuration to a YAML file.

    This function generates the defaults.yaml file from the Pydantic model defaults.

    Args:
        output_path: Path where to save the defaults.yaml file
    """
    # Create default Config with all nested defaults
    scan_config = ScanConfig()

    # Create default replication config
    defaults_rep_config = DefaultsReplicationConfig()

    # Build the defaults structure
    defaults = {
        "scan": {
            "discovery": {
                "include_patterns": scan_config.discovery.include_patterns,
                "exclude_patterns": scan_config.discovery.exclude_patterns,
            },
            "pii": {
                "enabled": scan_config.pii.enabled,
                "confidence_threshold": scan_config.pii.confidence_threshold,
                "entity_types": scan_config.pii.entity_types,
                "sample_size": scan_config.pii.sample_size,
                "sample_strategy": scan_config.pii.sample_strategy,
                "default_strategies": scan_config.pii.default_strategies,
                "allowlist": scan_config.pii.allowlist,
                "presidio_config": scan_config.pii.presidio_config,
            },
        },
        "replication": {
            "defaults": {
                "replicate_all": defaults_rep_config.replicate_all,
                "include_patterns": defaults_rep_config.include_patterns,
                "exclude_patterns": defaults_rep_config.exclude_patterns,
                "write_disposition": defaults_rep_config.write_disposition,
                "cursor_fields": defaults_rep_config.cursor_fields,
                "fallback_cursor": defaults_rep_config.fallback_cursor,
                "initial_value": defaults_rep_config.initial_value,
                "max_parallel_collections": defaults_rep_config.max_parallel_collections,
                "batch_size": defaults_rep_config.batch_size,
                "transform_error_mode": defaults_rep_config.transform_error_mode,
                "state": {
                    "runs_collection": defaults_rep_config.state.runs_collection,
                    "state_collection": defaults_rep_config.state.state_collection,
                },
            },
            "collections": {},
            "schema": [],
        },
    }

    # Write to YAML file
    with open(output_path, "w") as f:
        f.write("# MongoDB Replication Tool - Default Configuration\n")
        f.write("#\n")
        f.write("# This file defines all default values used by the replication tool.\n")
        f.write("# These defaults are loaded first, then overridden by:\n")
        f.write("#   1. Job-specific configuration files (e.g., config/qa_db_config.yaml)\n")
        f.write("#   2. CLI arguments and options\n")
        f.write("#\n")
        f.write("# DO NOT modify this file directly. Instead, override values in your\n")
        f.write("# job-specific configuration files.\n\n")
        yaml.dump(defaults, f, default_flow_style=False, sort_keys=False)

    logger.info(f"Saved defaults to {output_path}")


def main():
    """
    Main entry point for saving defaults.yaml.

    This can be run as a standalone script to regenerate the defaults.yaml file
    from the Pydantic model defaults.
    """
    import sys

    # Default path is in the config directory
    defaults_path = Path(__file__).parent / "defaults.yaml"

    # Allow override from command line
    if len(sys.argv) > 1:
        defaults_path = Path(sys.argv[1])

    print(f"Saving defaults to: {defaults_path}")
    _save_defaults(defaults_path)
    print("Done!")


if __name__ == "__main__":
    main()
