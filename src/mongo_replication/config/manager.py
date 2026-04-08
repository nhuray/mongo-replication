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
    ReplicationConfig,
    SchemaRelationshipConfig,
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

    # Merge config
    merged_config = deep_merge(system_defaults, raw_config)
    logger.debug(f"Merged config: {merged_config}")

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


def load_schema_relationships(config_path: Path) -> list[SchemaRelationshipConfig]:
    """
    Load schema relationships from configuration file.

    This function is used for cascade replication to load parent-child
    relationships between collections.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        List of SchemaRelationshipConfig objects

    Raises:
        ValueError: If config file doesn't have schema_relationships section
    """
    full_config = load_config(config_path)

    if not full_config.schema_relationships:
        raise ValueError(
            f"Configuration file {config_path} has no 'schema_relationships' section. "
            f"Cascade replication requires defining relationships between collections."
        )

    return full_config.schema_relationships


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
    # Render Config to dictionary
    data = config.model_dump(mode="json")

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
    # Create default Config
    default_config = Config(scan=(ScanConfig()), replication=(ReplicationConfig()))

    # Save config
    save_config(default_config, output_path)

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
