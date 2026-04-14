"""Configuration loader for Presidio analyzer and anonymizer.

This module provides utilities to load and parse Presidio configuration from YAML,
including both recognizers (for analysis) and operators (for anonymization).
"""

import logging
from pathlib import Path
from typing import Dict, Optional, List, Set

import yaml
from presidio_anonymizer.entities import OperatorConfig

logger = logging.getLogger(__name__)


class PresidioConfig:
    """Loads and parses Presidio configuration from YAML files.

    This class handles loading the YAML configuration for both:
    1. Presidio Analyzer (recognizers for PII detection)
    2. Presidio Anonymizer (operators for PII anonymization)

    The anonymization operators configuration is a custom mongo-replication feature
    that extends Presidio's configuration format.
    """

    def __init__(self, config_path: Optional[str] = None):
        """Initialize configuration loader.

        Args:
            config_path: Optional path to custom Presidio configuration file.
                        If None, uses the bundled default configuration.
        """
        if config_path:
            self.config_path = Path(config_path)
        else:
            # Use bundled default config
            self.config_path = Path(__file__).parent / "presidio.yaml"

        if not self.config_path.exists():
            raise FileNotFoundError(f"Presidio configuration file not found: {self.config_path}")

        self.config = self._load_config()

    def _load_config(self) -> dict:
        """Load YAML configuration from file.

        Returns:
            Parsed YAML configuration as dictionary

        Raises:
            yaml.YAMLError: If YAML parsing fails
        """
        try:
            with open(self.config_path, "r") as f:
                config = yaml.safe_load(f)
                logger.debug(f"Loaded Presidio config from: {self.config_path}")
                return config
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in {self.config_path}: {e}")

    def get_operator_configs(self) -> Dict[str, OperatorConfig]:
        """Parse anonymization_operators from YAML and convert to OperatorConfig objects.

        This method reads the 'anonymization_operators' section from the YAML config
        and converts each operator definition into a Presidio OperatorConfig object.

        Returns:
            Dictionary mapping entity types to OperatorConfig instances.
            Example: {"EMAIL_ADDRESS": OperatorConfig("mask", {...}), ...}

        Example YAML:
            anonymization_operators:
              EMAIL_ADDRESS:
                operator: mask
                params:
                  masking_char: "*"
                  chars_to_mask: 10
        """
        operators_yaml = self.config.get("anonymization_operators", {})
        operator_configs = {}

        for entity_type, config in operators_yaml.items():
            operator_name = config.get("operator")
            params = config.get("params", {})

            if not operator_name:
                logger.warning(f"No operator specified for entity type '{entity_type}', skipping")
                continue

            operator_configs[entity_type] = OperatorConfig(operator_name, params)
            logger.debug(f"Loaded operator config for {entity_type}: {operator_name}({params})")

        return operator_configs

    def get_strategy_aliases(self) -> Dict[str, str]:
        """Get custom strategy name aliases.

        This method reads the 'custom_strategy_aliases' section from YAML config
        and returns a mapping of strategy names to operator names.

        Returns:
            Dictionary mapping strategy alias names to operator names.
            Example: {"fake_email": "fake_email", "redact": "redact", ...}

        Example YAML:
            custom_strategy_aliases:
              fake_email:
                description: "Generate fake email"
                operator: fake_email
        """
        aliases_yaml = self.config.get("custom_strategy_aliases", {})
        aliases = {}

        for alias_name, config in aliases_yaml.items():
            operator = config.get("operator")
            if operator:
                aliases[alias_name] = operator
                logger.debug(f"Loaded strategy alias: {alias_name} -> {operator}")

        return aliases

    def get_operator_for_strategy(self, strategy_name: str) -> Optional[str]:
        """Get the operator name for a given strategy alias.

        Args:
            strategy_name: The strategy name (e.g., "fake_email", "hash", "redact")

        Returns:
            The operator name, or None if not found
        """
        aliases = self.get_strategy_aliases()
        return aliases.get(strategy_name)

    def get_supported_entity_types(self) -> List[str]:
        """Get all supported entity types from anonymizer_registry.

        Parses the anonymizer_registry section and extracts all unique entity types
        that are supported by the registered operators.

        Returns:
            Sorted list of unique entity type names.
            Example: ["EMAIL_ADDRESS", "PHONE_NUMBER", "PERSON", ...]
        """
        registry = self.config.get("anonymizer_registry", {})
        entity_types: Set[str] = set()

        for operator_name, operator_config in registry.items():
            if isinstance(operator_config, dict):
                supported = operator_config.get("supported_entities", [])
                if isinstance(supported, list):
                    entity_types.update(supported)

        return sorted(entity_types)

    def get_operator_examples(self, operator_name: str) -> List[Dict[str, str]]:
        """Get example inputs/outputs for a specific operator.

        Args:
            operator_name: Name of the operator (e.g., "mask_email", "fake_phone")

        Returns:
            List of example dicts with 'input' and 'output' keys.
            Example: [{"input": "test@example.com", "output": "te**@example.com"}]
        """
        registry = self.config.get("anonymizer_registry", {})
        operator_config = registry.get(operator_name, {})

        if isinstance(operator_config, dict):
            examples = operator_config.get("examples", [])
            if isinstance(examples, list):
                return examples

        return []

    def get_operators_for_entity_type(self, entity_type: str) -> List[str]:
        """Get all operators that support a specific entity type.

        Args:
            entity_type: Entity type name (e.g., "EMAIL_ADDRESS")

        Returns:
            List of operator names that support this entity type.
            Example: ["mask_email", "fake_email"]
        """
        registry = self.config.get("anonymizer_registry", {})
        operators = []

        for operator_name, operator_config in registry.items():
            if isinstance(operator_config, dict):
                supported = operator_config.get("supported_entities", [])
                if isinstance(supported, list) and entity_type in supported:
                    operators.append(operator_name)

        return operators


def load_presidio_config(config_path: Optional[str] = None) -> PresidioConfig:
    """Convenience function to load Presidio configuration.

    Args:
        config_path: Optional path to custom configuration file

    Returns:
        PresidioConfig instance
    """
    return PresidioConfig(config_path)
