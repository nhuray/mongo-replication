"""Automatic PII detection using Microsoft Presidio.

This module provides automatic PII detection capabilities using Microsoft Presidio's
NLP-based entity recognition. It supports both English and French languages.

Supports YAML-based configuration for:
- Custom PII recognizers (regex patterns, deny-lists)
- NLP model selection
- Language support
- Context-aware detection
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from presidio_analyzer import AnalyzerEngine, AnalyzerEngineProvider
from presidio_analyzer.nlp_engine import NlpEngineProvider

logger = logging.getLogger(__name__)


class PresidioAnalyzer:
    """
    Singleton class for automatic PII detection using Microsoft Presidio.

    Supports YAML-based configuration for custom PII recognizers and NLP models.
    Falls back to default configuration if no YAML is provided.

    The analyzer is initialized lazily on first use to avoid loading heavy NLP models
    (~500MB) unless auto-detection is actually enabled.
    """

    _instance: Optional["PresidioAnalyzer"] = None
    _analyzer_cache: Dict[str, AnalyzerEngine] = {}  # Cache analyzers by config path
    _supported_languages = ["en", "fr"]

    def __new__(cls) -> "PresidioAnalyzer":
        """Ensure only one instance exists (singleton pattern)."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_analyzer(self, presidio_config_path: Optional[str] = None) -> AnalyzerEngine:
        """
        Get or create the Presidio AnalyzerEngine instance.

        Lazy initialization - loads NLP models only on first call.
        This is a ~2-5 second operation that loads ~500MB of models.

        Args:
            presidio_config_path: Optional path to Presidio YAML configuration file.
                                 If None, uses default configuration.
                                 Supports absolute and relative paths.

        Returns:
            Initialized AnalyzerEngine instance

        Raises:
            FileNotFoundError: If config file doesn't exist at resolved path
            ValueError: If YAML configuration is invalid
        """
        # Use config path as cache key (None for default)
        cache_key = presidio_config_path or "default"

        # Return cached analyzer if available
        if cache_key in self._analyzer_cache:
            logger.debug(f"Using cached AnalyzerEngine for config: {cache_key}")
            return self._analyzer_cache[cache_key]

        # Create new analyzer
        if presidio_config_path:
            resolved_path = self._resolve_config_path(presidio_config_path)
            logger.info(f"Initializing Presidio AnalyzerEngine from YAML: {resolved_path}")
            analyzer = self._create_from_yaml(resolved_path)
        else:
            logger.info("Initializing Presidio AnalyzerEngine with default configuration...")
            analyzer = self._create_default_analyzer()

        # Cache and return
        self._analyzer_cache[cache_key] = analyzer
        logger.info("Presidio AnalyzerEngine initialized successfully")
        return analyzer

    def _resolve_config_path(self, config_path: str) -> Path:
        """
        Resolve configuration file path.

        Resolution order:
        1. Absolute path: Use as-is
        2. Relative to current working directory
        3. Relative to config/ directory
        4. Relative to default config location (src/mongo_replication/config/)

        Args:
            config_path: Configuration file path (absolute or relative)

        Returns:
            Resolved Path object

        Raises:
            FileNotFoundError: If file doesn't exist at any resolved location
        """
        path = Path(config_path)

        # Check if absolute path
        if path.is_absolute():
            if path.exists():
                return path
            raise FileNotFoundError(
                f"Presidio configuration file not found at absolute path: {path}"
            )

        # Try relative to current working directory
        cwd_path = Path.cwd() / path
        if cwd_path.exists():
            logger.debug(f"Resolved config path relative to cwd: {cwd_path}")
            return cwd_path

        # Try relative to config/ directory
        config_dir_path = Path.cwd() / "config" / path.name
        if config_dir_path.exists():
            logger.debug(f"Resolved config path in config/ directory: {config_dir_path}")
            return config_dir_path

        # Try default location (src/mongo_replication/config/)
        default_path = Path(__file__).parent.parent / "config" / path.name
        if default_path.exists():
            logger.debug(f"Resolved config path in default location: {default_path}")
            return default_path

        # File not found anywhere
        raise FileNotFoundError(
            f"Presidio configuration file not found: {config_path}\n"
            f"Searched locations:\n"
            f"  - Absolute: {path}\n"
            f"  - Relative to cwd: {cwd_path}\n"
            f"  - In config/ dir: {config_dir_path}\n"
            f"  - Default location: {default_path}"
        )

    def _create_from_yaml(self, config_path: Path) -> AnalyzerEngine:
        """
        Create AnalyzerEngine from YAML configuration file.

        Uses Presidio's AnalyzerEngineProvider to load configuration.
        Fails fast with clear error messages if configuration is invalid.

        Args:
            config_path: Path to YAML configuration file

        Returns:
            Initialized AnalyzerEngine instance

        Raises:
            ValueError: If YAML configuration is invalid
            FileNotFoundError: If configuration file doesn't exist
        """
        try:
            # Use AnalyzerEngineProvider to load from YAML
            # This validates the YAML and creates the engine with all recognizers
            provider = AnalyzerEngineProvider(analyzer_engine_conf_file=str(config_path))
            analyzer = provider.create_engine()

            logger.debug(
                f"Loaded Presidio configuration from {config_path} with "
                f"{len(analyzer.registry.recognizers)} recognizers"
            )

            return analyzer

        except FileNotFoundError as e:
            raise FileNotFoundError(f"Presidio configuration file not found: {config_path}") from e

        except Exception as e:
            raise ValueError(
                f"Failed to load Presidio configuration from {config_path}: {e}\n"
                f"Please check your YAML syntax and configuration.\n"
                f"See: https://microsoft.github.io/presidio/samples/python/no_code_config/"
            ) from e

    def _create_default_analyzer(self) -> AnalyzerEngine:
        """
        Create AnalyzerEngine with default configuration.

        Uses English and French NLP models with all built-in Presidio recognizers.
        This is the fallback when no YAML configuration is provided.

        Returns:
            Initialized AnalyzerEngine instance
        """
        # Configure NLP engine with English and French models
        configuration = {
            "nlp_engine_name": "spacy",
            "models": [
                {"lang_code": "en", "model_name": "en_core_web_lg"},
                {"lang_code": "fr", "model_name": "fr_core_news_lg"},
            ],
        }

        provider = NlpEngineProvider(nlp_configuration=configuration)
        nlp_engine = provider.create_engine()

        # Create analyzer with the configured NLP engine
        analyzer = AnalyzerEngine(
            nlp_engine=nlp_engine, supported_languages=self._supported_languages
        )

        logger.debug(
            f"Created default AnalyzerEngine with {len(analyzer.registry.recognizers)} recognizers"
        )

        return analyzer

    def analyze_document(
        self,
        document: Dict[str, Any],
        confidence_threshold: float = 0.7,
        language: str = "en",
        entity_types: Optional[List[str]] = None,
        allowlist_fields: Optional[List[str]] = None,
        presidio_config_path: Optional[str] = None,
    ) -> Dict[str, Tuple[str, float]]:
        """
        Analyze a MongoDB document to detect PII fields.

        Args:
            document: The MongoDB document to analyze
            confidence_threshold: Minimum confidence score (0.0-1.0) for detection
            language: Language code ('en' or 'fr')
            entity_types: List of specific entity types to detect (None = all types)
            allowlist_fields: List of field patterns that should never be treated as PII
            presidio_config_path: Optional path to Presidio YAML configuration file

        Returns:
            Dictionary mapping field paths to (entity_type, confidence_score) tuples
            Example: {"email": ("EMAIL_ADDRESS", 0.95), "user.ssn": ("US_SSN", 0.99)}
        """
        analyzer = self.get_analyzer(presidio_config_path=presidio_config_path)
        pii_map: Dict[str, Tuple[str, float]] = {}

        # Flatten the document to handle nested fields
        flattened = self._flatten_document(document)

        # Filter out allowlisted fields
        if allowlist_fields:
            flattened = self._filter_allowlist(flattened, allowlist_fields)

        # Analyze each field
        for field_path, value in flattened.items():
            # Only analyze string values
            if not isinstance(value, str) or not value.strip():
                continue

            # Analyze the text
            try:
                results = analyzer.analyze(
                    text=value,
                    language=language,
                    entities=entity_types,  # None means detect all entity types
                )

                # Find highest confidence detection for this field
                for result in results:
                    if result.score >= confidence_threshold:
                        # If we already detected PII in this field, keep the highest confidence
                        if field_path in pii_map:
                            existing_score = pii_map[field_path][1]
                            if result.score > existing_score:
                                pii_map[field_path] = (result.entity_type, result.score)
                        else:
                            pii_map[field_path] = (result.entity_type, result.score)

                        logger.debug(
                            f"Detected PII in field '{field_path}': {result.entity_type} "
                            f"(confidence: {result.score:.2f})"
                        )

            except Exception as e:
                logger.warning(f"Error analyzing field '{field_path}': {e}")
                continue

        return pii_map

    def _flatten_document(self, document: Dict[str, Any], parent_key: str = "") -> Dict[str, Any]:
        """
        Flatten a nested MongoDB document into dot-notation field paths.

        Args:
            document: The document to flatten
            parent_key: Parent key for nested recursion

        Returns:
            Flattened dictionary with dot-notation keys
            Example: {"user.address.street": "123 Main St", "user.email": "test@example.com"}
        """
        items: List[Tuple[str, Any]] = []

        for key, value in document.items():
            new_key = f"{parent_key}.{key}" if parent_key else key

            if isinstance(value, dict):
                # Recursively flatten nested documents
                items.extend(self._flatten_document(value, new_key).items())
            elif isinstance(value, list):
                # Handle lists by processing each element
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        items.extend(self._flatten_document(item, f"{new_key}[{i}]").items())
                    else:
                        items.append((f"{new_key}[{i}]", item))
            else:
                items.append((new_key, value))

        return dict(items)

    def _filter_allowlist(
        self, flattened: Dict[str, Any], allowlist_patterns: List[str]
    ) -> Dict[str, Any]:
        """
        Filter out fields matching allowlist patterns.

        Args:
            flattened: Flattened document
            allowlist_patterns: List of field patterns (supports wildcards with *)

        Returns:
            Filtered dictionary with allowlisted fields removed
        """
        filtered = {}

        for field_path, value in flattened.items():
            should_skip = False

            for pattern in allowlist_patterns:
                if self._matches_pattern(field_path, pattern):
                    should_skip = True
                    logger.debug(
                        f"Skipping field '{field_path}' (matches allowlist pattern '{pattern}')"
                    )
                    break

            if not should_skip:
                filtered[field_path] = value

        return filtered

    def _matches_pattern(self, field_path: str, pattern: str) -> bool:
        """
        Check if a field path matches a pattern.

        Supports simple wildcard matching:
        - "metadata.*" matches "metadata.created_at", "metadata.user_id", etc.
        - "_id" matches exactly "_id"
        - "*.id" matches "user.id", "account.id", etc.

        Args:
            field_path: The field path to check
            pattern: The pattern to match against

        Returns:
            True if the field matches the pattern
        """
        # Exact match
        if field_path == pattern:
            return True

        # Wildcard patterns
        if "*" in pattern:
            # Convert glob pattern to regex-like logic
            pattern_parts = pattern.split("*")

            # Pattern starts with wildcard: "*.id"
            if pattern.startswith("*"):
                if field_path.endswith(pattern[1:]):
                    return True

            # Pattern ends with wildcard: "metadata.*"
            if pattern.endswith("*"):
                if field_path.startswith(pattern[:-1]):
                    return True

            # Pattern has wildcard in middle: "user.*.email"
            # For simplicity, we'll just check if all non-wildcard parts are present in order
            current_pos = 0
            for part in pattern_parts:
                if part:  # Skip empty parts from consecutive wildcards
                    pos = field_path.find(part, current_pos)
                    if pos == -1:
                        return False
                    current_pos = pos + len(part)
            return True

        return False

    def get_supported_entity_types(self, presidio_config_path: Optional[str] = None) -> Set[str]:
        """
        Get the set of entity types supported by Presidio.

        Args:
            presidio_config_path: Optional path to Presidio YAML configuration file

        Returns:
            Set of supported entity type names
        """
        analyzer = self.get_analyzer(presidio_config_path=presidio_config_path)
        return set(analyzer.get_supported_entities())


# Singleton instance for easy import
analyzer = PresidioAnalyzer()


def analyze_document(
    document: Dict[str, Any],
    confidence_threshold: float = 0.7,
    language: str = "en",
    entity_types: Optional[List[str]] = None,
    allowlist_fields: Optional[List[str]] = None,
    presidio_config_path: Optional[str] = None,
) -> Dict[str, Tuple[str, float]]:
    """
    Convenience function to analyze a document for PII.

    Args:
        document: The MongoDB document to analyze
        confidence_threshold: Minimum confidence score (0.0-1.0) for detection
        language: Language code ('en' or 'fr')
        entity_types: List of specific entity types to detect (None = all types)
        allowlist_fields: List of field patterns that should never be treated as PII
        presidio_config_path: Optional path to Presidio YAML configuration file

    Returns:
        Dictionary mapping field paths to (entity_type, confidence_score) tuples
    """
    return analyzer.analyze_document(
        document=document,
        confidence_threshold=confidence_threshold,
        language=language,
        entity_types=entity_types,
        allowlist_fields=allowlist_fields,
        presidio_config_path=presidio_config_path,
    )
