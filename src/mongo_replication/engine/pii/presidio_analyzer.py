"""Automatic PII detection using Microsoft Presidio.

This module provides automatic PII detection capabilities using Microsoft Presidio's
NLP-based entity recognition. It supports both English and French languages.
"""

import logging
from typing import Any, Dict, List, Optional, Set, Tuple

from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import NlpEngineProvider

logger = logging.getLogger(__name__)


class PresidioAnalyzer:
    """
    Singleton class for automatic PII detection using Microsoft Presidio.

    Uses spaCy NLP models for English and French to detect various types of PII
    including emails, phone numbers, names, addresses, SSNs, credit cards, etc.

    The analyzer is initialized lazily on first use to avoid loading heavy NLP models
    (~500MB) unless auto-detection is actually enabled.
    """

    _instance: Optional["PresidioAnalyzer"] = None
    _analyzer_engine: Optional[AnalyzerEngine] = None
    _supported_languages = ["en", "fr"]

    def __new__(cls) -> "PresidioAnalyzer":
        """Ensure only one instance exists (singleton pattern)."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_analyzer(self) -> AnalyzerEngine:
        """
        Get or create the Presidio AnalyzerEngine instance.

        Lazy initialization - loads NLP models only on first call.
        This is a ~2-5 second operation that loads ~500MB of models.

        Returns:
            Initialized AnalyzerEngine instance
        """
        if self._analyzer_engine is None:
            logger.info("Initializing Presidio AnalyzerEngine with English and French models...")

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
            self._analyzer_engine = AnalyzerEngine(
                nlp_engine=nlp_engine, supported_languages=self._supported_languages
            )

            logger.info("Presidio AnalyzerEngine initialized successfully")

        return self._analyzer_engine

    def analyze_document(
        self,
        document: Dict[str, Any],
        confidence_threshold: float = 0.7,
        language: str = "en",
        entity_types: Optional[List[str]] = None,
        allowlist_fields: Optional[List[str]] = None,
    ) -> Dict[str, Tuple[str, float]]:
        """
        Analyze a MongoDB document to detect PII fields.

        Args:
            document: The MongoDB document to analyze
            confidence_threshold: Minimum confidence score (0.0-1.0) for detection
            language: Language code ('en' or 'fr')
            entity_types: List of specific entity types to detect (None = all types)
            allowlist_fields: List of field patterns that should never be treated as PII

        Returns:
            Dictionary mapping field paths to (entity_type, confidence_score) tuples
            Example: {"email": ("EMAIL_ADDRESS", 0.95), "user.ssn": ("US_SSN", 0.99)}
        """
        analyzer = self.get_analyzer()
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

    def get_supported_entity_types(self) -> Set[str]:
        """
        Get the set of entity types supported by Presidio.

        Returns:
            Set of supported entity type names
        """
        analyzer = self.get_analyzer()
        return set(analyzer.get_supported_entities())


# Singleton instance for easy import
analyzer = PresidioAnalyzer()


def analyze_document(
    document: Dict[str, Any],
    confidence_threshold: float = 0.7,
    language: str = "en",
    entity_types: Optional[List[str]] = None,
    allowlist_fields: Optional[List[str]] = None,
) -> Dict[str, Tuple[str, float]]:
    """
    Convenience function to analyze a document for PII.

    Args:
        document: The MongoDB document to analyze
        confidence_threshold: Minimum confidence score (0.0-1.0) for detection
        language: Language code ('en' or 'fr')
        entity_types: List of specific entity types to detect (None = all types)
        allowlist_fields: List of field patterns that should never be treated as PII

    Returns:
        Dictionary mapping field paths to (entity_type, confidence_score) tuples
    """
    return analyzer.analyze_document(
        document=document,
        confidence_threshold=confidence_threshold,
        language=language,
        entity_types=entity_types,
        allowlist_fields=allowlist_fields,
    )
