"""PII analysis engine for generating static configurations.

This module analyzes sampled documents for PII patterns and generates
detailed statistics for config generation.
"""

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from mongo_replication.engine.pii.presidio_analyzer import PresidioAnalyzer
from mongo_replication.engine.pii.sampler import SamplingResult

logger = logging.getLogger(__name__)


@dataclass
class FieldPIIStats:
    """Statistics for PII detected in a specific field."""
    
    field_path: str
    entity_type: str
    suggested_strategy: str
    detections: int
    total_samples: int
    prevalence_pct: float
    avg_confidence: float
    min_confidence: float
    max_confidence: float
    
    def __str__(self) -> str:
        """Human-readable representation."""
        return (
            f"{self.field_path} ({self.entity_type}): "
            f"{self.prevalence_pct:.1f}% prevalence, "
            f"avg confidence {self.avg_confidence:.2f}"
        )


@dataclass
class CollectionPIIAnalysis:
    """Analysis result for a single collection."""
    
    collection_name: str
    total_samples: int
    fields_with_pii: List[FieldPIIStats] = field(default_factory=list)
    all_field_names: Set[str] = field(default_factory=set)
    
    @property
    def has_pii(self) -> bool:
        """Check if any PII was detected."""
        return len(self.fields_with_pii) > 0
    
    @property
    def pii_field_count(self) -> int:
        """Number of fields containing PII."""
        return len(self.fields_with_pii)
    
    def get_pii_config(self) -> Dict[str, str]:
        """
        Get pii_fields config dict for YAML generation.
        
        Returns:
            Dict mapping field_path -> strategy
        """
        return {
            stat.field_path: stat.suggested_strategy
            for stat in self.fields_with_pii
        }


class PIIAnalysisEngine:
    """
    Analyzes sampled documents for PII and generates statistics.
    
    Uses Presidio for entity detection and aggregates results across
    all samples to provide confidence and prevalence metrics.
    """
    
    def __init__(
        self,
        confidence_threshold: float = 0.7,
        language: str = "en",
        prevalence_threshold: float = 0.10,  # Skip fields with <10% prevalence
        allowlist_fields: Optional[List[str]] = None,
        entity_types: Optional[List[str]] = None,
    ):
        """
        Initialize PII analysis engine.
        
        Args:
            confidence_threshold: Minimum confidence score for PII detection
            language: Language code for NLP ('en' or 'fr')
            prevalence_threshold: Minimum prevalence (0.0-1.0) to include field
            allowlist_fields: Field patterns to exclude from detection
            entity_types: Specific entity types to detect (None or [] = all types)
        """
        self.confidence_threshold = confidence_threshold
        self.language = language
        self.prevalence_threshold = prevalence_threshold
        self.allowlist_fields = allowlist_fields or ["_id", "meta.*", "*.id"]
        self.entity_types = entity_types if entity_types else None  # Empty list -> None (all types)
        
        # Lazy load analyzer
        self._analyzer: Optional[PresidioAnalyzer] = None
    
    def get_analyzer(self) -> PresidioAnalyzer:
        """Lazy load Presidio analyzer."""
        if self._analyzer is None:
            logger.info(f"🔍 Initializing Presidio analyzer (language={self.language})...")
            self._analyzer = PresidioAnalyzer()
        return self._analyzer
    
    def analyze_collection(
        self,
        sampling_result: SamplingResult,
    ) -> CollectionPIIAnalysis:
        """
        Analyze a collection's sampled documents for PII.
        
        Args:
            sampling_result: Result from CollectionSampler
            
        Returns:
            CollectionPIIAnalysis with aggregated statistics
        """
        collection_name = sampling_result.collection_name
        sample_docs = sampling_result.sample_docs
        
        if not sample_docs:
            logger.info(f"   ⚠️  No samples to analyze")
            return CollectionPIIAnalysis(
                collection_name=collection_name,
                total_samples=0,
            )
        
        logger.info(f"   🔍 Analyzing {len(sample_docs)} samples for PII...")
        
        # Track PII detections per field
        # Structure: {field_path: {entity_type: [confidence_scores]}}
        field_detections: Dict[str, Dict[str, List[float]]] = defaultdict(
            lambda: defaultdict(list)
        )
        
        # Track all field names seen
        all_fields: Set[str] = set()
        
        analyzer = self.get_analyzer()
        
        # Analyze each document
        for doc in sample_docs:
            # Get all field paths
            doc_fields = self._get_all_field_paths(doc)
            all_fields.update(doc_fields)
            
            # Analyze document for PII
            pii_map = analyzer.analyze_document(
                document=doc,
                language=self.language,
                confidence_threshold=self.confidence_threshold,
                allowlist_fields=self.allowlist_fields,
                entity_types=self.entity_types,
            )
            
            # Aggregate detections (normalize array paths)
            for field_path, entity_info in pii_map.items():
                entity_type, confidence = entity_info  # Unpack tuple (entity_type, confidence_score)
                # Normalize array indices (e.g., "invitations[0].email" -> "invitations.email")
                normalized_path = self._normalize_array_path(field_path)
                field_detections[normalized_path][entity_type].append(confidence)
        
        # Convert detections to statistics
        total_samples = len(sample_docs)
        fields_with_pii: List[FieldPIIStats] = []
        
        for field_path, entity_types in field_detections.items():
            for entity_type, confidences in entity_types.items():
                detections = len(confidences)
                prevalence = detections / total_samples
                
                # Skip low-prevalence fields (likely false positives or edge cases)
                if prevalence < self.prevalence_threshold:
                    logger.debug(
                        f"      Skipping {field_path} ({entity_type}): "
                        f"low prevalence {prevalence:.1%}"
                    )
                    continue
                
                avg_confidence = sum(confidences) / len(confidences)
                min_confidence = min(confidences)
                max_confidence = max(confidences)
                
                # Determine redaction strategy
                strategy = self._recommend_strategy(entity_type, avg_confidence)
                
                stats = FieldPIIStats(
                    field_path=field_path,
                    entity_type=entity_type,
                    suggested_strategy=strategy,
                    detections=detections,
                    total_samples=total_samples,
                    prevalence_pct=prevalence * 100,
                    avg_confidence=avg_confidence,
                    min_confidence=min_confidence,
                    max_confidence=max_confidence,
                )
                
                fields_with_pii.append(stats)
                logger.info(f"      ✓ {stats}")
        
        # Sort by prevalence (most common first)
        fields_with_pii.sort(key=lambda s: s.prevalence_pct, reverse=True)
        
        return CollectionPIIAnalysis(
            collection_name=collection_name,
            total_samples=total_samples,
            fields_with_pii=fields_with_pii,
            all_field_names=all_fields,
        )
    
    def analyze_all_collections(
        self,
        sampling_results: Dict[str, SamplingResult],
    ) -> Dict[str, CollectionPIIAnalysis]:
        """
        Analyze PII across multiple collections.
        
        Args:
            sampling_results: Dict of collection_name -> SamplingResult
            
        Returns:
            Dict of collection_name -> CollectionPIIAnalysis
        """
        logger.info(f"\n🔍 Analyzing PII in {len(sampling_results)} collections...")
        
        analyses = {}
        for i, (coll_name, sampling_result) in enumerate(sampling_results.items(), 1):
            logger.info(f"\n[{i}/{len(sampling_results)}] {coll_name}")
            try:
                analysis = self.analyze_collection(sampling_result)
                analyses[coll_name] = analysis
                
                if analysis.has_pii:
                    logger.info(
                        f"   ✅ Found PII in {analysis.pii_field_count} fields"
                    )
                else:
                    logger.info(f"   ℹ️  No PII detected")
                    
            except Exception as e:
                logger.error(f"   ❌ Analysis failed: {e}")
                # Continue with other collections
                continue
        
        return analyses
    
    def _recommend_strategy(self, entity_type: str, avg_confidence: float) -> str:
        """
        Recommend redaction strategy based on entity type and confidence.
        
        Args:
            entity_type: Detected entity type (e.g., "EMAIL_ADDRESS")
            avg_confidence: Average confidence score
            
        Returns:
            Strategy name (e.g., "redact", "hash")
        """
        # Highly sensitive - always hash for referential integrity
        if entity_type in [
            "CREDIT_CARD",
            "IBAN_CODE",
            "CRYPTO",
            "US_PASSPORT",
            "UK_PASSPORT",
        ]:
            return "hash"
        
        # High-confidence SSN - hash for referential integrity
        if entity_type == "US_SSN" and avg_confidence >= 0.9:
            return "hash"
        
        # Default to smart format-preserving redaction
        return "redact"
    
    @staticmethod
    def _normalize_array_path(field_path: str) -> str:
        """
        Remove array indices from field path.
        
        Examples:
            "invitations[0].invitee.email" -> "invitations.invitee.email"
            "contacts[5].name" -> "contacts.name"
            "simple.field" -> "simple.field"
        
        Args:
            field_path: Field path with array indices
            
        Returns:
            Normalized field path without array indices
        """
        import re
        # Remove [N] patterns and the dot that follows (if any)
        normalized = re.sub(r'\[\d+\]\.?', '.', field_path)
        # Clean up any double dots that might result
        normalized = re.sub(r'\.\.+', '.', normalized)
        # Remove leading/trailing dots
        return normalized.strip('.')
    
    def _get_all_field_paths(
        self,
        doc: Dict[str, Any],
        parent_path: str = "",
    ) -> Set[str]:
        """
        Recursively extract all field paths from a document.
        
        Args:
            doc: Document to extract fields from
            parent_path: Parent path prefix (for recursion)
            
        Returns:
            Set of field paths in dot notation
        """
        paths = set()
        
        for key, value in doc.items():
            # Build current path
            current_path = f"{parent_path}.{key}" if parent_path else key
            paths.add(current_path)
            
            # Recurse into nested dicts
            if isinstance(value, dict):
                nested_paths = self._get_all_field_paths(value, current_path)
                paths.update(nested_paths)
            
            # Handle arrays (only check first element for schema)
            elif isinstance(value, list) and value:
                first_item = value[0]
                if isinstance(first_item, dict):
                    # Add array notation
                    array_path = f"{current_path}[0]"
                    paths.add(array_path)
                    nested_paths = self._get_all_field_paths(first_item, array_path)
                    paths.update(nested_paths)
        
        return paths
    
    def get_summary_statistics(
        self,
        analyses: Dict[str, CollectionPIIAnalysis],
    ) -> Dict[str, Any]:
        """
        Generate summary statistics across all collections.
        
        Args:
            analyses: Dict of collection_name -> CollectionPIIAnalysis
            
        Returns:
            Dict with summary statistics
        """
        total_collections = len(analyses)
        collections_with_pii = sum(1 for a in analyses.values() if a.has_pii)
        collections_without_pii = total_collections - collections_with_pii
        
        total_pii_fields = sum(a.pii_field_count for a in analyses.values())
        
        # Count by entity type
        entity_type_counts: Dict[str, int] = defaultdict(int)
        for analysis in analyses.values():
            for field_stat in analysis.fields_with_pii:
                entity_type_counts[field_stat.entity_type] += 1
        
        # Sort entity types by count
        sorted_entity_types = sorted(
            entity_type_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )
        
        return {
            "total_collections": total_collections,
            "collections_with_pii": collections_with_pii,
            "collections_without_pii": collections_without_pii,
            "total_pii_fields": total_pii_fields,
            "entity_type_breakdown": sorted_entity_types,
        }
