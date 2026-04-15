"""
Scan report generator - creates markdown reports from scan analysis.

Includes PII analysis, cursor field detection, and schema relationships.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

from jinja2 import Environment, FileSystemLoader, select_autoescape

from mongo_replication.engine.pii import CollectionPIIAnalysis
from mongo_replication.engine.pii.presidio_anonymizer import PresidioAnonymizer

logger = logging.getLogger(__name__)


def generate_scan_report(
    job_id: str,
    pii_analyses: Dict[str, CollectionPIIAnalysis],
    output_path: Path,
    cursor_fields: Optional[Dict[str, Any]] = None,
    schema_relationships: Optional[List[Any]] = None,
    total_samples: int = 0,
) -> None:
    """
    Generate a markdown scan report with PII analysis, cursor detection, and schema relationships.

    Args:
        job_id: Job identifier
        pii_analyses: Dictionary mapping collection names to PII analyses
        output_path: Path to write the markdown report
        cursor_fields: Dictionary mapping collection names to cursor field info
        schema_relationships: List of schema relationships
        total_samples: Total number of documents sampled
    """
    # Setup Jinja2 environment
    template_dir = Path(__file__).parent / "templates"
    env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=select_autoescape(["html", "xml"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )

    template = env.get_template("scan_report.md.j2")

    # Build summary data
    total_collections = len(pii_analyses) if pii_analyses else 0
    collections_with_pii = sum(1 for a in pii_analyses.values() if a.has_pii) if pii_analyses else 0
    total_pii_fields = sum(a.pii_field_count for a in pii_analyses.values()) if pii_analyses else 0

    collections_with_cursor = 0
    if cursor_fields:
        collections_with_cursor = sum(
            1 for info in cursor_fields.values() if info.get("cursor_field")
        )

    summary = {
        "total_collections": total_collections,
        "total_samples": f"{total_samples:,}",
        "collections_with_pii": collections_with_pii,
        "total_pii_fields": total_pii_fields,
        "collections_with_cursor": collections_with_cursor,
        "total_relationships": len(schema_relationships) if schema_relationships else 0,
    }

    # Prepare PII data with anonymization examples
    collections_with_pii_data = []
    collections_without_pii_data = []

    # Initialize anonymizer for generating examples
    anonymizer = None

    if pii_analyses:
        # Sort collections alphabetically
        sorted_collections = sorted(
            [(name, analysis) for name, analysis in pii_analyses.items() if analysis.has_pii],
            key=lambda x: x[0],  # Sort by collection name
        )

        for collection_name, analysis in sorted_collections:
            # Sort fields by prevalence (descending)
            sorted_fields = sorted(
                analysis.fields_with_pii,
                key=lambda x: x.prevalence_pct,
                reverse=True,
            )

            fields_data = []
            for field in sorted_fields:
                field_data = {
                    "field_path": field.field_path,
                    "entity_type": field.entity_type,
                    "suggested_strategy": field.suggested_strategy,
                    "prevalence_pct": f"{field.prevalence_pct:.1f}",
                    "avg_confidence": f"{field.avg_confidence:.2f}",
                    "detections": field.detections,
                    "total_samples": field.total_samples,
                    "example": None,  # Will be populated if sample_value exists
                }

                # Generate anonymization example if sample value is available
                if field.sample_value:
                    try:
                        # Lazy initialize anonymizer only if needed
                        if anonymizer is None:
                            anonymizer = PresidioAnonymizer()

                        # Anonymize the sample value
                        anonymized = anonymizer.anonymize_text(
                            text=field.sample_value,
                            operator_name=field.suggested_strategy,
                            entity_type=field.entity_type,
                        )
                        field_data["example"] = f"{field.sample_value} → {anonymized}"
                        logger.debug(
                            f"Generated example for {collection_name}.{field.field_path}: "
                            f"{field.sample_value} → {anonymized}"
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to generate example for {collection_name}.{field.field_path}: {e}"
                        )
                        field_data["example"] = None
                else:
                    logger.debug(
                        f"No sample value for {collection_name}.{field.field_path} "
                        f"({field.entity_type}), skipping example generation"
                    )

                fields_data.append(field_data)

            collections_with_pii_data.append(
                {
                    "name": collection_name,
                    "total_samples": analysis.total_samples,
                    "fields": fields_data,
                }
            )

        collections_without_pii_data = sorted(
            [name for name, analysis in pii_analyses.items() if not analysis.has_pii]
        )

    # Prepare cursor field data
    cursor_data_with = []
    cursor_data_without = []

    if cursor_fields:
        for collection_name, info in sorted(cursor_fields.items()):
            cursor_field = info.get("cursor_field")
            if cursor_field:
                cursor_data_with.append(
                    {
                        "collection": collection_name,
                        "cursor_field": cursor_field,
                        "sample_value": info.get("sample_value", "N/A"),
                    }
                )
            else:
                cursor_data_without.append(collection_name)

    # Prepare relationship data
    relationships_data = []
    if schema_relationships:
        # Sort relationships alphabetically by child collection
        sorted_relationships = sorted(schema_relationships, key=lambda rel: rel.child)
        for rel in sorted_relationships:
            relationships_data.append(
                {
                    "parent": rel.parent,
                    "child": rel.child,
                    "parent_field": rel.parent_field,
                    "child_field": rel.child_field,
                }
            )

    # Render template
    report = template.render(
        job_id=job_id,
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        summary=summary,
        pii_enabled=pii_analyses is not None and len(pii_analyses) > 0,
        cursor_detection_enabled=cursor_fields is not None,
        relationships_enabled=schema_relationships is not None,
        collections_with_pii=collections_with_pii_data,
        collections_without_pii=collections_without_pii_data,
        collections_with_cursor=cursor_data_with,
        collections_without_cursor=cursor_data_without,
        relationships=relationships_data,
    )

    # Write to file
    output_path.write_text(report)
