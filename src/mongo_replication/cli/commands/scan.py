"""
Scan command - discover collections and analyze PII.

Usage:
    mongorep scan <job> [OPTIONS]
"""

import time
from pathlib import Path
from typing import Optional, List, Dict, Any

import typer
from typing_extensions import Annotated

from mongo_replication.cli.interactive.selectors import select_collections
from mongo_replication.cli.reporters.pii_report import generate_pii_report
from mongo_replication.cli.reporters.progress import progress_wrapper
from mongo_replication.cli.utils.output import (
    print_banner,
    print_success,
    print_error,
    print_warning,
    print_info,
    print_step,
    print_summary,
    console,
)
from mongo_replication.config.manager import save_config, load_config
from mongo_replication.config.models import (
    ScanConfig,
    ScanDiscoveryConfig,
    ScanSamplingConfig,
    ScanPIIAnalysisConfig,
    Config,
    CollectionConfig,
    ReplicationConfig,
)
from mongo_replication.engine.connection import ConnectionManager
from mongo_replication.engine.jobs import JobManager
from mongo_replication.engine.pii import CollectionSampler, PIIAnalysisEngine


def detect_cursor_field(
    collection_name: str, sample_document: Dict[str, Any], cursor_fields: List[str]
) -> Optional[str]:
    """
    Detect which cursor field exists in a collection by examining a sample document.

    Checks for field existence using case-insensitive matching and various case conventions.

    Args:
        collection_name: Name of the collection being scanned
        sample_document: A sample document from the collection
        cursor_fields: List of cursor field names to try (in priority order)

    Returns:
        The first matching field name found in the document, or None if no match

    Examples:
        >>> doc = {"updatedAt": "2024-01-01", "name": "test"}
        >>> detect_cursor_field("users", doc, ["updated_at", "updatedAt"])
        "updatedAt"

        >>> doc = {"meta": {"updated_at": "2024-01-01"}}
        >>> detect_cursor_field("users", doc, ["meta.updated_at", "updatedAt"])
        "meta.updated_at"
    """
    if not sample_document:
        return None

    def get_nested_field(doc: Dict[str, Any], field_path: str) -> Any:
        """Get a nested field value from a document using dot notation."""
        parts = field_path.split(".")
        current = doc

        for part in parts:
            if not isinstance(current, dict):
                return None

            # Try exact match first
            if part in current:
                current = current[part]
                continue

            # Try case-insensitive match
            for key in current.keys():
                if key.lower() == part.lower():
                    current = current[key]
                    break
            else:
                return None

        return current

    # Try each cursor field in priority order
    for cursor_field in cursor_fields:
        value = get_nested_field(sample_document, cursor_field)
        if value is not None:
            return cursor_field

    return None


def scan_command(
    job: Annotated[str, typer.Argument(help="Job ID to scan (e.g., 'prod_db')")],
    output: Annotated[
        Optional[str],
        typer.Option(
            "--output",
            "-o",
            help="Output path for config file (default: config/<job>_config.yaml)",
        ),
    ] = None,
    collections: Annotated[
        Optional[str],
        typer.Option(
            "--collections",
            help="Comma-separated list of collections to scan (default: all)",
        ),
    ] = None,
    interactive: Annotated[
        bool,
        typer.Option(
            "--interactive",
            "-i",
            help="Interactively select collections to scan",
        ),
    ] = False,
    sample_size: Annotated[
        Optional[int],
        typer.Option(
            "--sample-size",
            "-s",
            help="Number of documents to sample per collection (default: from config or 1000)",
        ),
    ] = None,
    confidence_threshold: Annotated[
        Optional[float],
        typer.Option(
            "--confidence",
            "-c",
            help="Minimum confidence for PII detection (default: from config or 0.85)",
        ),
    ] = None,
    language: Annotated[
        Optional[str],
        typer.Option(
            "--language",
            "-l",
            help="Language for NLP analysis (default: en)",
        ),
    ] = None,
    no_pii: Annotated[
        bool,
        typer.Option(
            "--no-pii",
            help="Skip PII analysis (only discover collections)",
        ),
    ] = False,
) -> None:
    """
    Discover collections and analyze PII for a replication job.

    Generates a configuration file and PII report for the specified job.

    Examples:
        # Scan all collections
        mongorep scan prod_db

        # Scan specific collections
        mongorep scan prod_db --collections users,orders,customers

        # Interactive mode
        mongorep scan prod_db --interactive
    """
    start_time = time.time()

    try:
        # Step 1: Load job configuration and existing scan config
        print_step(1, 6, "Load Job Configuration")

        job_manager = JobManager()

        try:
            job_config = job_manager.get_job(job)
        except KeyError:
            print_error(
                f"Job '{job}' not found. Available jobs: {', '.join(job_manager.list_jobs())}"
            )
            raise typer.Exit(code=1)

        print_success(f"Loaded job '{job}'")
        print_info(f"Source: {job_config.source_uri.split('@')[-1]}")

        # Load existing config if it exists (load_config already merges with defaults)
        # Determine output path first
        if output is None:
            output_path = Path(f"config/{job}_config.yaml")
        else:
            output_path = Path(output)

        existing_config = None
        include_patterns = []
        exclude_patterns = []

        if output_path.exists():
            try:
                existing_config = load_config(output_path)
                print_info(f"Loaded existing config from {output_path}")

                # Use discovery patterns from existing config
                if existing_config.scan and existing_config.scan.discovery:
                    include_patterns = existing_config.scan.discovery.include_patterns or []
                    exclude_patterns = existing_config.scan.discovery.exclude_patterns or []
                    if include_patterns:
                        print_info(f"Using {len(include_patterns)} include pattern(s) from config")
                    if exclude_patterns:
                        print_info(f"Using {len(exclude_patterns)} exclude pattern(s) from config")
            except Exception as e:
                print_warning(f"Could not load existing config (will create new): {e}")

        # Apply CLI option precedence: CLI options > Config file (with defaults already merged)
        # CLI options always override config values
        final_sample_size = sample_size
        final_confidence = confidence_threshold
        final_language = language or "en"
        pii_enabled_from_config = True

        if existing_config and existing_config.scan:
            # Use config values if CLI options not provided (config already has defaults merged)
            if sample_size is None and existing_config.scan.sampling:
                final_sample_size = existing_config.scan.sampling.sample_size
            if confidence_threshold is None and existing_config.scan.pii_analysis:
                final_confidence = existing_config.scan.pii_analysis.confidence_threshold

            # Check if PII is enabled in config (only if --no-pii not explicitly set)
            if existing_config.scan.pii_analysis:
                pii_enabled_from_config = existing_config.scan.pii_analysis.enabled

        # Show where values came from (for transparency)
        if sample_size is not None:
            print_info(f"Sample size: {final_sample_size} (from CLI)")
        else:
            print_info(f"Sample size: {final_sample_size} (from config)")

        if confidence_threshold is not None:
            print_info(f"Confidence threshold: {final_confidence} (from CLI)")
        else:
            print_info(f"Confidence threshold: {final_confidence} (from config)")

        console.print()

        # Determine if PII analysis will run
        # Precedence: --no-pii CLI flag > scan.pii.enabled from config > default (enabled)
        should_analyze_pii = not no_pii and pii_enabled_from_config

        # Print banner with final values
        pii_status = (
            "Disabled (--no-pii)"
            if no_pii
            else ("Disabled (config)" if not pii_enabled_from_config else "Enabled")
        )
        print_banner(
            "SCAN COLLECTIONS & ANALYZE PII",
            Job=job,
            **{"Sample Size": f"{final_sample_size} docs/collection"},
            **{"Confidence": f"{final_confidence:.0%}"},
            Language=final_language.upper(),
            **{"PII Analysis": pii_status},
            Interactive="Yes" if interactive else "No",
        )

        # Apply default exclude patterns if not already set (from config with defaults merged)
        if (
            not exclude_patterns
            and existing_config
            and existing_config.scan
            and existing_config.scan.discovery
        ):
            exclude_patterns = existing_config.scan.discovery.exclude_patterns or []

        # Step 2: Connect to database and discover collections
        print_step(2, 6, "Discover Collections")

        # Parse database name from URI
        db_name = job_config.source_uri.split("/")[-1].split("?")[0]

        conn_mgr = ConnectionManager(
            source_uri=job_config.source_uri,
            dest_uri=job_config.source_uri,  # Not used for scan
            source_db_name=db_name,
            dest_db_name="unused",
        )
        source_db = conn_mgr.get_source_db()

        # Use CollectionDiscovery to apply include/exclude patterns
        from mongo_replication.engine.discovery import CollectionDiscovery

        discovery = CollectionDiscovery(
            source_db=source_db,
            replicate_all=(not include_patterns),  # If no include patterns, replicate all
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
        )

        # Discover collections with patterns applied
        discovery_result = discovery.discover_collections(configured_collections=set())
        discovered_collections = discovery_result.included_collections

        print_info(f"Found {len(discovered_collections)} collections (after filtering)")

        # Filter by --collections option if provided
        if collections:
            collection_list = [c.strip() for c in collections.split(",")]
            # Validate that specified collections exist
            invalid_collections = [c for c in collection_list if c not in discovered_collections]
            if invalid_collections:
                print_error(f"Collections not found: {', '.join(invalid_collections)}")
                print_info(f"Available collections: {', '.join(sorted(discovered_collections))}")
                raise typer.Exit(code=1)

            selected_collections = collection_list
            print_info(f"Using specified collections: {', '.join(selected_collections)}")
        elif interactive:
            # Interactive collection selection
            console.print()
            selected_collections = select_collections(discovered_collections)

            if not selected_collections:
                print_warning("No collections selected. Exiting.")
                raise typer.Exit(code=0)
        else:
            # Use all discovered collections
            selected_collections = discovered_collections
            print_success(f"Selected {len(selected_collections)} collections")

        # Step 3: Sample documents
        print_step(3, 6, "Sample Documents")

        sampler = CollectionSampler(
            database=source_db,
            sample_size=final_sample_size,
        )

        sampling_results = {}
        for collection_name in progress_wrapper(
            selected_collections,
            desc="Sampling",
            unit="collection",
        ):
            result = sampler.sample_collection(collection_name)
            sampling_results[collection_name] = result

        total_samples = sum(r.sampled_documents for r in sampling_results.values())
        print_success(
            f"Sampled {total_samples:,} documents from {len(sampling_results)} collections"
        )

        # Step 4: Load or build scan configuration (needed before PII analysis)
        print_step(4, 6, "Load Scan Configuration")

        # Get configuration values from existing config (which already has defaults merged)
        entity_types = []
        strategies = {}
        allowlist = []
        sample_strategy = "stratified"
        presidio_config = None

        if existing_config and existing_config.scan:
            if existing_config.scan.pii_analysis:
                entity_types = existing_config.scan.pii_analysis.entity_types or []
                strategies = existing_config.scan.pii_analysis.default_strategies or {}
                allowlist = existing_config.scan.pii_analysis.allowlist or []
                presidio_config = existing_config.scan.pii_analysis.presidio_config

            if existing_config.scan.sampling:
                sample_strategy = existing_config.scan.sampling.sample_strategy

        # Show entity type configuration
        if entity_types:
            print_info(f"Entity types: {', '.join(entity_types)} (from config)")
        else:
            print_info("Entity types: All types (default)")

        # Show Presidio configuration if custom config is being used
        if presidio_config:
            print_info(f"Presidio config: {presidio_config}")

        console.print()

        # Step 5: Analyze PII (optional)
        # should_analyze_pii was already determined above based on CLI flag and config
        pii_analyses = {}
        if should_analyze_pii:
            print_step(5, 6, "Analyze PII")
            print_info("This may take a few minutes (loading NLP models + analysis)...")

            try:
                analyzer = PIIAnalysisEngine(
                    confidence_threshold=final_confidence,
                    language=final_language,
                    entity_types=entity_types
                    if entity_types
                    else None,  # Pass entity types from config
                    allowlist_fields=allowlist,
                    presidio_config=presidio_config,
                )

                for collection_name in progress_wrapper(
                    list(sampling_results.keys()),
                    desc="Analyzing",
                    unit="collection",
                ):
                    sampling_result = sampling_results[collection_name]
                    analysis = analyzer.analyze_collection(sampling_result)
                    pii_analyses[collection_name] = analysis

                total_pii_fields = sum(a.pii_field_count for a in pii_analyses.values())
                collections_with_pii = sum(1 for a in pii_analyses.values() if a.has_pii)

                print_success(
                    f"Found PII in {collections_with_pii} collections ({total_pii_fields} fields total)"
                )
            except FileNotFoundError as e:
                print_error(f"Presidio configuration error: {e}")
                raise typer.Exit(code=1)
            except ValueError as e:
                print_error(f"Invalid Presidio configuration: {e}")
                raise typer.Exit(code=1)
        else:
            # PII analysis is disabled
            if no_pii:
                print_info("Skipping PII analysis (--no-pii flag set)")
            else:
                print_info("Skipping PII analysis (disabled in configuration)")
            console.print()

        # Step 6: Generate configuration file
        print_step(6, 6, "Generate Configuration")
        console.print()

        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Preserve existing scan config (don't modify based on CLI flags)
        # CLI flags are for runtime only, not for updating the saved configuration
        if existing_config and existing_config.scan:
            # Keep the existing scan configuration unchanged
            scan_config = existing_config.scan
        else:
            # No existing config - create a minimal scan config
            # This happens on first scan when no config file exists yet
            scan_config = ScanConfig(
                discovery=ScanDiscoveryConfig(
                    include_patterns=include_patterns,
                    exclude_patterns=exclude_patterns,
                ),
                sampling=ScanSamplingConfig(
                    sample_size=final_sample_size,
                    sample_strategy=sample_strategy,
                ),
                pii_analysis=ScanPIIAnalysisConfig(
                    enabled=pii_enabled_from_config,
                    confidence_threshold=final_confidence,
                    entity_types=entity_types,
                    default_strategies=strategies,
                    allowlist=allowlist,
                    presidio_config=presidio_config,
                ),
            )

        # Build replication config from PII analysis
        # Load cursor_fields and replication defaults from existing config (which has defaults merged)
        cursor_fields = []
        if existing_config and existing_config.scan and existing_config.scan.cursor_detection:
            cursor_fields = existing_config.scan.cursor_detection.cursor_fields or cursor_fields

        new_collection_configs = {}
        for collection_name in selected_collections:
            pii_config = {}
            if collection_name in pii_analyses:
                pii_config = pii_analyses[collection_name].get_pii_config()

            # Detect cursor field from sampled documents
            detected_cursor_field = None
            if collection_name in sampling_results:
                sampling_result = sampling_results[collection_name]
                if sampling_result.sample_docs:
                    # Use first document to detect cursor field
                    sample_doc = sampling_result.sample_docs[0]
                    detected_cursor_field = detect_cursor_field(
                        collection_name, sample_doc, cursor_fields
                    )
                    if detected_cursor_field:
                        print_info(
                            f"Detected cursor field '{detected_cursor_field}' for collection '{collection_name}'"
                        )

            # Add collection to config (even if no PII fields)
            # This ensures all scanned collections get a replication config entry
            new_collection_configs[collection_name] = CollectionConfig(
                name=collection_name,
                cursor_field=detected_cursor_field,  # Use detected field or None
                write_disposition="merge",
                primary_key="_id",
                pii_anonymized_fields=pii_config,
            )

        # Merge with existing collections if config exists
        merged_collections_dict = {}
        defaults_dict = {}

        if existing_config and existing_config.replication:
            # Start with existing collections (convert CollectionConfig objects to dicts)
            # Exclude 'name' field since CollectionsConfig validator will set it from the key
            for coll_name, coll_config in existing_config.replication.collections.items():
                coll_dict = coll_config.model_dump()
                coll_dict.pop("name", None)  # Remove 'name' to avoid duplicate in validator
                merged_collections_dict[coll_name] = coll_dict

            # Use existing defaults (convert Pydantic model to dict)
            if existing_config.replication.defaults:
                defaults_dict = existing_config.replication.defaults.model_dump()

            # Update/add newly scanned collections (convert to dict)
            for coll_name, coll_config in new_collection_configs.items():
                if coll_name in merged_collections_dict:
                    print_info(f"Updating existing config for collection: {coll_name}")
                coll_dict = coll_config.model_dump()
                coll_dict.pop("name", None)  # Remove 'name' to avoid duplicate in validator
                merged_collections_dict[coll_name] = coll_dict
        else:
            # No existing config, use new collections (convert to dict)
            for coll_name, coll_config in new_collection_configs.items():
                coll_dict = coll_config.model_dump()
                coll_dict.pop("name", None)  # Remove 'name' to avoid duplicate in validator
                merged_collections_dict[coll_name] = coll_dict

        config = Config(
            scan=scan_config,
            replication=ReplicationConfig(
                defaults=defaults_dict,
                collections=merged_collections_dict,
            ),
        )

        # Save configuration
        save_config(config, output_path)

        if existing_config:
            updated_count = len(
                [
                    c
                    for c in selected_collections
                    if c
                    in (
                        existing_config.replication.collections
                        if existing_config.replication
                        else {}
                    )
                ]
            )
            added_count = len(selected_collections) - updated_count
            print_success(
                f"Merged configuration to {output_path} "
                f"(updated: {updated_count}, added: {added_count}, "
                f"total: {len(merged_collections_dict)} collections)"
            )
        else:
            print_success(f"Saved configuration to {output_path}")

        # Generate markdown PII report
        if not no_pii:
            report_path = output_path.parent / f"{job}_pii_report.md"
            generate_pii_report(
                job_id=job,
                pii_analyses=pii_analyses,
                output_path=report_path,
            )
            print_success(f"Saved PII report to {report_path}")

        # Print summary
        elapsed = time.time() - start_time
        print_summary(
            "Scan Complete",
            {
                "Collections Scanned": len(selected_collections),
                "Documents Sampled": f"{total_samples:,}",
                "PII Fields Found": total_pii_fields if not no_pii else "N/A",
                "Config File": str(output_path),
                "Time Elapsed": f"{elapsed:.1f}s",
            },
        )

    except KeyboardInterrupt:
        console.print()
        print_warning("Scan cancelled by user")
        raise typer.Exit(code=130)
    except Exception as e:
        console.print()
        print_error(f"Scan failed: {e}")
        raise typer.Exit(code=1)
