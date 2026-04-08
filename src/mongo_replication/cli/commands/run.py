"""
Run command - execute replication job.

Usage:
    mongorep run <job> [OPTIONS]
"""

import time
import json
from pathlib import Path
from typing import Optional, List, Dict, Any

import typer
from pymongo import MongoClient
from rich.live import Live
from rich.table import Table
from typing_extensions import Annotated

from mongo_replication.cli.interactive.selectors import select_collections
from mongo_replication.cli.utils.cascade_tree import CascadeTreeBuilder
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
from mongo_replication.config.manager import load_replication_config, load_schema_relationships
from mongo_replication.config.models import CollectionConfig
from mongo_replication.engine.cascade_filter import CascadeFilterBuilder, CascadeResult
from mongo_replication.engine.connection import ConnectionManager
from mongo_replication.engine.jobs import JobManager
from mongo_replication.engine.orchestrator import ReplicationOrchestrator
from mongo_replication.engine.relationships import RelationshipGraph, Relationship
from mongo_replication.engine.replicator import ReplicationResult


def parse_ids_option(ids_str: str) -> tuple[str, List[str]]:
    """
    Parse --ids option.

    Args:
        ids_str: IDs string (e.g., "customers=id1,id2,id3")

    Returns:
        Tuple of (collection_name, list_of_ids)

    Raises:
        ValueError: If format is invalid

    Examples:
        "customers=id1,id2" -> ("customers", ["id1", "id2"])
        "users=abc123" -> ("users", ["abc123"])
    """
    if "=" not in ids_str:
        raise ValueError(
            f"Invalid --ids format: '{ids_str}'. Expected format: collection=id1,id2,id3"
        )

    parts = ids_str.split("=", 1)
    collection = parts[0].strip()
    ids_value = parts[1].strip()

    if not collection:
        raise ValueError("Collection name cannot be empty in --ids")

    if not ids_value:
        raise ValueError(f"No IDs provided for collection '{collection}'")

    # Split by comma and strip whitespace
    ids = [id.strip() for id in ids_value.split(",")]
    ids = [id for id in ids if id]  # Remove empty strings

    if not ids:
        raise ValueError(f"No valid IDs provided for collection '{collection}'")

    return collection, ids


def parse_query_option(query_str: str) -> tuple[str, Dict[str, Any]]:
    """
    Parse --query option.

    Args:
        query_str: Query string (e.g., 'customers={"plan": "Basic"}')

    Returns:
        Tuple of (collection_name, mongodb_query_dict)

    Raises:
        ValueError: If format is invalid or JSON parsing fails

    Examples:
        'customers={"plan": "Basic"}' -> ("customers", {"plan": "Basic"})
        'users={"age": {"$gt": 18}}' -> ("users", {"age": {"$gt": 18}})
    """
    if "=" not in query_str:
        raise ValueError(
            f'Invalid --query format: \'{query_str}\'. Expected format: collection={{"field": "value"}}'
        )

    parts = query_str.split("=", 1)
    collection = parts[0].strip()
    query_json = parts[1].strip()

    if not collection:
        raise ValueError("Collection name cannot be empty in --query")

    if not query_json:
        raise ValueError(f"No query provided for collection '{collection}'")

    # Parse JSON query
    try:
        query_dict = json.loads(query_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in query for collection '{collection}': {e}")

    if not isinstance(query_dict, dict):
        raise ValueError(f"Query must be a JSON object (dict), got {type(query_dict).__name__}")

    return collection, query_dict


def run_command(
    job: Annotated[str, typer.Argument(help="Job ID to run (e.g., 'prod_db')")],
    collections: Annotated[
        Optional[str],
        typer.Option(
            "--collections",
            help="Comma-separated list of collections to replicate (default: all configured)",
        ),
    ] = None,
    interactive: Annotated[
        bool,
        typer.Option(
            "--interactive",
            "-i",
            help="Interactively select collections to replicate",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Preview what would be replicated without executing",
        ),
    ] = False,
    parallel: Annotated[
        Optional[int],
        typer.Option(
            "--parallel",
            "-p",
            help="Maximum number of parallel collections (default: from config or 5)",
        ),
    ] = None,
    batch_size: Annotated[
        Optional[int],
        typer.Option(
            "--batch-size",
            "-b",
            help="Batch size for document processing",
        ),
    ] = None,
    ids: Annotated[
        Optional[str],
        typer.Option(
            "--ids",
            help=(
                "Cascade replication from specific documents IDs. "
                "Format: collection=id1,id2,id3 "
                "(e.g., --ids customers=507f1f77bcf86cd799439011,507f191e810c19729de860ea)"
            ),
        ),
    ] = None,
    query: Annotated[
        Optional[str],
        typer.Option(
            "--query",
            help=(
                "Cascade replication from documents matching a MongoDB query. "
                'Format: collection={"field": "value"} '
                '(e.g., --query customers={"plan": "Basic"})'
            ),
        ),
    ] = None,
) -> None:
    """
    Execute replication for a job.

    Replicates data from source to destination MongoDB with PII handling.

    Examples:
        # Replicate all configured collections
        mongorep run prod_db

        # Replicate specific collections
        mongorep run prod_db --collections users,orders

        # Interactive mode
        mongorep run prod_db --interactive

        # Dry run to preview
        mongorep run prod_db --dry-run

        # Cascade replication from specific customer IDs
        mongorep run prod_db --ids customers=507f1f77bcf86cd799439011,507f191e810c19729de860ea

        # Cascade replication from documents matching a query
        mongorep run prod_db --query customers='{"plan": "Basic"}'
    """
    start_time = time.time()

    # Validate mutually exclusive options
    if ids and query:
        print_error(
            "--ids and --query cannot be used together. Choose one cascade replication method."
        )
        raise typer.Exit(code=1)

    if (ids or query) and (collections or interactive):
        print_error(
            "--ids/--query cannot be used with --collections or --interactive. "
            "Cascade options automatically select all related collections."
        )
        raise typer.Exit(code=1)

    try:
        # Step 1: Load job configuration
        print_step(1, 4, "Load Job Configuration")

        job_manager = JobManager()

        try:
            job_config = job_manager.get_job(job)
        except ValueError as e:
            print_error(str(e))
            print_info(f"Available jobs: {', '.join(job_manager.list_jobs())}")
            raise typer.Exit(code=1)

        # Check for config path
        if not job_config.config_path:
            print_error(
                f"No config path set for job '{job}'. "
                f"Set MONGOREP_{job.upper()}_CONFIG_PATH environment variable."
            )
            raise typer.Exit(code=1)

        config_file = Path(job_config.config_path)
        if not config_file.exists():
            print_error(f"Config file not found: {config_file}")
            print_info(f"Run 'mongorep scan {job}' to generate the config file.")
            raise typer.Exit(code=1)

        print_success(f"Loaded job '{job}'")
        print_info(f"Source: {job_config.source_uri.split('@')[-1]}")
        print_info(f"Destination: {job_config.destination_uri.split('@')[-1]}")
        print_info(f"Config: {config_file}")

        # Step 2: Load replication configuration
        print_step(2, 4, "Load Replication Configuration")

        try:
            replication_config = load_replication_config(config_file)
        except Exception as e:
            print_error(f"Failed to load config: {e}")
            raise typer.Exit(code=1)

        # Apply precedence: CLI options > Config > Defaults
        default_parallel = 5
        default_batch_size = 1000

        # Get values from config if available
        config_parallel = replication_config.performance.max_parallel_collections
        config_batch_size = replication_config.performance.batch_size

        # Resolve final values with precedence
        if parallel is not None:
            final_parallel = parallel
            parallel_source = "CLI"
        elif config_parallel is not None:
            final_parallel = config_parallel
            parallel_source = "config"
        else:
            final_parallel = default_parallel
            parallel_source = "default"

        if batch_size is not None:
            final_batch_size = batch_size
            batch_size_source = "CLI"
        elif config_batch_size is not None:
            final_batch_size = config_batch_size
            batch_size_source = "config"
        else:
            final_batch_size = default_batch_size
            batch_size_source = "default"

        # Show transparency messages
        print_info(f"Max parallel collections: {final_parallel} (from {parallel_source})")
        print_info(f"Batch size: {final_batch_size} (from {batch_size_source})")

        console.print()

        # Print banner with resolved values
        print_banner(
            "RUN REPLICATION",
            Job=job,
            **{"Max Parallel": f"{final_parallel} collections"},
            **{"Batch Size": str(final_batch_size)},
            Interactive="Yes" if interactive else "No",
            **{"Dry Run": "Yes" if dry_run else "No"},
        )

        # Apply final values to config
        replication_config.performance.max_parallel_collections = final_parallel
        replication_config.performance.batch_size = final_batch_size

        configured_collections = list(replication_config.collections.keys())
        print_success(f"Loaded configuration with {len(configured_collections)} collections")

        # Initialize cascade mode variables (will be populated if --ids is used)
        graph = None
        root_collection = None
        cascade_result = None

        # Handle cascade replication with --ids option
        if ids:
            console.print()
            print_step("2a", 4, "Build Cascade Filters")

            try:
                # Parse the ids option
                root_collection, root_ids = parse_ids_option(ids)
                print_info(f"Root collection: {root_collection}")
                print_info(f"Root IDs: {len(root_ids)} provided")

                # Load schema relationships
                try:
                    schema_relationships = load_schema_relationships(config_file)
                    print_success(f"Loaded {len(schema_relationships)} relationships from schema")
                except ValueError as e:
                    print_error(str(e))
                    print_info(f"See config file: {config_file}")
                    raise typer.Exit(code=1)

                # Build relationship graph from schema
                relationships = [
                    Relationship(
                        parent=rel.parent,
                        child=rel.child,
                        parent_field=rel.parent_field,
                        child_field=rel.child_field,
                    )
                    for rel in schema_relationships
                ]

                graph = RelationshipGraph(relationships)

                # Validate graph has no cycles
                if graph.has_cycles():
                    print_error(
                        "Circular dependencies detected in relationships. "
                        "Cascade replication requires a DAG (directed acyclic graph)."
                    )
                    raise typer.Exit(code=1)

                # Connect to source DB temporarily for filter building
                print_info("Connecting to source database...")
                source_db_name = job_config.source_uri.split("/")[-1].split("?")[0]
                source_client = MongoClient(job_config.source_uri)
                source_db = source_client[source_db_name]

                # Validate collections exist in source
                try:
                    existing_collections = set(source_db.list_collection_names())
                    graph.validate_collections(existing_collections)
                except ValueError as e:
                    print_error(str(e))
                    source_client.close()
                    raise typer.Exit(code=1)

                # Build cascade filters
                print_info("Building cascade filters...")
                builder = CascadeFilterBuilder(source_db, graph)

                try:
                    cascade_result: CascadeResult = builder.build_filters(root_collection, root_ids)
                except ValueError as e:
                    print_error(f"Filter building failed: {e}")
                    source_client.close()
                    raise typer.Exit(code=1)

                source_client.close()

                # Apply filters to collection configs
                affected_collections = set(cascade_result.filters.keys())
                print_success(f"Built filters for {len(affected_collections)} collections")

                # Remove collections not in cascade
                for coll_name in list(replication_config.collections.keys()):
                    if coll_name not in affected_collections:
                        del replication_config.collections[coll_name]

                # Add or update collection configurations for cascade mode
                for coll_name in affected_collections:
                    if coll_name in cascade_result.skipped_collections:
                        # Skip collections with 0 documents
                        continue

                    # Get or create collection config
                    if coll_name in replication_config.collections:
                        coll_config = replication_config.collections[coll_name]
                    else:
                        # Create new CollectionConfig with defaults
                        coll_config = CollectionConfig(
                            name=coll_name,
                            cursor_field=None,
                            write_disposition="replace",
                            primary_key="_id",
                            pii_anonymized_fields={},
                            match=None,
                            field_transforms=[],
                            fields_exclude=[],
                            transform_error_mode="skip",
                        )

                    # Apply match filter
                    cascade_filter = cascade_result.filters[coll_name]
                    if coll_config.match:
                        # Combine existing match with cascade filter using $and
                        coll_config.match = {"$and": [coll_config.match, cascade_filter]}
                    else:
                        coll_config.match = cascade_filter

                    # Override write disposition to replace (drop+insert)
                    coll_config.write_disposition = "replace"

                    # Disable incremental loading (cursor-based)
                    coll_config.cursor_field = None

                    replication_config.collections[coll_name] = coll_config

                # Collections to actually replicate (excluding skipped)
                collections_to_replicate = affected_collections - cascade_result.skipped_collections

                # Disable auto-discovery and set include patterns
                replication_config.discovery.replicate_all = False
                replication_config.discovery.include_patterns = [
                    f"^{coll}$" for coll in collections_to_replicate
                ]

                print_info(
                    f"Cascade mode: {len(collections_to_replicate)} collections to replicate"
                )
                if cascade_result.skipped_collections:
                    print_warning(
                        f"Skipped collections (0 docs): {', '.join(sorted(cascade_result.skipped_collections))}"
                    )

                # Debug: Show filters being applied
                # console.print()
                # print_info("Applied filters:")
                # for coll_name in sorted(replication_config.collections.keys()):
                #     filter_str = str(replication_config.collections[coll_name].match)
                #     if len(filter_str) > 100:
                #         filter_str = filter_str[:97] + "..."
                #     print_info(f"  • {coll_name}: {filter_str}")
                # console.print()

            except ValueError as e:
                print_error(f"Invalid --ids option: {e}")
                raise typer.Exit(code=1)
            except Exception as e:
                print_error(f"Cascade filter building failed: {e}")
                raise typer.Exit(code=1)

        # Handle cascade replication with --query option
        elif query:
            console.print()
            print_step("2a", 4, "Build Cascade Filters")

            try:
                # Parse the query option
                root_collection, root_query = parse_query_option(query)
                print_info(f"Root collection: {root_collection}")
                print_info(f"Query: {json.dumps(root_query, default=str)}")

                # Load schema relationships
                try:
                    schema_relationships = load_schema_relationships(config_file)
                    print_success(f"Loaded {len(schema_relationships)} relationships from schema")
                except ValueError as e:
                    print_error(str(e))
                    print_info(f"See config file: {config_file}")
                    raise typer.Exit(code=1)

                # Build relationship graph from schema
                relationships = [
                    Relationship(
                        parent=rel.parent,
                        child=rel.child,
                        parent_field=rel.parent_field,
                        child_field=rel.child_field,
                    )
                    for rel in schema_relationships
                ]

                graph = RelationshipGraph(relationships)

                # Validate graph has no cycles
                if graph.has_cycles():
                    print_error(
                        "Circular dependencies detected in relationships. "
                        "Cascade replication requires a DAG (directed acyclic graph)."
                    )
                    raise typer.Exit(code=1)

                # Connect to source DB temporarily for filter building
                print_info("Connecting to source database...")
                source_db_name = job_config.source_uri.split("/")[-1].split("?")[0]
                source_client = MongoClient(job_config.source_uri)
                source_db = source_client[source_db_name]

                # Validate collections exist in source
                try:
                    existing_collections = set(source_db.list_collection_names())
                    graph.validate_collections(existing_collections)
                except ValueError as e:
                    print_error(str(e))
                    source_client.close()
                    raise typer.Exit(code=1)

                # Build cascade filters from query
                print_info("Building cascade filters from query...")
                builder = CascadeFilterBuilder(source_db, graph)

                try:
                    cascade_result: CascadeResult = builder.build_filters_from_query(
                        root_collection, root_query
                    )
                except ValueError as e:
                    print_error(f"Filter building failed: {e}")
                    source_client.close()
                    raise typer.Exit(code=1)

                source_client.close()

                # Apply filters to collection configs
                affected_collections = set(cascade_result.filters.keys())
                print_success(f"Built filters for {len(affected_collections)} collections")

                # Remove collections not in cascade
                for coll_name in list(replication_config.collections.keys()):
                    if coll_name not in affected_collections:
                        del replication_config.collections[coll_name]

                # Add or update collection configurations for cascade mode
                for coll_name in affected_collections:
                    if coll_name in cascade_result.skipped_collections:
                        # Skip collections with 0 documents
                        continue

                    # Get or create collection config
                    if coll_name in replication_config.collections:
                        coll_config = replication_config.collections[coll_name]
                    else:
                        # Create new CollectionConfig with defaults
                        coll_config = CollectionConfig(
                            name=coll_name,
                            cursor_field=None,
                            write_disposition="replace",
                            primary_key="_id",
                            pii_anonymized_fields={},
                            match=None,
                            field_transforms=[],
                            fields_exclude=[],
                            transform_error_mode="skip",
                        )
                        replication_config.collections[coll_name] = coll_config

                    # Set match filter from cascade
                    coll_config.match = cascade_result.filters[coll_name]

                print_success(
                    f"Configured {len(affected_collections) - len(cascade_result.skipped_collections)} "
                    f"collections with cascade filters"
                )

            except ValueError as e:
                print_error(f"Invalid --query option: {e}")
                raise typer.Exit(code=1)
            except Exception as e:
                print_error(f"Cascade filter building failed: {e}")
                raise typer.Exit(code=1)

        # Filter by --collections option if provided
        elif collections:
            collection_list = [c.strip() for c in collections.split(",")]
            # Validate that specified collections exist in config
            invalid_collections = [c for c in collection_list if c not in configured_collections]
            if invalid_collections:
                print_error(f"Collections not found in config: {', '.join(invalid_collections)}")
                print_info(f"Configured collections: {', '.join(sorted(configured_collections))}")
                raise typer.Exit(code=1)

            # Filter configuration to only specified collections
            replication_config.collections = {
                name: config
                for name, config in replication_config.collections.items()
                if name in collection_list
            }
            # Disable auto-discovery and set include patterns for specified collections
            replication_config.discovery.replicate_all = False
            replication_config.discovery.include_patterns = [
                f"^{coll}$" for coll in collection_list
            ]
            print_info(f"Using specified collections: {', '.join(collection_list)}")
        elif interactive and configured_collections:
            # Interactive collection selection
            console.print()

            # Filter configured collections by discovery patterns
            collections_to_show = configured_collections
            if replication_config.discovery:
                import re

                # Apply include patterns
                if replication_config.discovery.include_patterns:
                    include_regexes = [
                        re.compile(pattern)
                        for pattern in replication_config.discovery.include_patterns
                    ]
                    collections_to_show = [
                        coll
                        for coll in collections_to_show
                        if any(regex.match(coll) for regex in include_regexes)
                    ]

                # Apply exclude patterns
                if replication_config.discovery.exclude_patterns:
                    exclude_regexes = [
                        re.compile(pattern)
                        for pattern in replication_config.discovery.exclude_patterns
                    ]
                    collections_to_show = [
                        coll
                        for coll in collections_to_show
                        if not any(regex.match(coll) for regex in exclude_regexes)
                    ]

            selected_collections = select_collections(collections_to_show)

            if not selected_collections:
                print_warning("No collections selected. Exiting.")
                raise typer.Exit(code=0)

            # Filter configuration to only selected collections
            replication_config.collections = {
                name: config
                for name, config in replication_config.collections.items()
                if name in selected_collections
            }
            # Disable auto-discovery and set include patterns for selected collections
            replication_config.discovery.replicate_all = False
            replication_config.discovery.include_patterns = [
                f"^{coll}$" for coll in selected_collections
            ]

            print_success(f"Selected {len(selected_collections)} collections")

        # Dry run - just show what would be replicated
        if dry_run:
            console.print()
            console.rule("[bold]Dry Run - Collections to Replicate[/bold]", style="yellow")
            console.print()

            if ids or query:
                # Show tree visualization for cascade mode
                tree_structure = graph.get_tree_structure(root_collection)
                tree = CascadeTreeBuilder.build_dry_run_tree(
                    tree_structure=tree_structure,
                    doc_counts=cascade_result.doc_counts,
                    skipped=cascade_result.skipped_collections,
                )
                console.print(tree)
                console.print()

                # Show summary stats
                total_docs = sum(cascade_result.doc_counts.values())
                print_info(f"Total documents to replicate: {total_docs:,}")
                print_info(f"Collections: {len(cascade_result.doc_counts)}")
                if cascade_result.skipped_collections:
                    print_info(f"Skipped: {len(cascade_result.skipped_collections)}")
            else:
                # Show standard collection list
                for coll_name, coll_config in replication_config.collections.items():
                    pii_count = (
                        len(coll_config.pii_anonymized_fields)
                        if coll_config.pii_anonymized_fields
                        else 0
                    )
                    console.print(f"  • [cyan]{coll_name}[/cyan] (PII fields: {pii_count})")

            console.print()
            print_info("Dry run complete. No data was replicated.")
            raise typer.Exit(code=0)

        # Step 3: Connect to databases
        print_step(3, 4, "Connect to Databases")

        # Parse database names from URIs
        source_db_name = job_config.source_uri.split("/")[-1].split("?")[0]
        dest_db_name = job_config.destination_uri.split("/")[-1].split("?")[0]

        try:
            conn_mgr = ConnectionManager(
                source_uri=job_config.source_uri,
                dest_uri=job_config.destination_uri,
                source_db_name=source_db_name,
                dest_db_name=dest_db_name,
            )
        except ValueError as e:
            print_error(str(e))
            raise typer.Exit(code=1)

        print_success("Connected to source and destination")

        # Step 4: Execute replication
        print_step(4, 4, "Run Replication")

        orchestrator = ReplicationOrchestrator(
            connection_manager=conn_mgr,
            config=replication_config,
        )

        # Get max parallel collections for progress display
        max_parallel = replication_config.performance.max_parallel_collections

        # Run replication with progress bars
        console.print()
        console.print(f"[bold]Replicating collections (max {max_parallel} parallel)...[/bold]\n")

        # Track collection states
        replicating_collections = {}  # name -> start_time
        completed_collections = []  # list of (name, result)
        failed_collections = []  # list of (name, error)
        total_collections = 0

        def create_progress_display():
            """Create progress display (table or tree based on mode)."""
            if ids or query:
                # Cascade mode: use tree visualization
                # Build status map and error map
                status_map = {}
                error_map = {}

                # Pending collections (not started yet)
                all_collections = set(cascade_result.filters.keys())
                started_collections = (
                    set(replicating_collections.keys())
                    | {name for name, _ in completed_collections}
                    | {name for name, _ in failed_collections}
                )
                pending_collections = all_collections - started_collections

                for coll_name in pending_collections:
                    status_map[coll_name] = "pending"

                # Replicating collections
                for coll_name in replicating_collections:
                    status_map[coll_name] = "replicating"

                # Completed collections
                for coll_name, result in completed_collections:
                    status_map[coll_name] = "completed"

                # Failed collections
                for coll_name, error in failed_collections:
                    status_map[coll_name] = "failed"
                    error_map[coll_name] = error

                # Skipped collections
                for coll_name in cascade_result.skipped_collections:
                    status_map[coll_name] = "skipped"

                tree_structure = graph.get_tree_structure(root_collection)
                return CascadeTreeBuilder.build_progress_tree(
                    tree_structure=tree_structure,
                    doc_counts=cascade_result.doc_counts,
                    statuses=status_map,
                    errors=error_map,
                )
            else:
                # Standard mode: use table
                table = Table.grid(padding=(0, 2))
                table.add_column("Status", style="bold", width=2)
                table.add_column("Collection", style="cyan")
                table.add_column("Details", style="dim")

                # Overall stats
                completed_count = len(completed_collections)
                failed_count = len(failed_collections)
                total_processed = completed_count + failed_count

                if total_collections > 0:
                    table.add_row(
                        "📊",
                        "[bold]Overall Progress[/bold]",
                        f"{total_processed}/{total_collections} collections",
                    )
                    table.add_row("", "", "")  # Spacer

                # Replicating section
                if replicating_collections:
                    table.add_row("", "[bold yellow]⏳ Replicating[/bold yellow]", "")
                    for coll_name, start_time in sorted(replicating_collections.items()):
                        elapsed = time.time() - start_time
                        table.add_row("", f"  {coll_name}", f"[dim]{elapsed:.0f}s[/dim]")
                    table.add_row("", "", "")  # Spacer

                # Completed section
                if completed_collections:
                    table.add_row(
                        "",
                        f"[bold green]✓ Completed ({len(completed_collections)})[/bold green]",
                        "",
                    )
                    # Show last 5 completed
                    for coll_name, result in completed_collections[-5:]:
                        docs = result.documents_processed if result else 0
                        duration = result.duration_seconds if result else 0
                        indexes = result.indexes_replicated if result else 0
                        details = f"{docs:,} docs, {indexes} indexes, {duration:.1f}s"
                        table.add_row("", f"  {coll_name}", f"[dim]{details}[/dim]")
                    if len(completed_collections) > 5:
                        table.add_row(
                            "", f"  [dim]... and {len(completed_collections) - 5} more[/dim]", ""
                        )
                    table.add_row("", "", "")  # Spacer

                # Failed section
                if failed_collections:
                    table.add_row(
                        "", f"[bold red]✗ Failed ({len(failed_collections)})[/bold red]", ""
                    )
                    for coll_name, error in failed_collections:
                        # Truncate error message
                        error_msg = error[:60] + "..." if len(error) > 60 else error
                        table.add_row("", f"  {coll_name}", f"[dim red]{error_msg}[/dim red]")

                return table

        # Use Live display for dynamic updates
        with Live(create_progress_display(), console=console, refresh_per_second=2) as live:

            def on_progress(collection_name: str, status: str, result: Optional[ReplicationResult]):
                """Progress callback for orchestrator."""
                nonlocal total_collections

                if status == "started":
                    # Set total on first started event
                    if total_collections == 0:
                        # Count will be set as collections start
                        pass

                    # Add to replicating
                    replicating_collections[collection_name] = time.time()
                    total_collections = (
                        len(replicating_collections)
                        + len(completed_collections)
                        + len(failed_collections)
                    )

                elif status == "completed":
                    # Move from replicating to completed
                    if collection_name in replicating_collections:
                        del replicating_collections[collection_name]
                    completed_collections.append((collection_name, result))

                elif status == "failed":
                    # Move from replicating to failed
                    if collection_name in replicating_collections:
                        del replicating_collections[collection_name]
                    error = result.error_message if result else "Unknown error"
                    failed_collections.append((collection_name, error))

                # Update display
                live.update(create_progress_display())

            # Run replication with progress callback
            result = orchestrator.replicate(progress_callback=on_progress)

        console.print()

        # Print summary
        elapsed = time.time() - start_time

        if result.failed_collections:
            status = f"[yellow]Completed with {len(result.failed_collections)} failures[/yellow]"
        else:
            status = "[green]Success[/green]"

        print_summary(
            "Replication Complete",
            {
                "Status": status,
                "Collections Processed": result.total_collections_processed,
                "Successful": len(result.successful_collections),
                "Failed": len(result.failed_collections),
                "Documents Replicated": f"{result.total_documents_processed:,}",
                "Time Elapsed": f"{elapsed:.1f}s",
            },
        )

        # Exit with error code if any collections failed
        if result.failed_collections:
            raise typer.Exit(code=1)

    except (KeyboardInterrupt, EOFError):
        console.print()
        print_warning("Replication cancelled by user")
        raise typer.Exit(code=130)
    except typer.Exit:
        # Re-raise typer exits (dry-run, user cancellations, etc.)
        raise
    except Exception as e:
        console.print()
        print_error(f"Replication failed: {e}")
        raise typer.Exit(code=1)
