"""
Init command - interactive setup for scan configuration.

Usage:
    mongorep init <job> [OPTIONS]
"""

import logging
from pathlib import Path
from typing import Optional, List, Dict

import questionary
import typer
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from questionary import Style
from typing_extensions import Annotated

from mongo_replication.cli.utils.output import (
    print_banner,
    print_success,
    print_error,
    print_warning,
    print_step,
    console,
)
from mongo_replication.config.manager import save_config, load_defaults
from mongo_replication.config.models import (
    ScanConfig,
    ScanDiscoveryConfig,
    ScanSamplingConfig,
    ScanPIIAnalysisConfig,
    ScanCursorDetectionConfig,
    Config,
    ReplicationConfig,
    ReplicationDiscoveryConfig,
)
from mongo_replication.engine.connection import ConnectionManager
from mongo_replication.config.presidio_config import PresidioConfig

logger = logging.getLogger(__name__)

# Custom style for questionary
custom_style = Style(
    [
        ("qmark", "fg:#673ab7 bold"),
        ("question", "bold"),
        ("answer", "fg:#2196f3 bold"),
        ("pointer", "fg:#673ab7 bold"),
        ("highlighted", "fg:#673ab7 bold"),
        ("selected", "fg:#2196f3"),
        ("separator", "fg:#cc5454"),
        ("instruction", ""),
        ("text", ""),
        ("disabled", "fg:#858585 italic"),
    ]
)


def load_entity_strategies_from_config(
    presidio_config_path: Optional[str] = None,
) -> Dict[str, str]:
    """Load entity type to strategy mappings from Presidio YAML config.

    Args:
        presidio_config_path: Optional path to custom Presidio config.
                             If None, uses the bundled default configuration.

    Returns:
        Dictionary mapping entity types to operator names.
        Example: {"EMAIL_ADDRESS": "smart_redact", "PERSON": "replace", ...}
    """
    try:
        presidio_config = PresidioConfig(presidio_config_path)
        operator_configs = presidio_config.get_operator_configs()

        # Extract just the operator names from OperatorConfig objects
        entity_strategies = {}
        for entity_type, operator_config in operator_configs.items():
            entity_strategies[entity_type] = operator_config.operator_name

        return entity_strategies
    except Exception as e:
        logger.warning(f"Failed to load Presidio config: {e}, using minimal defaults")
        # Fallback to minimal defaults if config loading fails
        return {
            "EMAIL_ADDRESS": "redact",
            "PERSON": "redact",
            "PHONE_NUMBER": "redact",
            "CREDIT_CARD": "redact",
            "DEFAULT": "redact",
        }


def validate_connection(uri: str, db_name: str) -> bool:
    """Validate MongoDB connection.

    Args:
        uri: MongoDB connection URI
        db_name: Database name

    Returns:
        True if connection successful, False otherwise
    """
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        # Force connection
        client.admin.command("ping")
        # Try to access the database
        db = client[db_name]
        db.list_collection_names(comment="Testing connection")
        client.close()
        return True
    except (ConnectionFailure, OperationFailure) as e:
        print_error(f"Connection failed: {e}")
        return False
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        return False


def get_collections_from_source(uri: str, db_name: str) -> Optional[List[str]]:
    """Get list of collections from source database.

    Args:
        uri: MongoDB connection URI
        db_name: Database name

    Returns:
        List of collection names or None if failed
    """
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        db = client[db_name]
        collections = db.list_collection_names(comment="Listing collections")
        client.close()
        return collections
    except Exception as e:
        print_error(f"Failed to list collections: {e}")
        return None


def init_command(
    job: Annotated[str, typer.Argument(help="Job ID (e.g., 'prod_db', 'staging_db')")],
    output: Annotated[
        Optional[Path],
        typer.Option(
            "--output",
            "-o",
            help="Output config file path (default: config/<job>_config.yaml)",
        ),
    ] = None,
) -> None:
    """
    Initialize scan configuration interactively.

    Guides you through setting up:
    - Source and destination MongoDB URIs
    - Collection selection (include/exclude patterns)
    - PII Analysis settings
    - Anonymization strategies

    Examples:
        # Initialize configuration for prod_db job
        mongorep init prod_db

        # Specify custom output path
        mongorep init prod_db --output /path/to/config.yaml
    """
    print_banner("SETUP CONFIGURATION", Job=job)

    console.print()
    console.print(
        "[bold]This wizard will help you set up the Mongo Replication Tool configuration.[/bold]"
    )
    console.print()

    # Step 1: Source URI
    print_step(1, 8, "Source Database Connection")
    console.print()

    source_uri = questionary.text(
        "Enter source MongoDB URI:",
        style=custom_style,
        instruction="(e.g., mongodb://localhost:27017 or mongodb+srv://...)",
    ).ask()

    if not source_uri:
        print_error("Source URI is required")
        raise typer.Exit(code=1)

    # Parse database name from URI
    source_db_name = source_uri.split("/")[-1].split("?")[0]
    if not source_db_name:
        source_db_name = questionary.text(
            "Enter source database name:",
            style=custom_style,
        ).ask()

        if not source_db_name:
            print_error("Source database name is required")
            raise typer.Exit(code=1)

    # Test source connection
    console.print()
    with console.status("[bold blue]Testing source connection..."):
        if not validate_connection(source_uri, source_db_name):
            raise typer.Exit(code=1)

    print_success(f"Connected to source database: {source_db_name}")

    # Step 2: Destination URI
    print_step(2, 8, "Destination Database Connection")
    console.print()

    dest_uri = questionary.text(
        "Enter destination MongoDB URI:",
        style=custom_style,
        instruction="(e.g., mongodb://localhost:27017 or mongodb+srv://...)",
    ).ask()

    if not dest_uri:
        print_error("Destination URI is required")
        raise typer.Exit(code=1)

    # Parse database name from URI
    dest_db_name = dest_uri.split("/")[-1].split("?")[0]
    if not dest_db_name:
        dest_db_name = questionary.text(
            "Enter destination database name:",
            style=custom_style,
        ).ask()

        if not dest_db_name:
            print_error("Destination database name is required")
            raise typer.Exit(code=1)

    # Test destination connection
    console.print()
    with console.status("[bold blue]Testing destination connection..."):
        if not validate_connection(dest_uri, dest_db_name):
            raise typer.Exit(code=1)

    print_success(f"Connected to destination database: {dest_db_name}")

    # Validate that source and destination are different databases
    console.print()
    with console.status("[bold blue]Validating database configuration..."):
        try:
            # This will raise ValueError if source and destination point to same database
            ConnectionManager(
                source_uri=source_uri,
                dest_uri=dest_uri,
                source_db_name=source_db_name,
                dest_db_name=dest_db_name,
            )
        except ValueError as e:
            print_error(str(e))
            raise typer.Exit(code=1)

    print_success("Source and destination databases are different")

    # Step 3: Collection Discovery
    print_step(3, 8, "Collection Discovery")
    console.print()

    # Get collections from source
    with console.status("[bold blue]Discovering collections..."):
        collections = get_collections_from_source(source_uri, source_db_name)

    if collections is None:
        raise typer.Exit(code=1)

    print_success(f"Found {len(collections)} collections in source database")
    console.print()

    # Ask how to filter collections
    filter_mode = questionary.select(
        "How do you want to select collections?",
        choices=[
            "Replicate all collections",
            "Select specific collections",
            "Use include/exclude patterns",
        ],
        style=custom_style,
    ).ask()

    include_patterns = []
    exclude_patterns = []

    if filter_mode == "Select specific collections":
        # Show collection checkboxes
        selected = questionary.checkbox(
            "Select collections to include:",
            choices=sorted(collections),
            style=custom_style,
            instruction="(Space to select/deselect, Enter to confirm)",
        ).ask()

        if not selected:
            print_warning("No collections selected. Config will include all collections.")
        else:
            # Convert selected collections to exact regex patterns
            include_patterns = [f"^{coll}$" for coll in selected]
            print_success(f"Selected {len(selected)} collections")

    elif filter_mode == "Use include/exclude patterns":
        console.print()
        console.print(
            "[dim]Include patterns are regex patterns (e.g., '^users$', '.*_temp$')[/dim]"
        )

        # Include patterns
        add_include = questionary.confirm(
            "Add include patterns? (if empty, all collections are included)",
            default=False,
            style=custom_style,
        ).ask()

        if add_include:
            while True:
                pattern = questionary.text(
                    "Enter include pattern (or press Enter to finish):",
                    style=custom_style,
                ).ask()

                if not pattern:
                    break

                include_patterns.append(pattern)
                print_success(f"Added include pattern: {pattern}")

            if include_patterns:
                print_success(f"Added {len(include_patterns)} include pattern(s)")

        console.print()

        # Exclude patterns
        add_exclude = questionary.confirm(
            "Add exclude patterns?",
            default=False,
            style=custom_style,
        ).ask()

        if add_exclude:
            while True:
                pattern = questionary.text(
                    "Enter exclude pattern (or press Enter to finish):",
                    style=custom_style,
                ).ask()

                if not pattern:
                    break

                exclude_patterns.append(pattern)
                print_success(f"Added exclude pattern: {pattern}")

            if exclude_patterns:
                print_success(f"Added {len(exclude_patterns)} exclude pattern(s)")
    else:
        # Replicate all - no patterns needed
        print_success("Will replicate all collections")

    # Step 4: Cursor Detection Settings
    print_step(4, 10, "Cursor Detection Settings")
    console.print()

    console.print(
        "[dim]Cursor fields are used to track incremental changes during replication.[/dim]"
    )
    console.print(
        "[dim]Default candidates: updated_at, updatedAt, meta.updated_at, meta.updatedAt[/dim]"
    )
    console.print()

    customize_cursor_fields = questionary.confirm(
        "Customize cursor field candidates?",
        default=False,
        style=custom_style,
        instruction="(use default candidates if unsure)",
    ).ask()

    cursor_fields = ["updated_at", "updatedAt", "meta.updated_at", "meta.updatedAt"]  # Default
    if customize_cursor_fields:
        console.print()
        console.print("[dim]Enter cursor field candidates (one per line, in priority order):[/dim]")
        cursor_fields = []
        while True:
            field = questionary.text(
                "Cursor field:",
                style=custom_style,
                instruction="(e.g., 'updated_at' or 'meta.last_modified', empty to finish)",
            ).ask()
            if not field:
                break
            cursor_fields.append(field)

        if not cursor_fields:
            print_warning("No fields provided, using default candidates")
            cursor_fields = ["updated_at", "updatedAt", "meta.updated_at", "meta.updatedAt"]
        else:
            print_success(f"Configured {len(cursor_fields)} cursor field candidate(s)")
    else:
        print_success("Using default cursor field candidates")

    # Step 5: PII Analysis Settings
    print_step(5, 10, "PII Analysis Settings")
    console.print()

    enable_pii = questionary.confirm(
        "Enable PII Analysis?",
        default=True,
        style=custom_style,
    ).ask()

    sampling_config = None
    pii_analysis_config = None

    if enable_pii:
        console.print()

        # Ask for Presidio config first (moved from Step 7)
        console.print(
            "[dim]Presidio configuration defines PII detection and anonymization strategies.[/dim]"
        )
        console.print("[dim]You can use the default config or provide a custom one.[/dim]")
        console.print()

        use_custom_presidio = questionary.confirm(
            "Use custom Presidio configuration?",
            default=False,
            style=custom_style,
            instruction="(recommended for domain-specific PII patterns)",
        ).ask()

        presidio_config = None
        if use_custom_presidio:
            # Suggest default path
            default_presidio_path = f"config/{job}_presidio.yaml"

            presidio_path = questionary.text(
                "Enter Presidio configuration file path:",
                default=default_presidio_path,
                style=custom_style,
                instruction="(relative or absolute path)",
            ).ask()

            if presidio_path:
                presidio_config = presidio_path
                console.print()
                console.print(
                    f"[yellow]Note:[/yellow] Copy the default template to get started:\n"
                    f"  src/mongo_replication/config/presidio.yaml → {presidio_config}"
                )
                console.print()
                console.print("[dim]See docs/configuration.md for examples and guidance.[/dim]")
            else:
                print_warning("No path provided, using default Presidio configuration")
        else:
            print_success("Using default Presidio configuration")

        console.print()

        # Load entity strategies from the config (default or custom)
        entity_strategies = load_entity_strategies_from_config(presidio_config)

        # Confidence threshold
        confidence = questionary.text(
            "PII confidence threshold (0.0-1.0):",
            default="0.85",
            style=custom_style,
            instruction="(higher = fewer false positives, lower = more sensitive)",
        ).ask()

        try:
            confidence_threshold = float(confidence)
            if not 0.0 <= confidence_threshold <= 1.0:
                raise ValueError()
        except ValueError:
            print_error("Invalid confidence threshold, using default: 0.85")
            confidence_threshold = 0.85

        # Sample size
        sample = questionary.text(
            "Sample size per collection:",
            default="1000",
            style=custom_style,
            instruction="(number of documents to analyze for PII)",
        ).ask()

        try:
            sample_size = int(sample)
            if sample_size < 1:
                raise ValueError()
        except ValueError:
            print_error("Invalid sample size, using default: 1000")
            sample_size = 1000

        # Sample strategy
        sample_strategy = questionary.select(
            "Sampling strategy:",
            choices=["stratified", "random"],
            default="stratified",
            style=custom_style,
        ).ask()

        # Entity types
        console.print()
        default_entity_types = [
            "EMAIL_ADDRESS",
            "PHONE_NUMBER",
            "PERSON",
            "CREDIT_CARD",
            "IBAN_CODE",
            "US_SSN",
            "IP_ADDRESS",
            "URL",
        ]

        entity_types = questionary.checkbox(
            "Select PII entity types to detect:",
            choices=[
                questionary.Choice(
                    title=f"{et} → {entity_strategies.get(et, entity_strategies.get('DEFAULT', 'redact'))}",
                    value=et,
                    checked=True,
                )
                for et in default_entity_types
            ],
            style=custom_style,
            instruction="(Space to select/deselect, Enter to confirm)",
        ).ask()

        if not entity_types:
            print_warning("No entity types selected, using all defaults")
            entity_types = default_entity_types

        # Step 6: PII Anonymization Strategies
        print_step(6, 10, "PII Anonymization Strategies")
        console.print()

        console.print("[dim]Available operators (see docs/presidio.md for details):[/dim]")
        console.print("[dim]  Built-in: replace, redact, mask, hash, encrypt, keep[/dim]")
        console.print(
            "[dim]  Custom: fake_email, fake_name, fake_phone, smart_redact, stripe_testing_cc, etc.[/dim]"
        )
        console.print("[dim]  Aliases: fake, partial_redact, sha256, etc.[/dim]")
        console.print()

        use_custom_strategies = questionary.confirm(
            "Customize anonymization strategies per entity type?",
            default=False,
            style=custom_style,
        ).ask()

        # Use strategies loaded from the Presidio config
        default_strategies = entity_strategies.copy()

        if use_custom_strategies:
            # Common operators for user selection
            common_operators = [
                "replace",
                "redact",
                "mask",
                "hash",
                "fake",
                "fake_email",
                "fake_name",
                "fake_phone",
                "smart_redact",
                "keep",
            ]

            custom_strategies = {}
            for entity_type in entity_types:
                default_strategy = default_strategies.get(
                    entity_type, default_strategies.get("DEFAULT", "replace")
                )
                strategy = questionary.select(
                    f"Strategy for {entity_type}:",
                    choices=common_operators,
                    default=default_strategy if default_strategy in common_operators else "replace",
                    style=custom_style,
                ).ask()
                custom_strategies[entity_type] = strategy

            default_strategies = custom_strategies
            print_success("Custom strategies configured")
        else:
            # Use defaults
            print_success("Using default strategies")

        # Step 7: Allowlist (Optional)
        print_step(7, 10, "Field Allowlist (Optional)")
        console.print()

        console.print("[dim]Allowlist field patterns to exclude from PII Analysis.[/dim]")
        console.print("[dim]Default allowlist: _id, meta.*, *.id[/dim]")
        console.print()

        use_allowlist = questionary.confirm(
            "Customize field allowlist?",
            default=False,
            style=custom_style,
        ).ask()

        allowlist = ["_id", "meta.*", "*.id"]  # Default allowlist
        if use_allowlist:
            console.print()
            console.print("[dim]Enter field patterns (one per line, empty line to finish):[/dim]")
            allowlist = []
            while True:
                pattern = questionary.text(
                    "Field pattern:",
                    style=custom_style,
                    instruction="(e.g., 'metadata.*' or '*.created_at', empty to finish)",
                ).ask()
                if not pattern:
                    break
                allowlist.append(pattern)

            if not allowlist:
                print_warning("No patterns provided, using default allowlist")
                allowlist = ["_id", "meta.*", "*.id"]
            else:
                print_success(f"Allowlist configured with {len(allowlist)} patterns")
        else:
            print_success("Using default allowlist")

        # Create sampling config
        sampling_config = ScanSamplingConfig(
            sample_size=sample_size,
            sample_strategy=sample_strategy,
        )

        # Create PII analysis config
        pii_analysis_config = ScanPIIAnalysisConfig(
            enabled=True,
            confidence_threshold=confidence_threshold,
            entity_types=entity_types,
            default_strategies=default_strategies,
            allowlist=allowlist,
            presidio_config=presidio_config,
        )
    else:
        # PII disabled - use defaults
        system_defaults = load_defaults()
        scan_defaults = system_defaults.get("scan", {})
        sampling_defaults = scan_defaults.get("sampling", {})

        sampling_config = ScanSamplingConfig(
            sample_size=sampling_defaults.get("sample_size", 1000),
            sample_strategy=sampling_defaults.get("sample_strategy", "stratified"),
        )

        pii_analysis_config = ScanPIIAnalysisConfig(
            enabled=False,
        )

    # Step 9: Schema Relationship Inference
    print_step(9, 10, "Schema Relationship Inference (Optional)")
    console.print()

    console.print("[dim]Automatically detect relationships between collections.[/dim]")
    console.print("[dim]This analyzes field names to infer parent-child relationships.[/dim]")
    console.print("[dim]For example: 'customer_id' in 'orders' → relationship to 'customers'[/dim]")
    console.print()

    enable_schema_analysis = questionary.confirm(
        "Enable schema relationship inference?",
        default=False,
        style=custom_style,
        instruction="(useful for cascade replication)",
    ).ask()

    from mongo_replication.config.models import ScanSchemaRelationshipsConfig

    schema_relationships_config = ScanSchemaRelationshipsConfig(
        enabled=enable_schema_analysis,
    )

    if enable_schema_analysis:
        print_success("Schema relationship inference will be performed during scan")
    else:
        print_success("Schema relationship inference disabled")

    # Step 10: Save Configuration
    print_step(10, 10, "Save Configuration")
    console.print()

    # Determine output path
    if output is None:
        output = Path(f"config/{job}_config.yaml")

    # Create parent directory if needed
    output.parent.mkdir(parents=True, exist_ok=True)

    # Check if file exists
    if output.exists():
        overwrite = questionary.confirm(
            f"Config file {output} already exists. Overwrite?",
            default=False,
            style=custom_style,
        ).ask()

        if not overwrite:
            print_warning("Configuration not saved")
            raise typer.Exit(code=0)

    # Build config
    discovery_config = ScanDiscoveryConfig(
        include_patterns=include_patterns,
        exclude_patterns=exclude_patterns,
    )

    cursor_detection_config = ScanCursorDetectionConfig(
        cursor_fields=cursor_fields,
    )

    scan_config = ScanConfig(
        discovery=discovery_config,
        sampling=sampling_config,
        pii_analysis=pii_analysis_config,
        cursor_detection=cursor_detection_config,
        schema_relationships=schema_relationships_config,
    )

    # Load system defaults for replication
    system_defaults = load_defaults()
    replication_defaults_raw = system_defaults.get("replication", {}).get("defaults", {})

    # Build replication discovery config with same patterns as scan discovery
    replication_discovery_config = ReplicationDiscoveryConfig(
        replicate_all=(len(include_patterns) == 0),
        include_patterns=include_patterns,
        exclude_patterns=exclude_patterns,
    )

    # Build replication config with defaults and discovery
    replication_config = ReplicationConfig(
        discovery=replication_discovery_config,
        defaults=replication_defaults_raw,
        collections={},
    )

    rep_config = Config(scan=scan_config, replication=replication_config, schema_relationships=[])

    # Save config
    try:
        save_config(rep_config, output)
        print_success(f"Configuration saved to {output}")
    except Exception as e:
        print_error(f"Failed to save configuration: {e}")
        raise typer.Exit(code=1)

    # Print summary
    console.print()
    console.rule("[bold]Configuration Summary[/bold]", style="green")
    console.print()
    console.print(f"  • Job ID: [cyan]{job}[/cyan]")
    console.print(f"  • Source: [cyan]{source_db_name}[/cyan]")
    console.print(f"  • Destination: [cyan]{dest_db_name}[/cyan]")
    console.print(f"  • Config file: [cyan]{output}[/cyan]")

    if include_patterns:
        console.print(f"  • Include patterns: {len(include_patterns)}")
    if exclude_patterns:
        console.print(f"  • Exclude patterns: {len(exclude_patterns)}")

    console.print(f"  • Cursor field candidates: {len(cursor_fields)}")

    if pii_analysis_config:
        console.print("  • PII Analysis: [green]Enabled[/green]")
        console.print(f"    - Confidence: {pii_analysis_config.confidence_threshold}")
        console.print(f"    - Sample size: {sampling_config.sample_size}")
        console.print(f"    - Entity types: {len(pii_analysis_config.entity_types)}")
    else:
        console.print("  • PII Analysis: [yellow]Disabled[/yellow]")

    console.print()
    console.rule(style="green")
    console.print()

    # Print next steps
    console.print("[bold]Next steps:[/bold]")
    console.print()
    console.print(f"  1. Set environment variables for job '{job}':")
    console.print(f"     [dim]export MONGOREP_{job.upper()}_ENABLED=true[/dim]")
    console.print(f'     [dim]export MONGOREP_{job.upper()}_SOURCE_URI="{source_uri}"[/dim]')
    console.print(f'     [dim]export MONGOREP_{job.upper()}_DESTINATION_URI="{dest_uri}"[/dim]')
    console.print(f'     [dim]export MONGOREP_{job.upper()}_CONFIG_PATH="{output}"[/dim]')
    console.print()
    console.print("  2. Run scan to analyze collections:")
    console.print(f"     [dim]mongorep scan {job}[/dim]")
    console.print()
    console.print("  3. Review PII report and update config as needed:")
    console.print(f"     [dim]cat config/{job}_pii_report.md[/dim]")
    console.print()
    console.print("  4. Run replication:")
    console.print(f"     [dim]mongorep run {job}[/dim]")
    console.print()
