"""
Init command - interactive setup for scan configuration.

Usage:
    rep init <job> [OPTIONS]
"""

from pathlib import Path
from typing import Optional, List

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
from mongo_replication.config.loader import save_config, load_defaults, ReplicationConfig
from mongo_replication.config.models import ScanConfig, ScanDiscoveryConfig, ScanPIIConfig, Config

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
    - PII detection settings
    - Anonymization strategies

    Examples:
        # Initialize configuration for prod_db job
        rep init prod_db

        # Specify custom output path
        rep init prod_db --output /path/to/config.yaml
    """
    print_banner("INITIALIZE SCAN CONFIGURATION", Job=job)

    console.print()
    console.print(
        "[bold]This wizard will help you set up scan configuration for PII detection.[/bold]"
    )
    console.print()

    # Step 1: Source URI
    print_step(1, 6, "Source Database Connection")
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
    print_step(2, 6, "Destination Database Connection")
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

    # Step 3: Collection Discovery
    print_step(3, 6, "Collection Discovery")
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

    # Step 4: PII Detection Settings
    print_step(4, 6, "PII Detection Settings")
    console.print()

    enable_pii = questionary.confirm(
        "Enable PII detection?",
        default=True,
        style=custom_style,
    ).ask()

    pii_config = None

    if enable_pii:
        console.print()

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
            choices=[questionary.Choice(title=et, checked=True) for et in default_entity_types],
            style=custom_style,
            instruction="(Space to select/deselect, Enter to confirm)",
        ).ask()

        if not entity_types:
            print_warning("No entity types selected, using all defaults")
            entity_types = default_entity_types

        # Step 5: PII Anonymization Strategies
        print_step(5, 6, "PII Anonymization Strategies")
        console.print()

        console.print("[dim]Available strategies:[/dim]")
        console.print("[dim]  • redact: Replace with ***[/dim]")
        console.print("[dim]  • hash: SHA-256 hash[/dim]")
        console.print("[dim]  • fake: Generate fake data (emails, names, etc.)[/dim]")
        console.print()

        use_custom_strategies = questionary.confirm(
            "Customize anonymization strategies per entity type?",
            default=False,
            style=custom_style,
        ).ask()

        default_strategies = {
            "EMAIL_ADDRESS": "fake",
            "PHONE_NUMBER": "fake",
            "PERSON": "fake",
            "CREDIT_CARD": "hash",
            "IBAN_CODE": "hash",
            "US_SSN": "redact",
            "IP_ADDRESS": "hash",
            "URL": "hash",
            "LOCATION": "redact",
            "CRYPTO": "hash",
        }

        if use_custom_strategies:
            custom_strategies = {}
            for entity_type in entity_types:
                default_strategy = default_strategies.get(entity_type, "redact")
                strategy = questionary.select(
                    f"Strategy for {entity_type}:",
                    choices=["redact", "hash", "fake"],
                    default=default_strategy,
                    style=custom_style,
                ).ask()
                custom_strategies[entity_type] = strategy

            default_strategies = custom_strategies
            print_success("Custom strategies configured")
        else:
            # Use defaults
            print_success("Using default strategies")

        # Create PII config
        pii_config = ScanPIIConfig(
            enabled=True,
            confidence_threshold=confidence_threshold,
            entity_types=entity_types,
            sample_size=sample_size,
            sample_strategy=sample_strategy,
            default_strategies=default_strategies,
            allowlist=[],
        )

    # Step 6: Save Configuration
    print_step(6, 6, "Save Configuration")
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

    scan_config = ScanConfig(
        discovery=discovery_config,
        pii=pii_config,
    )

    # Load system defaults for replication
    system_defaults = load_defaults()
    replication_defaults_raw = system_defaults.get("replication", {}).get("defaults", {})

    # Build replication config with defaults
    replication_config = ReplicationConfig(
        defaults=replication_defaults_raw, collections={}, schema=[]
    )

    rep_config = Config(scan=scan_config, replication=replication_config)

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

    if pii_config:
        console.print("  • PII detection: [green]Enabled[/green]")
        console.print(f"    - Confidence: {pii_config.confidence_threshold}")
        console.print(f"    - Sample size: {pii_config.sample_size}")
        console.print(f"    - Entity types: {len(pii_config.entity_types)}")
    else:
        console.print("  • PII detection: [yellow]Disabled[/yellow]")

    console.print()
    console.rule(style="green")
    console.print()

    # Print next steps
    console.print("[bold]Next steps:[/bold]")
    console.print()
    console.print(f"  1. Set environment variables for job '{job}':")
    console.print(f"     [dim]export REP_{job.upper()}_ENABLED=true[/dim]")
    console.print(f'     [dim]export REP_{job.upper()}_SOURCE_URI="{source_uri}"[/dim]')
    console.print(f'     [dim]export REP_{job.upper()}_DESTINATION_URI="{dest_uri}"[/dim]')
    console.print(f'     [dim]export REP_{job.upper()}_CONFIG_PATH="{output}"[/dim]')
    console.print()
    console.print("  2. Run scan to analyze collections:")
    console.print(f"     [dim]mongo-replication scan {job}[/dim]")
    console.print()
    console.print("  3. Review PII report and update config as needed:")
    console.print(f"     [dim]cat config/{job}_pii_report.md[/dim]")
    console.print()
    console.print("  4. Run replication:")
    console.print(f"     [dim]mongo-replication run {job}[/dim]")
    console.print()
