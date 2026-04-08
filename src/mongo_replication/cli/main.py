#!/usr/bin/env python3
"""
Main CLI entry point for the MongoDB replication tool.

Usage:
    mongorep init <job> [OPTIONS]    # Initialize scan configuration
    mongorep scan <job> [OPTIONS]    # Discover collections and PII
    mongorep run <job> [OPTIONS]     # Execute replication job
"""

import signal
import sys
from pathlib import Path
import typer
from rich.console import Console
from mongo_replication.cli.commands import init, scan, run

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv

    # Load .env file from current working directory
    dotenv_path = Path.cwd() / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path)
    else:
        # Try loading from default location (current directory)
        load_dotenv()
except ImportError:
    # python-dotenv not installed - this should not happen in production
    # but we handle gracefully for development
    console = Console()
    console.print(
        "[yellow]Warning: python-dotenv is not installed. "
        ".env file will not be loaded automatically.[/yellow]"
    )
    console.print("[dim]To use .env files, install: pip install python-dotenv[/dim]\n")

# Ensure parent directories are in path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

app = typer.Typer(
    name="rep",
    help="MongoDB replication tool with PII detection and anonymization",
    add_completion=False,
)
console = Console()


def signal_handler(signum, frame):
    """Handle interrupt signals (Ctrl+C, SIGTERM) gracefully."""
    console.print()
    console.print("[yellow]⚠ Operation interrupted by user[/yellow]")
    sys.exit(130)  # Standard exit code for SIGINT


# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


@app.callback()
def main():
    """
    MongoDB Replication Tool

    A job-based replication system with PII detection and anonymization.
    Each job has its own source, destination, and configuration.
    """
    pass


app.command(name="init")(init.init_command)
app.command(name="scan")(scan.scan_command)
app.command(name="run")(run.run_command)


if __name__ == "__main__":
    app()
