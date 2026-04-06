#!/usr/bin/env python3
"""
Main CLI entry point for the MongoDB replication tool.

Usage:
    rep init <job> [OPTIONS]    # Initialize scan configuration
    mongo-replication scan <job> [OPTIONS]    # Discover collections and PII
    mongo-replication run <job> [OPTIONS]     # Execute replication job
"""

import sys
from pathlib import Path
import typer
from rich.console import Console
from mongo_replication.cli.commands import init, scan, run

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    # dotenv not installed, skip automatic loading
    pass

# Ensure parent directories are in path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

app = typer.Typer(
    name="rep",
    help="MongoDB replication tool with PII detection and anonymization",
    add_completion=False,
)
console = Console()


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
