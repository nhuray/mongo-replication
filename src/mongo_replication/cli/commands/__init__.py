"""
CLI commands for the replication tool.
"""

from mongo_replication.cli.commands.scan import scan_command
from mongo_replication.cli.commands.run import run_command

__all__ = ["scan_command", "run_command"]
