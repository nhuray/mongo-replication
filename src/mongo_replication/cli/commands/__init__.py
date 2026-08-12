"""
CLI commands for the replication tool.
"""

from mongo_replication.cli.commands.run import run_command
from mongo_replication.cli.commands.scan import scan_command

__all__ = ["run_command", "scan_command"]
