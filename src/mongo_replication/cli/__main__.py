#!/usr/bin/env python3
"""
Entry point for running the CLI as a module: python -m mongo_replication.cli
"""

from mongo_replication.cli.main import app

if __name__ == "__main__":
    app()
