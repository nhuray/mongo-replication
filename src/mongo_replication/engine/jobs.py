"""Job management for MongoDB replication.

This module handles job discovery and configuration based on environment variables.
Each job represents a complete replication pipeline with its own source, destination,
and configuration.

Environment Variable Pattern:
    REP_<JOB>_ENABLED=true|false
    REP_<JOB>_SOURCE_URI=mongodb://...
    REP_<JOB>_DESTINATION_URI=mongodb://...
    REP_<JOB>_CONFIG_PATH=/path/to/config.yaml

Examples:
    # Production to Analytics job
    REP_PROD_DB_ENABLED=true
    REP_PROD_DB_SOURCE_URI=mongodb://prod-host:27017/prod_db
    REP_PROD_DB_DESTINATION_URI=mongodb://analytics-host:27017/analytics_db
    REP_PROD_DB_CONFIG_PATH=config/prod_db.yaml

    # Backup job
    REP_BACKUP_ENABLED=true
    REP_BACKUP_SOURCE_URI=mongodb://prod-host:27017/prod_db
    REP_BACKUP_DESTINATION_URI=mongodb://backup-host:27017/backup_db
"""

import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class JobConfig:
    """Configuration for a replication job.

    Attributes:
        job_id: Unique identifier for the job (e.g., "prod_db", "backup")
        source_uri: MongoDB connection URI for source database
        destination_uri: MongoDB connection URI for destination database
        config_path: Optional path to YAML configuration file
        enabled: Whether this job is enabled for execution
    """

    job_id: str
    source_uri: str
    destination_uri: str
    config_path: Optional[Path] = None
    enabled: bool = True

    def __repr__(self) -> str:
        """String representation with URIs masked for security."""
        return (
            f"JobConfig(job_id='{self.job_id}', "
            f"source_uri='***', destination_uri='***', "
            f"config_path={self.config_path}, enabled={self.enabled})"
        )


class JobManager:
    """Manager for discovering and accessing replication jobs from environment variables."""

    # Pattern to match job environment variables: REP_<JOB>_<SUFFIX>
    JOB_PATTERN = re.compile(r"^REP_([A-Z0-9_]+)_(ENABLED|SOURCE_URI|DESTINATION_URI|CONFIG_PATH)$")

    @staticmethod
    def discover_jobs() -> List[JobConfig]:
        """Discover all enabled replication jobs from environment variables.

        Scans environment for variables matching the pattern:
            REP_<JOB>_ENABLED=true
            REP_<JOB>_SOURCE_URI=mongodb://...
            REP_<JOB>_DESTINATION_URI=mongodb://...
            REP_<JOB>_CONFIG_PATH=/path/to/config.yaml (optional)

        Returns:
            List of JobConfig objects for enabled jobs

        Raises:
            ValueError: If no enabled jobs are found or required variables are missing
        """
        # Group environment variables by job ID
        job_vars: Dict[str, Dict[str, str]] = {}

        for key, value in os.environ.items():
            match = JobManager.JOB_PATTERN.match(key)
            if match:
                job_id = match.group(1)
                suffix = match.group(2)

                if job_id not in job_vars:
                    job_vars[job_id] = {}

                job_vars[job_id][suffix] = value

        if not job_vars:
            raise ValueError(
                "No replication jobs found in environment. "
                "Please set environment variables with pattern: "
                "REP_<JOB>_ENABLED=true, REP_<JOB>_SOURCE_URI=..., REP_<JOB>_DESTINATION_URI=..."
            )

        # Create JobConfig objects for enabled jobs
        jobs = []
        for job_id, vars in job_vars.items():
            # Skip if not enabled
            enabled = vars.get("ENABLED", "false").lower() in ("true", "1", "yes")
            if not enabled:
                logger.debug(f"Skipping disabled job: {job_id}")
                continue

            # Validate required variables
            source_uri = vars.get("SOURCE_URI")
            if not source_uri:
                logger.warning(
                    f"Job {job_id} is enabled but missing REP_{job_id}_SOURCE_URI. Skipping."
                )
                continue

            destination_uri = vars.get("DESTINATION_URI")
            if not destination_uri:
                logger.warning(
                    f"Job {job_id} is enabled but missing REP_{job_id}_DESTINATION_URI. Skipping."
                )
                continue

            # Optional config path
            config_path = vars.get("CONFIG_PATH")
            config_path_obj = Path(config_path) if config_path else None

            # Create job config
            job = JobConfig(
                job_id=job_id.lower(),  # Normalize to lowercase
                source_uri=source_uri,
                destination_uri=destination_uri,
                config_path=config_path_obj,
                enabled=True,
            )

            jobs.append(job)
            logger.info(f"Discovered job: {job.job_id}")

        if not jobs:
            raise ValueError("No enabled jobs found. Set REP_<JOB>_ENABLED=true to enable a job.")

        return jobs

    @staticmethod
    def get_job(job_id: str) -> JobConfig:
        """Get a specific job by ID.

        Args:
            job_id: Job identifier (case-insensitive)

        Returns:
            JobConfig object

        Raises:
            ValueError: If job not found or not properly configured
        """
        # Normalize job ID to uppercase for environment variable lookup
        job_id_upper = job_id.upper()

        # Check if job is enabled
        enabled_key = f"REP_{job_id_upper}_ENABLED"
        enabled = os.environ.get(enabled_key, "false").lower() in ("true", "1", "yes")

        if not enabled:
            raise ValueError(
                f"Job '{job_id}' is not enabled. Set {enabled_key}=true to enable this job."
            )

        # Get required URIs
        source_uri_key = f"REP_{job_id_upper}_SOURCE_URI"
        source_uri = os.environ.get(source_uri_key)
        if not source_uri:
            raise ValueError(
                f"Job '{job_id}' is missing source URI. Set {source_uri_key}=mongodb://..."
            )

        destination_uri_key = f"REP_{job_id_upper}_DESTINATION_URI"
        destination_uri = os.environ.get(destination_uri_key)
        if not destination_uri:
            raise ValueError(
                f"Job '{job_id}' is missing destination URI. "
                f"Set {destination_uri_key}=mongodb://..."
            )

        # Get optional config path
        config_path_key = f"REP_{job_id_upper}_CONFIG_PATH"
        config_path = os.environ.get(config_path_key)
        if not config_path:
            raise ValueError(
                f"Job '{job_id}' is missing configuration file."
                f"Set {config_path_key}=config/{job_id.lower()}_config.yaml"
            )
        if not Path(config_path).exists():
            raise ValueError(
                f"Job '{job_id}' is missing configuration file."
                f"Config file {config_path} does not exist."
            )
        config_path_obj = Path(config_path) if config_path else None

        return JobConfig(
            job_id=job_id.lower(),
            source_uri=source_uri,
            destination_uri=destination_uri,
            config_path=config_path_obj,
            enabled=True,
        )

    @staticmethod
    def list_jobs() -> List[str]:
        """List all enabled job IDs.

        Returns:
            List of job IDs (lowercase)
        """
        try:
            jobs = JobManager.discover_jobs()
            return [job.job_id for job in jobs]
        except ValueError:
            return []

    @staticmethod
    def validate_uri(uri: str, uri_type: str = "MongoDB") -> None:
        """Validate a MongoDB connection URI.

        Args:
            uri: Connection URI to validate
            uri_type: Type description for error messages

        Raises:
            ValueError: If URI is invalid
        """
        if not uri:
            raise ValueError(f"{uri_type} URI cannot be empty")

        if not uri.startswith("mongodb://") and not uri.startswith("mongodb+srv://"):
            raise ValueError(
                f"{uri_type} URI must start with 'mongodb://' or 'mongodb+srv://'. "
                f"Got: {uri[:20]}..."
            )

        # Basic validation - should have host part
        if uri.count("/") < 3:
            raise ValueError(
                f"{uri_type} URI appears to be malformed. "
                f"Expected format: mongodb://host:port/database"
            )
