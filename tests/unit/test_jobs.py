"""Unit tests for JobManager and JobConfig."""

import os
import pytest
from pathlib import Path
from unittest.mock import patch

from mongo_replication.engine.jobs import JobConfig, JobManager


class TestJobConfig:
    """Tests for JobConfig dataclass."""

    def test_job_config_creation(self):
        """Test creating a JobConfig."""
        job = JobConfig(
            job_id="test_job",
            source_uri="mongodb://source:27017/test",
            destination_uri="mongodb://dest:27017/test",
        )

        assert job.job_id == "test_job"
        assert job.source_uri == "mongodb://source:27017/test"
        assert job.destination_uri == "mongodb://dest:27017/test"
        assert job.config_path is None
        assert job.enabled is True

    def test_job_config_with_config_path(self):
        """Test JobConfig with config path."""
        job = JobConfig(
            job_id="test_job",
            source_uri="mongodb://source:27017/test",
            destination_uri="mongodb://dest:27017/test",
            config_path=Path("/path/to/config.yaml"),
        )

        assert job.config_path == Path("/path/to/config.yaml")

    def test_job_config_repr_masks_uris(self):
        """Test that repr masks URIs for security."""
        job = JobConfig(
            job_id="test_job",
            source_uri="mongodb://user:pass@source:27017/test",
            destination_uri="mongodb://user:pass@dest:27017/test",
        )

        repr_str = repr(job)
        assert "user:pass" not in repr_str
        assert "***" in repr_str
        assert "test_job" in repr_str


class TestJobManager:
    """Tests for JobManager."""

    def test_discover_jobs_success(self):
        """Test discovering jobs from environment."""
        env = {
            "REP_PROD_DB_ENABLED": "true",
            "REP_PROD_DB_SOURCE_URI": "mongodb://prod:27017/db",
            "REP_PROD_DB_DESTINATION_URI": "mongodb://analytics:27017/db",
            "REP_BACKUP_ENABLED": "true",
            "REP_BACKUP_SOURCE_URI": "mongodb://prod:27017/db",
            "REP_BACKUP_DESTINATION_URI": "mongodb://backup:27017/db",
        }

        with patch.dict(os.environ, env, clear=True):
            jobs = JobManager.discover_jobs()

        assert len(jobs) == 2

        job_ids = [j.job_id for j in jobs]
        assert "prod_db" in job_ids
        assert "backup" in job_ids

    def test_discover_jobs_with_config_path(self):
        """Test discovering jobs with config paths."""
        env = {
            "REP_TEST_ENABLED": "true",
            "REP_TEST_SOURCE_URI": "mongodb://source:27017/db",
            "REP_TEST_DESTINATION_URI": "mongodb://dest:27017/db",
            "REP_TEST_CONFIG_PATH": "/path/to/config.yaml",
        }

        with patch.dict(os.environ, env, clear=True):
            jobs = JobManager.discover_jobs()

        assert len(jobs) == 1
        assert jobs[0].job_id == "test"
        assert jobs[0].config_path == Path("/path/to/config.yaml")

    def test_discover_jobs_skips_disabled(self):
        """Test that disabled jobs are skipped."""
        env = {
            "REP_ENABLED_JOB_ENABLED": "true",
            "REP_ENABLED_JOB_SOURCE_URI": "mongodb://source:27017/db",
            "REP_ENABLED_JOB_DESTINATION_URI": "mongodb://dest:27017/db",
            "REP_DISABLED_JOB_ENABLED": "false",
            "REP_DISABLED_JOB_SOURCE_URI": "mongodb://source:27017/db",
            "REP_DISABLED_JOB_DESTINATION_URI": "mongodb://dest:27017/db",
        }

        with patch.dict(os.environ, env, clear=True):
            jobs = JobManager.discover_jobs()

        assert len(jobs) == 1
        assert jobs[0].job_id == "enabled_job"

    def test_discover_jobs_skips_missing_source_uri(self):
        """Test that jobs without source URI are skipped with warning."""
        env = {
            "REP_INCOMPLETE_ENABLED": "true",
            "REP_INCOMPLETE_DESTINATION_URI": "mongodb://dest:27017/db",
        }

        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="No enabled jobs found"):
                JobManager.discover_jobs()

    def test_discover_jobs_skips_missing_destination_uri(self):
        """Test that jobs without destination URI are skipped with warning."""
        env = {
            "REP_INCOMPLETE_ENABLED": "true",
            "REP_INCOMPLETE_SOURCE_URI": "mongodb://source:27017/db",
        }

        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="No enabled jobs found"):
                JobManager.discover_jobs()

    def test_discover_jobs_no_jobs_raises_error(self):
        """Test that no jobs raises ValueError."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="No replication jobs found"):
                JobManager.discover_jobs()

    def test_discover_jobs_normalizes_job_id_to_lowercase(self):
        """Test that job IDs are normalized to lowercase."""
        env = {
            "REP_PROD_DB_ENABLED": "true",
            "REP_PROD_DB_SOURCE_URI": "mongodb://source:27017/db",
            "REP_PROD_DB_DESTINATION_URI": "mongodb://dest:27017/db",
        }

        with patch.dict(os.environ, env, clear=True):
            jobs = JobManager.discover_jobs()

        assert jobs[0].job_id == "prod_db"  # lowercase

    def test_get_job_not_enabled_raises_error(self):
        """Test that getting a disabled job raises error."""
        env = {
            "REP_DISABLED_ENABLED": "false",
            "REP_DISABLED_SOURCE_URI": "mongodb://source:27017/db",
            "REP_DISABLED_DESTINATION_URI": "mongodb://dest:27017/db",
        }

        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="not enabled"):
                JobManager.get_job("disabled")

    def test_get_job_missing_source_uri_raises_error(self):
        """Test that missing source URI raises error."""
        env = {
            "REP_INCOMPLETE_ENABLED": "true",
            "REP_INCOMPLETE_DESTINATION_URI": "mongodb://dest:27017/db",
        }

        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="missing source URI"):
                JobManager.get_job("incomplete")

    def test_get_job_missing_destination_uri_raises_error(self):
        """Test that missing destination URI raises error."""
        env = {
            "REP_INCOMPLETE_ENABLED": "true",
            "REP_INCOMPLETE_SOURCE_URI": "mongodb://source:27017/db",
        }

        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="missing destination URI"):
                JobManager.get_job("incomplete")

    def test_list_jobs(self):
        """Test listing all job IDs."""
        env = {
            "REP_JOB1_ENABLED": "true",
            "REP_JOB1_SOURCE_URI": "mongodb://source:27017/db",
            "REP_JOB1_DESTINATION_URI": "mongodb://dest:27017/db",
            "REP_JOB2_ENABLED": "true",
            "REP_JOB2_SOURCE_URI": "mongodb://source:27017/db",
            "REP_JOB2_DESTINATION_URI": "mongodb://dest:27017/db",
        }

        with patch.dict(os.environ, env, clear=True):
            job_ids = JobManager.list_jobs()

        assert len(job_ids) == 2
        assert "job1" in job_ids
        assert "job2" in job_ids

    def test_list_jobs_empty_returns_empty_list(self):
        """Test that list_jobs returns empty list when no jobs."""
        with patch.dict(os.environ, {}, clear=True):
            job_ids = JobManager.list_jobs()

        assert job_ids == []

    def test_validate_uri_valid_mongodb(self):
        """Test validating valid mongodb:// URI."""
        JobManager.validate_uri("mongodb://localhost:27017/testdb")
        # Should not raise

    def test_validate_uri_valid_mongodb_srv(self):
        """Test validating valid mongodb+srv:// URI."""
        JobManager.validate_uri("mongodb+srv://cluster.mongodb.net/testdb")
        # Should not raise

    def test_validate_uri_empty_raises_error(self):
        """Test that empty URI raises error."""
        with pytest.raises(ValueError, match="cannot be empty"):
            JobManager.validate_uri("")

    def test_validate_uri_invalid_protocol_raises_error(self):
        """Test that invalid protocol raises error."""
        with pytest.raises(ValueError, match="must start with"):
            JobManager.validate_uri("http://localhost:27017/testdb")

    def test_validate_uri_malformed_raises_error(self):
        """Test that malformed URI raises error."""
        with pytest.raises(ValueError, match="appears to be malformed"):
            JobManager.validate_uri("mongodb://localhost")
