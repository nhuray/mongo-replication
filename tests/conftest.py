"""Shared pytest fixtures for all tests."""

import os
import sys
from pathlib import Path

import pytest

# Add src directory to Python path for imports
src_path = Path(__file__).parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))


@pytest.fixture(scope="session")
def project_root():
    """Return the project root directory."""
    return Path(__file__).parent


@pytest.fixture(scope="session")
def src_dir(project_root):
    """Return the src directory."""
    return project_root / "src"


@pytest.fixture(scope="session")
def tests_dir(project_root):
    """Return the tests directory."""
    return project_root / "tests"


@pytest.fixture
def mock_env(monkeypatch):
    """
    Fixture to easily set environment variables for tests.

    Usage:
        def test_something(mock_env):
            mock_env({
                "MONGOREP_SOURCE_TEST_ENABLED": "true",
                "MONGOREP_SOURCE_TEST_MONGODB_URI": "mongodb://localhost/test"
            })
    """

    def _set_env(env_vars: dict):
        for key, value in env_vars.items():
            monkeypatch.setenv(key, value)

    return _set_env


@pytest.fixture
def clear_rep_env(monkeypatch):
    """Clear all MONGOREP_* environment variables."""
    for key in list(os.environ.keys()):
        if key.startswith("MONGOREP_"):
            monkeypatch.delenv(key, raising=False)
