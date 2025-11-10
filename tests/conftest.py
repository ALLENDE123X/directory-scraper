"""Pytest configuration and shared fixtures."""

import pytest
from pathlib import Path


@pytest.fixture(scope="session")
def fixtures_dir():
    """Path to test fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def examples_dir():
    """Path to examples directory."""
    return Path(__file__).parent.parent / "examples"


@pytest.fixture
def sample_list_page_html(fixtures_dir):
    """Load sample list page HTML."""
    return (fixtures_dir / "sample_list_page.html").read_text()


@pytest.fixture
def sample_profile_page_html(fixtures_dir):
    """Load sample profile page HTML."""
    return (fixtures_dir / "sample_profile_page.html").read_text()

