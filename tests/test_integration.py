"""Integration tests for the full pipeline."""

import asyncio
import tempfile
from pathlib import Path

import pytest

from scraper.config import ScraperConfig, load_schema
from scraper.pipeline import ScraperPipeline
from scraper.storage import RecordReader


@pytest.fixture
def person_schema():
    """Load person schema."""
    schema_path = Path(__file__).parent.parent / "examples" / "schema_person.json"
    return load_schema(str(schema_path))


@pytest.fixture
def config():
    """Create test configuration."""
    return ScraperConfig(
        rate_limit=10.0,
        max_retries=1,
        timeout=10,
        respect_robots=False,  # For testing
    )


@pytest.mark.asyncio
@pytest.mark.skip(reason="Integration test requires live site or mock server")
async def test_full_pipeline(config, person_schema):
    """Test full scraping pipeline with fixtures."""
    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
        output_path = f.name
    
    try:
        # Create pipeline
        pipeline = ScraperPipeline(config, person_schema)
        
        # Run (would need a mock server or use fixtures)
        # This is a placeholder for a real integration test
        metadata = await pipeline.run(
            start_url="https://engineering.stanford.edu/people",
            output_path=output_path,
            max_pages=2,  # Very limited for testing
            use_llm=False,
        )
        
        # Verify results
        assert metadata.pages_fetched > 0
        assert metadata.records_extracted >= 0
        
        # Read output
        if Path(output_path).exists():
            records = await RecordReader.read_jsonl(output_path)
            assert isinstance(records, list)
    
    finally:
        Path(output_path).unlink(missing_ok=True)


def test_schema_loading(person_schema):
    """Test that schema loads correctly."""
    assert person_schema is not None
    assert len(person_schema.fields) > 0
    
    # Check for expected fields
    field_names = [f.name for f in person_schema.fields]
    assert "name" in field_names
    assert "email" in field_names
    assert "page_url" in field_names

