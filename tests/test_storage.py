"""Tests for storage functionality."""

import asyncio
import tempfile
from pathlib import Path

import pytest

from scraper.models import RecordSchema, FieldSchema, FieldType, RunMetadata
from scraper.storage import (
    RunHistory,
    RecordWriter,
    RecordReader,
    deduplicate_records,
)


@pytest.fixture
def sample_records():
    """Sample records for testing."""
    return [
        {"name": "John Smith", "email": "john@example.com", "title": "Professor"},
        {"name": "Jane Doe", "email": "jane@example.com", "title": "Associate Professor"},
        {"name": "John Smith", "email": "john@example.com", "title": "Professor"},  # Duplicate
    ]


@pytest.fixture
def temp_db():
    """Temporary database file."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        yield f.name
    Path(f.name).unlink(missing_ok=True)


def test_deduplicate_records(sample_records):
    """Test record deduplication."""
    unique = deduplicate_records(sample_records, ["name", "email"])
    
    assert len(unique) == 2  # Should remove one duplicate
    names = [r["name"] for r in unique]
    assert "John Smith" in names
    assert "Jane Doe" in names


@pytest.mark.asyncio
async def test_run_history(temp_db):
    """Test run history database operations."""
    schema = RecordSchema(fields=[
        FieldSchema(name="name", type=FieldType.STR),
    ])
    
    from datetime import datetime
    
    metadata = RunMetadata(
        run_id="test_run_123",
        started_at=datetime.utcnow(),
        start_url="https://example.com",
        schema=schema,
    )
    
    async with RunHistory(temp_db) as history:
        # Create run
        await history.create_run(metadata)
        
        # Log event
        await history.log_event("test_run_123", "page_fetched", "task_123")
        
        # Check if task is completed
        completed = await history.is_task_completed("test_run_123", "task_123")
        assert completed
        
        # Check non-existent task
        not_completed = await history.is_task_completed("test_run_123", "task_456")
        assert not not_completed


@pytest.mark.asyncio
async def test_jsonl_write_read(sample_records):
    """Test JSONL write and read."""
    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
        temp_file = f.name
    
    try:
        # Write
        await RecordWriter.write_jsonl(sample_records, temp_file)
        
        # Read
        records = await RecordReader.read_jsonl(temp_file)
        
        assert len(records) == len(sample_records)
        assert records[0]["name"] == "John Smith"
    finally:
        Path(temp_file).unlink(missing_ok=True)


def test_csv_write_read(sample_records):
    """Test CSV write and read."""
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        temp_file = f.name
    
    try:
        # Write
        RecordWriter.write_csv(sample_records, temp_file)
        
        # Read
        records = RecordReader.read_csv(temp_file)
        
        assert len(records) == len(sample_records)
        assert records[0]["name"] == "John Smith"
    finally:
        Path(temp_file).unlink(missing_ok=True)

