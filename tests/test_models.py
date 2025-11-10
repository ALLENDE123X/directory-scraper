"""Tests for data models."""

import pytest
from scraper.models import (
    FieldSchema,
    FieldType,
    RecordSchema,
    ExtractedRecord,
    PageTask,
    normalize_url,
    extract_domain,
)


def test_field_schema():
    """Test field schema creation."""
    field = FieldSchema(
        name="email",
        type=FieldType.EMAIL,
        synonyms=["e-mail", "contact"],
    )
    
    assert field.name == "email"
    assert field.type == FieldType.EMAIL
    assert "e-mail" in field.synonyms


def test_record_schema():
    """Test record schema operations."""
    schema = RecordSchema(fields=[
        FieldSchema(name="name", type=FieldType.STR),
        FieldSchema(name="email", type=FieldType.EMAIL_OPTIONAL),
    ])
    
    assert len(schema.fields) == 2
    assert schema.is_required("name")
    assert not schema.is_required("email")
    
    field = schema.get_field("name")
    assert field is not None
    assert field.name == "name"


def test_extracted_record_id():
    """Test record ID generation."""
    record1 = ExtractedRecord(
        data={"name": "John Smith", "email": "john@example.com"},
        source_url="https://example.com/john",
    )
    
    record2 = ExtractedRecord(
        data={"name": "John Smith", "email": "john@example.com"},
        source_url="https://example.com/john-smith",
    )
    
    # Same person should have same ID
    assert record1.record_id == record2.record_id


def test_page_task_id_generation():
    """Test page task ID generation."""
    task1 = PageTask(url="https://example.com/page1")
    task2 = PageTask(url="https://example.com/page1")
    task3 = PageTask(url="https://example.com/page2")
    
    # Same URL should have same task ID
    assert task1.task_id == task2.task_id
    assert task1.task_id != task3.task_id


def test_normalize_url():
    """Test URL normalization."""
    assert normalize_url("https://Example.com/Path") == "https://example.com/path"
    assert normalize_url("https://example.com/path/") == "https://example.com/path"
    assert normalize_url("https://example.com/path#fragment") == "https://example.com/path"


def test_extract_domain():
    """Test domain extraction."""
    assert extract_domain("https://example.com/path") == "example.com"
    assert extract_domain("https://sub.example.com/path") == "sub.example.com"
    assert extract_domain(None) is None

