"""Tests for evaluation functionality."""

import pytest
from scraper.evaluate import (
    EvaluationReport,
    evaluate_records,
    check_stanford_profile_count,
)


@pytest.fixture
def sample_records():
    """Sample records for testing."""
    return [
        {"name": "John Smith", "email": "john@example.com", "title": "Professor", "page_url": "http://example.com/1"},
        {"name": "Jane Doe", "email": "jane@example.com", "title": "", "page_url": "http://example.com/2"},
        {"name": "Bob Jones", "email": "invalid-email", "title": "Associate Professor", "page_url": "http://example.com/3"},
        {"name": "Alice Brown", "email": "", "title": "Lecturer", "page_url": ""},
        {"name": "John Smith", "email": "john@example.com", "title": "Professor", "page_url": "http://example.com/1"},  # Duplicate
    ]


def test_evaluation_report_duplicates(sample_records):
    """Test duplicate detection."""
    evaluator = EvaluationReport(sample_records, dupe_keys=["name", "email"])
    report = evaluator.evaluate()
    
    assert report["total_records"] == 5
    assert report["duplicates"] == 1
    assert report["unique_records"] == 4


def test_evaluation_report_completeness(sample_records):
    """Test field completeness calculation."""
    evaluator = EvaluationReport(sample_records)
    report = evaluator.evaluate()
    
    completeness = report["field_completeness"]
    
    # Name is 100% complete
    assert completeness["name"] == 100.0
    
    # Title is 80% complete (4 out of 5)
    assert completeness["title"] == 80.0
    
    # Email is 80% complete (4 out of 5)
    assert completeness["email"] == 80.0


def test_evaluation_report_validity(sample_records):
    """Test field validity calculation."""
    evaluator = EvaluationReport(sample_records)
    report = evaluator.evaluate()
    
    validity = report["field_validity"]
    
    # Email validity should be 75% (3 valid out of 4 non-empty)
    assert "email" in validity
    assert validity["email"] == 75.0


def test_evaluation_warnings(sample_records):
    """Test warning generation."""
    evaluator = EvaluationReport(sample_records)
    report = evaluator.evaluate()
    
    warnings = report["warnings"]
    
    # Should have warnings about duplicates and low completeness
    assert len(warnings) > 0
    assert any("duplicate" in w.lower() for w in warnings)


def test_evaluate_with_thresholds():
    """Test evaluation with expected thresholds."""
    records = [{"name": f"Person {i}", "email": f"person{i}@example.com"} for i in range(100)]
    
    evaluator = evaluate_records(records, expected_min=90, expected_max=110)
    report = evaluator.evaluate()
    
    # Should be within range, no warnings about count
    assert report["total_records"] == 100


def test_stanford_profile_count_check():
    """Test Stanford profile count validation."""
    # Within tolerance
    assert check_stanford_profile_count(6300)
    assert check_stanford_profile_count(6297)
    assert check_stanford_profile_count(6250)
    
    # Outside tolerance
    assert not check_stanford_profile_count(5000)
    assert not check_stanford_profile_count(7000)


def test_markdown_report_generation(sample_records):
    """Test markdown report generation."""
    evaluator = EvaluationReport(sample_records)
    evaluator.evaluate()
    
    markdown = evaluator.to_markdown()
    
    assert "# Scraper Evaluation Report" in markdown
    assert "Total Records" in markdown
    assert "Field Completeness" in markdown
    assert "|" in markdown  # Should have tables

