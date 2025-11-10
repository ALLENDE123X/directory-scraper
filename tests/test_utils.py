"""Tests for utility functions."""

import pytest
from scraper.utils import (
    normalize_url,
    make_absolute_url,
    is_valid_url,
    extract_emails,
    extract_phones,
    clean_text,
    validate_email,
    is_person_url,
    extract_name_parts,
)


def test_normalize_url():
    """Test URL normalization."""
    assert normalize_url("HTTPS://Example.COM/Path") == "https://example.com/path"
    assert normalize_url("https://example.com/path/") == "https://example.com/path"
    assert normalize_url("https://example.com#fragment") == "https://example.com"


def test_make_absolute_url():
    """Test absolute URL creation."""
    base = "https://example.com/people"
    
    assert make_absolute_url(base, "/profile/1") == "https://example.com/profile/1"
    assert make_absolute_url(base, "profile/1") == "https://example.com/profile/1"
    assert make_absolute_url(base, "https://other.com/page") == "https://other.com/page"


def test_is_valid_url():
    """Test URL validation."""
    assert is_valid_url("https://example.com")
    assert is_valid_url("http://example.com/path")
    assert not is_valid_url("not-a-url")
    assert not is_valid_url("/relative/path")


def test_extract_emails():
    """Test email extraction."""
    text = "Contact john.smith@example.com or jane_doe@company.org for more info"
    emails = extract_emails(text)
    
    assert len(emails) == 2
    assert "john.smith@example.com" in emails
    assert "jane_doe@company.org" in emails


def test_extract_phones():
    """Test phone number extraction."""
    text = "Call 650-555-1234 or (415) 555-6789"
    phones = extract_phones(text)
    
    assert len(phones) >= 2
    assert any("650" in phone for phone in phones)


def test_clean_text():
    """Test text cleaning."""
    text = "  Multiple   spaces  and\n\nnewlines  "
    cleaned = clean_text(text)
    
    assert "  " not in cleaned
    assert cleaned == "Multiple spaces and newlines"


def test_validate_email():
    """Test email validation."""
    assert validate_email("john@example.com")
    assert validate_email("john.smith+tag@example.co.uk")
    assert not validate_email("invalid-email")
    assert not validate_email("@example.com")
    assert not validate_email("john@")


def test_is_person_url():
    """Test person URL detection."""
    assert is_person_url("https://example.com/people/john-smith")
    assert is_person_url("https://example.com/faculty/jane-doe")
    assert is_person_url("https://example.com/profile/123")
    assert not is_person_url("https://example.com/about")
    assert not is_person_url("https://example.com/contact")


def test_extract_name_parts():
    """Test name parsing."""
    parts = extract_name_parts("John Smith")
    assert parts["first"] == "John"
    assert parts["last"] == "Smith"
    assert parts["middle"] == ""
    
    parts = extract_name_parts("Mary Jane Watson")
    assert parts["first"] == "Mary"
    assert parts["middle"] == "Jane"
    assert parts["last"] == "Watson"
    
    parts = extract_name_parts("Madonna")
    assert parts["first"] == "Madonna"
    assert parts["last"] == ""

