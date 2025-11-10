"""Tests for heuristic extraction."""

import pytest
from pathlib import Path
from selectolax.parser import HTMLParser

from scraper.models import RecordSchema, FieldSchema, FieldType
from scraper.extractor.heuristics import HeuristicExtractor, extract_from_item


@pytest.fixture
def person_schema():
    """Create a person schema for testing."""
    return RecordSchema(fields=[
        FieldSchema(name="name", type=FieldType.STR),
        FieldSchema(name="title", type=FieldType.STR_OPTIONAL),
        FieldSchema(name="email", type=FieldType.EMAIL_OPTIONAL),
        FieldSchema(name="phone", type=FieldType.STR_OPTIONAL),
        FieldSchema(name="page_url", type=FieldType.URL),
        FieldSchema(name="bio", type=FieldType.STR_OPTIONAL),
        FieldSchema(name="org", type=FieldType.STR_OPTIONAL),
        FieldSchema(name="location", type=FieldType.STR_OPTIONAL),
    ])


@pytest.fixture
def profile_html():
    """Load sample profile HTML."""
    fixture_path = Path(__file__).parent / "fixtures" / "sample_profile_page.html"
    return fixture_path.read_text()


def test_extract_name(person_schema, profile_html):
    """Test name extraction."""
    tree = HTMLParser(profile_html)
    extractor = HeuristicExtractor(person_schema)
    
    name = extractor._extract_name(tree)
    assert name == "Dr. John Smith"


def test_extract_email(person_schema, profile_html):
    """Test email extraction."""
    tree = HTMLParser(profile_html)
    extractor = HeuristicExtractor(person_schema)
    
    text = tree.text() or ""
    email = extractor._extract_email(text)
    assert email == "john.smith@university.edu"


def test_extract_title(person_schema, profile_html):
    """Test title extraction."""
    tree = HTMLParser(profile_html)
    extractor = HeuristicExtractor(person_schema)
    
    title = extractor._extract_title(tree)
    assert "Professor" in title
    assert "Computer Science" in title


def test_extract_bio(person_schema, profile_html):
    """Test biography extraction."""
    tree = HTMLParser(profile_html)
    extractor = HeuristicExtractor(person_schema)
    
    bio = extractor._extract_bio(tree)
    assert len(bio) > 50
    assert "artificial intelligence" in bio.lower()


def test_full_extraction(person_schema, profile_html):
    """Test full record extraction."""
    tree = HTMLParser(profile_html)
    extractor = HeuristicExtractor(person_schema)
    
    record = extractor.extract(tree, "https://university.edu/people/john-smith")
    
    assert record["name"] == "Dr. John Smith"
    assert record["email"] == "john.smith@university.edu"
    assert "Professor" in record["title"]
    assert record["page_url"] == "https://university.edu/people/john-smith"
    assert len(record["bio"]) > 50


def test_extract_from_list_item(person_schema):
    """Test extraction from list page item."""
    html = """
    <div class="person-card">
        <h3><a href="/people/john-smith">Dr. John Smith</a></h3>
        <p class="title">Professor of Computer Science</p>
        <p class="email">john.smith@university.edu</p>
        <p class="phone">650-555-1234</p>
    </div>
    """
    
    tree = HTMLParser(html)
    item = tree.css_first(".person-card")
    
    record = extract_from_item(item, person_schema, "https://university.edu")
    
    assert record["name"] == "Dr. John Smith"
    assert record["email"] == "john.smith@university.edu"
    assert record["phone"] == "650-555-1234"
    assert "/people/john-smith" in record["page_url"]

