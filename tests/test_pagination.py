"""Tests for pagination detection and extraction."""

import pytest
from pathlib import Path
from selectolax.parser import HTMLParser

from scraper.models import PaginationStrategy
from scraper.pagination import (
    PaginationDetector,
    extract_list_items,
    extract_detail_links,
)


@pytest.fixture
def list_html():
    """Load sample list page HTML."""
    fixture_path = Path(__file__).parent / "fixtures" / "sample_list_page.html"
    return fixture_path.read_text()


def test_detect_next_link_strategy(list_html):
    """Test detection of next link pagination."""
    tree = HTMLParser(list_html)
    detector = PaginationDetector("https://university.edu/people")
    
    strategy = detector.detect_strategy(tree)
    # The fixture has both numbered and next link, should detect one
    assert strategy in [PaginationStrategy.NEXT_LINK, PaginationStrategy.NUMBERED]


def test_detect_numbered_strategy():
    """Test detection of numbered pagination."""
    html = """
    <nav class="pagination">
        <a href="?page=1">1</a>
        <a href="?page=2">2</a>
        <a href="?page=3">3</a>
    </nav>
    """
    tree = HTMLParser(html)
    detector = PaginationDetector("https://example.com")
    
    strategy = detector.detect_strategy(tree)
    assert strategy == PaginationStrategy.NUMBERED


def test_extract_next_link():
    """Test extraction of next page link."""
    html = """
    <nav>
        <a href="/people?page=1">1</a>
        <a href="/people?page=2" rel="next">Next</a>
    </nav>
    """
    tree = HTMLParser(html)
    detector = PaginationDetector("https://university.edu/people")
    
    tasks = detector._extract_next_link(tree, "https://university.edu/people")
    assert len(tasks) == 1
    assert "page=2" in tasks[0].url


def test_extract_list_items(list_html):
    """Test extraction of list items."""
    tree = HTMLParser(list_html)
    
    # With selector
    items = extract_list_items(tree, ".person-card")
    assert len(items) == 3
    
    # Without selector (heuristics)
    items = extract_list_items(tree, None)
    assert len(items) >= 3


def test_extract_detail_links(list_html):
    """Test extraction of profile links."""
    tree = HTMLParser(list_html)
    
    links = extract_detail_links(tree, "https://university.edu", None)
    assert len(links) >= 3
    assert any("/people/john-smith" in link for link in links)


def test_pagination_deduplication():
    """Test that pagination doesn't return duplicate URLs."""
    html = """
    <nav>
        <a href="/page/1">1</a>
        <a href="/page/2">2</a>
        <a href="/page/2">Next</a>
    </nav>
    """
    tree = HTMLParser(html)
    detector = PaginationDetector("https://example.com")
    
    tasks = detector._extract_numbered_pages(tree, "https://example.com/page/1", max_pages=10)
    urls = [t.url for t in tasks]
    
    # Should not have duplicates
    assert len(urls) == len(set(urls))


def test_generate_numbered_urls():
    """Test generation of numbered page URLs."""
    detector = PaginationDetector("https://example.com/people")
    
    tasks = detector.generate_numbered_urls(
        "https://example.com/people",
        max_pages=3,
        param_name="page",
        start_page=1,
    )
    
    assert len(tasks) == 3
    assert "page=1" in tasks[0].url
    assert "page=2" in tasks[1].url
    assert "page=3" in tasks[2].url

