"""Pagination detection and strategies."""

import re
from typing import List, Optional, Set
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import structlog
from selectolax.parser import HTMLParser, Node

from scraper.models import PaginationStrategy, PageTask
from scraper.utils import make_absolute_url, normalize_url

logger = structlog.get_logger()


class PaginationDetector:
    """Detect and handle various pagination strategies."""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.seen_urls: Set[str] = set()
    
    def detect_strategy(self, tree: HTMLParser) -> PaginationStrategy:
        """Auto-detect pagination strategy from page structure."""
        # Check for rel="next" link
        next_link = tree.css_first('a[rel="next"]') or tree.css_first('link[rel="next"]')
        if next_link:
            return PaginationStrategy.NEXT_LINK
        
        # Check for numbered pagination
        if self._has_numbered_pagination(tree):
            return PaginationStrategy.NUMBERED
        
        # Check for "Load more" button (suggests infinite scroll)
        load_more = tree.css_first('[class*="load-more"]') or tree.css_first(
            'button:contains("Load More")'
        )
        if load_more:
            return PaginationStrategy.INFINITE_SCROLL
        
        # Check URL for cursor parameter
        parsed = urlparse(self.base_url)
        params = parse_qs(parsed.query)
        if "cursor" in params or "after" in params or "offset" in params:
            return PaginationStrategy.CURSOR
        
        # Default to single page
        return PaginationStrategy.NONE
    
    def _has_numbered_pagination(self, tree: HTMLParser) -> bool:
        """Check if page has numbered pagination links."""
        # Look for pagination containers
        pagination = (
            tree.css_first('[class*="pagination"]')
            or tree.css_first('[class*="pager"]')
            or tree.css_first('nav[role="navigation"]')
        )
        
        if not pagination:
            return False
        
        # Count links that look like page numbers
        links = pagination.css("a")
        number_links = 0
        for link in links:
            text = (link.text() or "").strip()
            if text.isdigit():
                number_links += 1
        
        return number_links >= 2
    
    def extract_next_pages(
        self,
        tree: HTMLParser,
        current_url: str,
        strategy: PaginationStrategy,
        max_pages: Optional[int] = None,
    ) -> List[PageTask]:
        """Extract next page URLs based on strategy."""
        if strategy == PaginationStrategy.NEXT_LINK:
            return self._extract_next_link(tree, current_url)
        elif strategy == PaginationStrategy.NUMBERED:
            return self._extract_numbered_pages(tree, current_url, max_pages)
        elif strategy == PaginationStrategy.CURSOR:
            return self._extract_cursor_pages(tree, current_url)
        else:
            return []
    
    def _extract_next_link(self, tree: HTMLParser, current_url: str) -> List[PageTask]:
        """Extract next page from rel=next or "Next" link."""
        tasks = []
        
        # Try rel="next"
        next_link = tree.css_first('a[rel="next"]') or tree.css_first('link[rel="next"]')
        if next_link:
            href = next_link.attributes.get("href", "")
            if href:
                url = make_absolute_url(self.base_url, href)
                normalized = normalize_url(url)
                if normalized not in self.seen_urls:
                    self.seen_urls.add(normalized)
                    tasks.append(PageTask(url=url, parent_url=current_url))
                    return tasks
        
        # Try "Next" button/link
        next_candidates = tree.css("a")
        for link in next_candidates:
            text = (link.text() or "").strip().lower()
            if text in ["next", "next page", "→", "»"]:
                href = link.attributes.get("href", "")
                if href:
                    url = make_absolute_url(self.base_url, href)
                    normalized = normalize_url(url)
                    if normalized not in self.seen_urls:
                        self.seen_urls.add(normalized)
                        tasks.append(PageTask(url=url, parent_url=current_url))
                        return tasks
        
        return tasks
    
    def _extract_numbered_pages(
        self,
        tree: HTMLParser,
        current_url: str,
        max_pages: Optional[int] = None,
    ) -> List[PageTask]:
        """Extract numbered pagination links."""
        tasks = []
        
        # Find pagination container
        pagination = (
            tree.css_first('[class*="pagination"]')
            or tree.css_first('[class*="pager"]')
            or tree.css_first('nav[role="navigation"]')
        )
        
        if not pagination:
            return tasks
        
        # Extract all numbered links
        links = pagination.css("a")
        page_urls = set()
        
        for link in links:
            text = (link.text() or "").strip()
            href = link.attributes.get("href", "")
            
            if href and (text.isdigit() or text.lower() in ["next", "→", "»"]):
                url = make_absolute_url(self.base_url, href)
                normalized = normalize_url(url)
                
                if normalized not in self.seen_urls:
                    page_urls.add(url)
        
        # Sort and limit
        sorted_urls = sorted(page_urls)
        if max_pages:
            sorted_urls = sorted_urls[: max_pages - 1]
        
        for url in sorted_urls:
            normalized = normalize_url(url)
            if normalized not in self.seen_urls:
                self.seen_urls.add(normalized)
                tasks.append(PageTask(url=url, parent_url=current_url))
        
        return tasks
    
    def _extract_cursor_pages(self, tree: HTMLParser, current_url: str) -> List[PageTask]:
        """Extract next page from cursor-based pagination."""
        # Look for next page link with cursor parameter
        links = tree.css("a")
        
        for link in links:
            href = link.attributes.get("href", "")
            text = (link.text() or "").strip().lower()
            
            if href and text in ["next", "more", "load more"]:
                parsed = urlparse(href)
                params = parse_qs(parsed.query)
                
                if "cursor" in params or "after" in params or "offset" in params:
                    url = make_absolute_url(self.base_url, href)
                    normalized = normalize_url(url)
                    
                    if normalized not in self.seen_urls:
                        self.seen_urls.add(normalized)
                        return [PageTask(url=url, parent_url=current_url)]
        
        return []
    
    def generate_numbered_urls(
        self,
        base_url: str,
        max_pages: int,
        param_name: str = "page",
        start_page: int = 1,
    ) -> List[PageTask]:
        """Generate numbered page URLs when pattern is known."""
        tasks = []
        parsed = urlparse(base_url)
        
        for page_num in range(start_page, start_page + max_pages):
            # Update or add page parameter
            params = parse_qs(parsed.query)
            params[param_name] = [str(page_num)]
            
            new_query = urlencode(params, doseq=True)
            new_url = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                new_query,
                "",
            ))
            
            normalized = normalize_url(new_url)
            if normalized not in self.seen_urls:
                self.seen_urls.add(normalized)
                tasks.append(PageTask(url=new_url, page_num=page_num))
        
        return tasks


def extract_list_items(tree: HTMLParser, item_selector: Optional[str] = None) -> List[Node]:
    """Extract list items (person cards) from page.
    
    Uses provided selector or heuristics to find person-like containers.
    """
    if item_selector:
        items = tree.css(item_selector)
        logger.debug("extracted_items_with_selector", count=len(items), selector=item_selector)
        return items
    
    # Heuristic detection of person cards
    candidates = []
    
    # Try common container classes
    for selector in [
        '[class*="person"]',
        '[class*="profile"]',
        '[class*="member"]',
        '[class*="card"]',
        '[class*="item"]',
        'article',
        '[itemtype*="Person"]',
    ]:
        items = tree.css(selector)
        if items:
            candidates.extend(items)
    
    # Score candidates by person-like features
    scored_items = []
    for item in candidates:
        score = 0
        text = (item.text() or "").lower()
        html = item.html or ""
        
        # Check for person-like features
        if any(word in text for word in ["phd", "dr.", "professor", "research"]):
            score += 2
        if "@" in text:  # Email
            score += 3
        if re.search(r'\d{3}[-.]?\d{3}[-.]?\d{4}', text):  # Phone
            score += 2
        if item.css("img"):  # Photo
            score += 1
        if item.css("a[href*='/profile']") or item.css("a[href*='/people']"):
            score += 3
        
        if score > 0:
            scored_items.append((score, item))
    
    # Return items above threshold
    scored_items.sort(reverse=True, key=lambda x: x[0])
    items = [item for score, item in scored_items if score >= 2]
    
    logger.debug("extracted_items_with_heuristics", count=len(items))
    return items


def extract_detail_links(
    tree: HTMLParser,
    base_url: str,
    link_selector: Optional[str] = None,
) -> List[str]:
    """Extract links to detail pages from list items."""
    links = []
    
    if link_selector:
        link_nodes = tree.css(link_selector)
        for node in link_nodes:
            href = node.attributes.get("href", "")
            if href:
                links.append(make_absolute_url(base_url, href))
    else:
        # Heuristic: find links that look like profile pages
        all_links = tree.css("a")
        for link in all_links:
            href = link.attributes.get("href", "")
            if href and re.search(
                r'/(people|person|profile|faculty|staff|member)/', href, re.I
            ):
                links.append(make_absolute_url(base_url, href))
    
    # Deduplicate
    return list(set(links))

