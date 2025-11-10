"""Heuristic extraction using DOM structure and patterns."""

import re
from typing import Any, Dict, List, Optional

import structlog
from selectolax.parser import HTMLParser, Node

from scraper.models import RecordSchema, FieldSchema
from scraper.utils import extract_emails, extract_phones, clean_text

logger = structlog.get_logger()


class HeuristicExtractor:
    """Extract structured data using DOM heuristics."""
    
    def __init__(self, schema: RecordSchema):
        self.schema = schema
    
    def extract(self, tree: HTMLParser, page_url: str) -> Dict[str, Any]:
        """Extract record from HTML tree using heuristics.
        
        Args:
            tree: Parsed HTML tree
            page_url: URL of the page
            
        Returns:
            Dictionary with extracted fields
        """
        record: Dict[str, Any] = {"page_url": page_url}
        
        # Get full page text for fallback searches
        page_text = tree.text() or ""
        
        for field in self.schema.fields:
            value = self._extract_field(tree, page_text, field)
            record[field.name] = value
        
        return record
    
    def _extract_field(
        self,
        tree: HTMLParser,
        page_text: str,
        field: FieldSchema,
    ) -> Any:
        """Extract a single field value."""
        # Try label-based extraction first
        value = self._extract_by_label(tree, field)
        if value:
            return value
        
        # Try pattern-based extraction (email, phone)
        if "email" in field.type.value:
            return self._extract_email(page_text)
        elif "phone" in field.name.lower():
            return self._extract_phone(page_text)
        
        # Try semantic extraction based on field name
        if field.name == "name":
            return self._extract_name(tree)
        elif field.name in ["title", "position", "role"]:
            return self._extract_title(tree)
        elif field.name in ["bio", "biography", "description"]:
            return self._extract_bio(tree)
        elif field.name in ["org", "organization", "company", "department"]:
            return self._extract_organization(tree)
        elif field.name in ["location", "address"]:
            return self._extract_location(tree)
        
        return ""
    
    def _extract_by_label(self, tree: HTMLParser, field: FieldSchema) -> str:
        """Extract field by finding label and adjacent value."""
        # Build list of label patterns
        label_patterns = [field.name] + field.synonyms
        
        for pattern in label_patterns:
            # Try case-insensitive search for label
            for node in tree.css("*"):
                text = (node.text() or "").strip()
                
                # Check if this is a label
                if re.search(rf'\b{re.escape(pattern)}\b', text, re.I):
                    # Look for value in nearby elements
                    value = self._find_value_near_label(node, pattern)
                    if value:
                        return value
        
        return ""
    
    def _find_value_near_label(self, label_node: Node, label_text: str) -> str:
        """Find value near a label node."""
        # Strategy 1: Next sibling
        next_sib = label_node.next
        if next_sib:
            text = (next_sib.text() or "").strip()
            if text and text.lower() != label_text.lower():
                return clean_text(text)
        
        # Strategy 2: Parent's next sibling
        if label_node.parent:
            parent_next = label_node.parent.next
            if parent_next:
                text = (parent_next.text() or "").strip()
                if text and text.lower() != label_text.lower():
                    return clean_text(text)
        
        # Strategy 3: Look for colon separator in same node
        node_text = (label_node.text() or "").strip()
        if ":" in node_text:
            parts = node_text.split(":", 1)
            if len(parts) == 2:
                return clean_text(parts[1])
        
        return ""
    
    def _extract_email(self, text: str) -> str:
        """Extract email address from text."""
        emails = extract_emails(text)
        return emails[0] if emails else ""
    
    def _extract_phone(self, text: str) -> str:
        """Extract phone number from text."""
        phones = extract_phones(text)
        return phones[0] if phones else ""
    
    def _extract_name(self, tree: HTMLParser) -> str:
        """Extract person name from page."""
        # Try h1 first (common for profile pages)
        h1 = tree.css_first("h1")
        if h1:
            text = clean_text(h1.text() or "")
            if text and len(text) < 100:  # Reasonable name length
                return text
        
        # Try meta tags
        og_name = tree.css_first('meta[property="og:title"]')
        if og_name:
            content = og_name.attributes.get("content", "")
            if content:
                return clean_text(content)
        
        # Try specific classes
        for selector in [
            '[class*="name"]',
            '[itemprop="name"]',
            '[class*="title"]',
            "h2",
            "h3",
        ]:
            node = tree.css_first(selector)
            if node:
                text = clean_text(node.text() or "")
                if text and 5 < len(text) < 100:
                    return text
        
        return ""
    
    def _extract_title(self, tree: HTMLParser) -> str:
        """Extract job title/position."""
        for selector in [
            '[class*="title"]',
            '[class*="position"]',
            '[class*="role"]',
            '[itemprop="jobTitle"]',
            ".job-title",
            ".position",
        ]:
            node = tree.css_first(selector)
            if node:
                text = clean_text(node.text() or "")
                if text and len(text) < 200:
                    return text
        
        # Look for text near "Title:" or "Position:"
        for node in tree.css("*"):
            text = (node.text() or "").strip()
            if re.search(r'\b(title|position|role)\s*:', text, re.I):
                parts = re.split(r':\s*', text, maxsplit=1)
                if len(parts) == 2:
                    return clean_text(parts[1])
        
        return ""
    
    def _extract_bio(self, tree: HTMLParser) -> str:
        """Extract biography/description."""
        for selector in [
            '[class*="bio"]',
            '[class*="about"]',
            '[class*="description"]',
            '[itemprop="description"]',
            ".biography",
            ".about-text",
        ]:
            node = tree.css_first(selector)
            if node:
                text = clean_text(node.text() or "")
                if text and len(text) > 50:  # Meaningful bio
                    return text[:1000]  # Truncate to reasonable length
        
        # Try paragraphs in main content
        main = tree.css_first("main") or tree.css_first("#content") or tree.body
        if main:
            paragraphs = main.css("p")
            if paragraphs:
                texts = [clean_text(p.text() or "") for p in paragraphs[:3]]
                combined = " ".join(t for t in texts if len(t) > 30)
                if combined:
                    return combined[:1000]
        
        return ""
    
    def _extract_organization(self, tree: HTMLParser) -> str:
        """Extract organization/department."""
        for selector in [
            '[class*="department"]',
            '[class*="organization"]',
            '[class*="affiliation"]',
            '[itemprop="affiliation"]',
            ".department",
            ".org",
        ]:
            node = tree.css_first(selector)
            if node:
                text = clean_text(node.text() or "")
                if text and len(text) < 200:
                    return text
        
        return ""
    
    def _extract_location(self, tree: HTMLParser) -> str:
        """Extract location/address."""
        for selector in [
            '[class*="location"]',
            '[class*="address"]',
            '[itemprop="address"]',
            ".location",
            ".address",
        ]:
            node = tree.css_first(selector)
            if node:
                text = clean_text(node.text() or "")
                if text and len(text) < 300:
                    return text
        
        return ""


def extract_from_item(item: Node, schema: RecordSchema, base_url: str) -> Dict[str, Any]:
    """Extract record from a list item (person card).
    
    Args:
        item: HTML node representing a person card
        schema: Record schema
        base_url: Base URL for resolving relative links
        
    Returns:
        Extracted record dictionary
    """
    record: Dict[str, Any] = {}
    
    # Get item text
    item_text = item.text() or ""
    
    # Extract link to detail page (if exists)
    detail_link = None
    for link in item.css("a"):
        href = link.attributes.get("href", "")
        if href and re.search(r'/(people|person|profile|faculty)/', href, re.I):
            from scraper.utils import make_absolute_url
            detail_link = make_absolute_url(base_url, href)
            break
    
    if not detail_link:
        # Use first link as fallback
        first_link = item.css_first("a")
        if first_link:
            href = first_link.attributes.get("href", "")
            if href:
                from scraper.utils import make_absolute_url
                detail_link = make_absolute_url(base_url, href)
    
    record["page_url"] = detail_link or ""
    
    # Extract name (often in a link or heading)
    name = ""
    for selector in ["h1", "h2", "h3", "h4", "a", '[class*="name"]']:
        node = item.css_first(selector)
        if node:
            text = clean_text(node.text() or "")
            if text and 5 < len(text) < 100:
                name = text
                break
    record["name"] = name
    
    # Extract other fields
    record["email"] = extract_emails(item_text)[0] if extract_emails(item_text) else ""
    record["phone"] = extract_phones(item_text)[0] if extract_phones(item_text) else ""
    
    # Extract title (look for common patterns)
    title = ""
    for node in item.css("*"):
        text = clean_text(node.text() or "")
        if text and text != name and len(text) < 200:
            # Check if it looks like a title
            if any(word in text.lower() for word in [
                "professor", "researcher", "engineer", "scientist", "manager",
                "director", "phd", "dr.", "assistant", "associate"
            ]):
                title = text
                break
    record["title"] = title
    
    # Fill in other schema fields with empty values
    for field in schema.fields:
        if field.name not in record:
            record[field.name] = ""
    
    return record

