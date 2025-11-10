"""Specialized field resolvers for common fields."""

from typing import Any, Dict, Optional

from selectolax.parser import HTMLParser

from scraper.utils import extract_emails, extract_phones, clean_text


class FieldResolvers:
    """Collection of specialized field resolvers."""
    
    @staticmethod
    def resolve_name(tree: HTMLParser, text: str) -> str:
        """Resolve name field with high accuracy."""
        # Try h1 (most common for person pages)
        h1 = tree.css_first("h1")
        if h1:
            name = clean_text(h1.text() or "")
            if name and 3 < len(name) < 100:
                return name
        
        # Try meta tags
        for meta_prop in ["og:title", "twitter:title"]:
            meta = tree.css_first(f'meta[property="{meta_prop}"]')
            if not meta:
                meta = tree.css_first(f'meta[name="{meta_prop}"]')
            if meta:
                content = meta.attributes.get("content", "")
                if content and 3 < len(content) < 100:
                    return clean_text(content)
        
        # Try schema.org
        name_node = tree.css_first('[itemprop="name"]')
        if name_node:
            name = clean_text(name_node.text() or "")
            if name and 3 < len(name) < 100:
                return name
        
        return ""
    
    @staticmethod
    def resolve_email(tree: HTMLParser, text: str) -> str:
        """Resolve email with validation."""
        # Try mailto links first
        mailto = tree.css_first('a[href^="mailto:"]')
        if mailto:
            href = mailto.attributes.get("href", "")
            email = href.replace("mailto:", "").split("?")[0]
            if "@" in email:
                return email.strip()
        
        # Extract from text
        emails = extract_emails(text)
        if emails:
            # Prefer non-generic emails
            for email in emails:
                if not any(generic in email.lower() for generic in [
                    "example", "test", "noreply", "no-reply"
                ]):
                    return email
            return emails[0]
        
        return ""
    
    @staticmethod
    def resolve_phone(tree: HTMLParser, text: str) -> str:
        """Resolve phone number."""
        # Try tel: links first
        tel = tree.css_first('a[href^="tel:"]')
        if tel:
            href = tel.attributes.get("href", "")
            return href.replace("tel:", "").strip()
        
        # Extract from text
        phones = extract_phones(text)
        return phones[0] if phones else ""
    
    @staticmethod
    def resolve_title(tree: HTMLParser, text: str) -> str:
        """Resolve job title/position."""
        # Try schema.org
        title_node = tree.css_first('[itemprop="jobTitle"]')
        if title_node:
            title = clean_text(title_node.text() or "")
            if title:
                return title
        
        # Try common classes
        for selector in [
            '[class*="title"]',
            '[class*="position"]',
            '[class*="role"]',
            ".job-title",
        ]:
            node = tree.css_first(selector)
            if node:
                title = clean_text(node.text() or "")
                if title and len(title) < 200:
                    # Make sure it's not the person's name
                    name = FieldResolvers.resolve_name(tree, text)
                    if title.lower() != name.lower():
                        return title
        
        return ""
    
    @staticmethod
    def resolve_bio(tree: HTMLParser, text: str, max_length: int = 1000) -> str:
        """Resolve biography/description."""
        # Try schema.org
        desc_node = tree.css_first('[itemprop="description"]')
        if desc_node:
            bio = clean_text(desc_node.text() or "")
            if bio and len(bio) > 50:
                return bio[:max_length]
        
        # Try common bio containers
        for selector in [
            '[class*="bio"]',
            '[class*="about"]',
            '[class*="description"]',
            "#biography",
            "#about",
        ]:
            node = tree.css_first(selector)
            if node:
                bio = clean_text(node.text() or "")
                if bio and len(bio) > 50:
                    return bio[:max_length]
        
        # Fallback: concatenate first few paragraphs
        main = tree.css_first("main") or tree.body
        if main:
            paragraphs = main.css("p")
            texts = []
            for p in paragraphs[:3]:
                p_text = clean_text(p.text() or "")
                if len(p_text) > 30:
                    texts.append(p_text)
            
            if texts:
                combined = " ".join(texts)
                return combined[:max_length]
        
        return ""
    
    @staticmethod
    def resolve_page_url(tree: HTMLParser, current_url: str) -> str:
        """Resolve canonical page URL."""
        # Try canonical link
        canonical = tree.css_first('link[rel="canonical"]')
        if canonical:
            href = canonical.attributes.get("href", "")
            if href:
                return href
        
        # Try og:url
        og_url = tree.css_first('meta[property="og:url"]')
        if og_url:
            content = og_url.attributes.get("content", "")
            if content:
                return content
        
        return current_url
    
    @staticmethod
    def resolve_organization(tree: HTMLParser, text: str) -> str:
        """Resolve organization/department."""
        # Try schema.org
        org_node = tree.css_first('[itemprop="affiliation"]')
        if org_node:
            org = clean_text(org_node.text() or "")
            if org:
                return org
        
        # Try common classes
        for selector in [
            '[class*="department"]',
            '[class*="organization"]',
            '[class*="affiliation"]',
            ".dept",
            ".org",
        ]:
            node = tree.css_first(selector)
            if node:
                org = clean_text(node.text() or "")
                if org and len(org) < 200:
                    return org
        
        return ""
    
    @staticmethod
    def resolve_location(tree: HTMLParser, text: str) -> str:
        """Resolve location/address."""
        # Try schema.org
        addr_node = tree.css_first('[itemprop="address"]')
        if addr_node:
            addr = clean_text(addr_node.text() or "")
            if addr:
                return addr
        
        # Try common classes
        for selector in [
            '[class*="location"]',
            '[class*="address"]',
            ".office",
            ".location",
        ]:
            node = tree.css_first(selector)
            if node:
                loc = clean_text(node.text() or "")
                if loc and len(loc) < 300:
                    return loc
        
        return ""


def resolve_all_fields(tree: HTMLParser, page_url: str) -> Dict[str, Any]:
    """Resolve all common fields using specialized resolvers.
    
    Returns:
        Dictionary with extracted field values
    """
    page_text = tree.text() or ""
    
    return {
        "name": FieldResolvers.resolve_name(tree, page_text),
        "email": FieldResolvers.resolve_email(tree, page_text),
        "phone": FieldResolvers.resolve_phone(tree, page_text),
        "title": FieldResolvers.resolve_title(tree, page_text),
        "bio": FieldResolvers.resolve_bio(tree, page_text),
        "page_url": FieldResolvers.resolve_page_url(tree, page_url),
        "org": FieldResolvers.resolve_organization(tree, page_text),
        "location": FieldResolvers.resolve_location(tree, page_text),
    }

