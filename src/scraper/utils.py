"""Utility functions for scraping."""

import hashlib
import random
import re
from typing import Optional
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import structlog
import tldextract

logger = structlog.get_logger()


def generate_run_id() -> str:
    """Generate unique run ID."""
    import time
    timestamp = int(time.time() * 1000)
    random_part = random.randint(1000, 9999)
    return f"run_{timestamp}_{random_part}"


def normalize_url(url: str) -> str:
    """Normalize URL for comparison and deduplication."""
    url = url.strip().lower()
    # Remove fragments
    if "#" in url:
        url = url.split("#")[0]
    # Remove trailing slash from path (but not from root)
    parsed = urlparse(url)
    if parsed.path and parsed.path != "/" and parsed.path.endswith("/"):
        url = url.rstrip("/")
    return url


def make_absolute_url(base: str, url: str) -> str:
    """Convert relative URL to absolute."""
    return urljoin(base, url)


def is_valid_url(url: str) -> bool:
    """Check if URL is valid."""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def extract_domain(url: str) -> str:
    """Extract domain from URL."""
    ext = tldextract.extract(url)
    return f"{ext.domain}.{ext.suffix}"


def get_base_url(url: str) -> str:
    """Get base URL (scheme + netloc)."""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def check_robots_txt(url: str, user_agent: str) -> bool:
    """Check if URL is allowed by robots.txt."""
    try:
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        
        rp = RobotFileParser()
        rp.set_url(robots_url)
        rp.read()
        
        return rp.can_fetch(user_agent, url)
    except Exception as e:
        logger.warning("robots_check_failed", url=url, error=str(e))
        # If we can't check, assume allowed
        return True


def extract_emails(text: str) -> list[str]:
    """Extract email addresses from text."""
    pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    return re.findall(pattern, text)


def extract_phones(text: str) -> list[str]:
    """Extract phone numbers from text."""
    # US/International formats
    patterns = [
        r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',  # 123-456-7890
        r'\b\(\d{3}\)\s*\d{3}[-.]?\d{4}\b',  # (123) 456-7890
        r'\b\+\d{1,3}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}\b',  # International
    ]
    phones = []
    for pattern in patterns:
        phones.extend(re.findall(pattern, text))
    return phones


def clean_text(text: str) -> str:
    """Clean and normalize text."""
    if not text:
        return ""
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text)
    # Remove special characters but keep basic punctuation
    text = re.sub(r'[^\w\s@.,;:!?()\-]', '', text)
    return text.strip()


def truncate_text(text: str, max_length: int = 500) -> str:
    """Truncate text to max length."""
    if len(text) <= max_length:
        return text
    return text[:max_length] + "..."


def hash_content(content: str) -> str:
    """Generate hash of content."""
    return hashlib.sha256(content.encode()).hexdigest()


def generate_task_id(url: str) -> str:
    """Generate stable task ID from URL."""
    normalized = normalize_url(url)
    return hashlib.sha256(normalized.encode()).hexdigest()


def random_user_agent() -> str:
    """Get random user agent."""
    agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    ]
    return random.choice(agents)


def estimate_reading_time(text: str, wpm: int = 200) -> int:
    """Estimate reading time in seconds."""
    words = len(text.split())
    minutes = words / wpm
    return int(minutes * 60)


def is_person_url(url: str) -> bool:
    """Heuristic check if URL likely points to a person profile."""
    person_patterns = [
        r'/people/',
        r'/person/',
        r'/profile/',
        r'/faculty/',
        r'/staff/',
        r'/team/',
        r'/member/',
        r'/employee/',
        r'/researcher/',
        r'/expert/',
    ]
    url_lower = url.lower()
    return any(re.search(pattern, url_lower) for pattern in person_patterns)


def extract_name_parts(full_name: str) -> dict[str, str]:
    """Split full name into parts."""
    parts = full_name.strip().split()
    if not parts:
        return {"first": "", "middle": "", "last": "", "full": full_name}
    
    if len(parts) == 1:
        return {"first": parts[0], "middle": "", "last": "", "full": full_name}
    elif len(parts) == 2:
        return {"first": parts[0], "middle": "", "last": parts[1], "full": full_name}
    else:
        return {
            "first": parts[0],
            "middle": " ".join(parts[1:-1]),
            "last": parts[-1],
            "full": full_name,
        }


def validate_email(email: str) -> bool:
    """Validate email format."""
    pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$'
    return bool(re.match(pattern, email))

