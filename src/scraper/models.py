"""Core data models for the scraper."""

import hashlib
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urlunparse

from pydantic import BaseModel, Field, field_validator, model_validator


class FieldType(str, Enum):
    """Supported field types in schema."""
    STR = "str"
    STR_OPTIONAL = "str?"
    EMAIL = "email"
    EMAIL_OPTIONAL = "email?"
    URL = "url"
    URL_OPTIONAL = "url?"
    INT = "int"
    INT_OPTIONAL = "int?"
    BOOL = "bool"
    BOOL_OPTIONAL = "bool?"


class FieldSchema(BaseModel):
    """Schema definition for a single field."""
    name: str
    type: FieldType
    pattern: Optional[str] = None  # Regex pattern for validation
    synonyms: List[str] = Field(default_factory=list)  # Label synonyms for extraction


class RecordSchema(BaseModel):
    """Schema definition for extracted records."""
    fields: List[FieldSchema]
    
    def get_field(self, name: str) -> Optional[FieldSchema]:
        """Get field schema by name."""
        for field in self.fields:
            if field.name == name:
                return field
        return None
    
    def is_required(self, field_name: str) -> bool:
        """Check if a field is required (not optional)."""
        field = self.get_field(field_name)
        if not field:
            return False
        return not field.type.value.endswith("?")
    
    def to_json_types(self) -> Dict[str, str]:
        """Convert to simple JSON type mapping."""
        return {f.name: f.type.value for f in self.fields}


class ExtractedRecord(BaseModel):
    """A single extracted record with validation."""
    data: Dict[str, Any]
    source_url: str
    extraction_method: str = "heuristic"  # heuristic, llm, hybrid
    confidence: float = 1.0
    warnings: List[str] = Field(default_factory=list)
    
    @property
    def record_id(self) -> str:
        """Generate stable ID for deduplication."""
        # Use page_url if available, otherwise hash name+email
        if "page_url" in self.data and self.data["page_url"]:
            return hashlib.sha256(self.data["page_url"].encode()).hexdigest()[:16]
        
        key_parts = []
        for field in ["name", "email"]:
            if field in self.data and self.data[field]:
                key_parts.append(str(self.data[field]).lower().strip())
        
        if key_parts:
            key_str = "|".join(key_parts)
            return hashlib.sha256(key_str.encode()).hexdigest()[:16]
        
        # Fallback to source URL
        return hashlib.sha256(self.source_url.encode()).hexdigest()[:16]


class PaginationStrategy(str, Enum):
    """Pagination detection strategies."""
    NEXT_LINK = "next_link"  # <a rel="next"> or "Next" link
    NUMBERED = "numbered"  # Page numbers 1, 2, 3...
    CURSOR = "cursor"  # ?cursor=xyz
    INFINITE_SCROLL = "infinite_scroll"  # Load more on scroll
    NONE = "none"  # Single page


class PageTask(BaseModel):
    """A page to fetch and extract."""
    url: str
    page_num: int = 1
    parent_url: Optional[str] = None
    task_id: str = ""
    depth: int = 0  # 0 = list page, 1 = detail page
    
    @model_validator(mode="after")
    def set_task_id(self) -> "PageTask":
        """Generate stable task ID from normalized URL."""
        if not self.task_id:
            normalized = normalize_url(self.url)
            self.task_id = hashlib.sha256(normalized.encode()).hexdigest()
        return self


class SiteHints(BaseModel):
    """Optional hints for a specific site to aid extraction."""
    root_url: Optional[str] = None
    list_item_selector: Optional[str] = None  # CSS selector for person items
    next_page_selector: Optional[str] = None  # Selector for next page link
    profile_link_selector: Optional[str] = None  # Selector for detail page links
    pagination_strategy: Optional[PaginationStrategy] = None
    field_label_synonyms: Dict[str, List[str]] = Field(default_factory=dict)
    requires_js: bool = False  # Whether to use Playwright
    wait_selector: Optional[str] = None  # Selector to wait for when using JS


class RunMetadata(BaseModel):
    """Metadata about a scraping run."""
    run_id: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    start_url: str
    schema: RecordSchema
    site_hints: Optional[SiteHints] = None
    pages_fetched: int = 0
    pages_failed: int = 0
    records_extracted: int = 0
    records_valid: int = 0
    records_invalid: int = 0
    llm_calls: int = 0
    llm_cost: float = 0.0
    duration_ms: Optional[int] = None
    errors: List[str] = Field(default_factory=list)
    params: Dict[str, Any] = Field(default_factory=dict)


class EnrichmentRequest(BaseModel):
    """Request payload for Sixtyfour enrichment."""
    name: Optional[str] = None
    company: Optional[str] = None
    domain: Optional[str] = None
    linkedin_url: Optional[str] = None
    email: Optional[str] = None
    
    @classmethod
    def from_record(cls, record: Dict[str, Any]) -> "EnrichmentRequest":
        """Create enrichment request from extracted record."""
        return cls(
            name=record.get("name"),
            company=record.get("org") or record.get("organization"),
            domain=extract_domain(record.get("page_url")),
            linkedin_url=record.get("linkedin_url"),
            email=record.get("email"),
        )


class EnrichmentResult(BaseModel):
    """Result from Sixtyfour enrichment."""
    original_data: Dict[str, Any]
    enriched_data: Dict[str, Any]
    enrichment_success: bool
    enrichment_fields: List[str] = Field(default_factory=list)
    error: Optional[str] = None


def normalize_url(url: str) -> str:
    """Normalize URL for deduplication."""
    parsed = urlparse(url.lower().strip())
    # Remove fragments, normalize path
    normalized = urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path.rstrip("/") or "/",
        parsed.params,
        parsed.query,
        ""  # Remove fragment
    ))
    return normalized


def extract_domain(url: Optional[str]) -> Optional[str]:
    """Extract domain from URL."""
    if not url:
        return None
    try:
        parsed = urlparse(url)
        return parsed.netloc or None
    except Exception:
        return None

