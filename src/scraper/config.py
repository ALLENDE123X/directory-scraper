"""Configuration management."""

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel

from scraper.models import RecordSchema, SiteHints, FieldSchema, FieldType

# Load environment variables
load_dotenv()


class ScraperConfig(BaseModel):
    """Global scraper configuration."""
    # Rate limiting
    rate_limit: float = float(os.getenv("DEFAULT_RATE_LIMIT", "10"))  # requests/sec
    max_retries: int = int(os.getenv("DEFAULT_MAX_RETRIES", "3"))
    timeout: int = int(os.getenv("DEFAULT_TIMEOUT", "30"))
    
    # User agent
    user_agent: str = os.getenv(
        "DEFAULT_USER_AGENT",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    
    # LLM settings
    llm_provider: Optional[str] = os.getenv("LLM_PROVIDER")
    llm_api_key: Optional[str] = os.getenv("LLM_API_KEY")
    llm_model: Optional[str] = os.getenv("LLM_MODEL")
    llm_max_tokens: int = 2000
    llm_temperature: float = 0.0
    llm_budget: int = 1000  # Max LLM calls per run
    
    # Sixtyfour API
    sixtyfour_api_key: Optional[str] = os.getenv("SIXTYFOUR_API_KEY")
    sixtyfour_endpoint: str = "https://api.sixtyfour.ai/enrich-lead"
    sixtyfour_batch_size: int = 1  # API processes one lead at a time
    
    # Scraping behavior
    respect_robots: bool = True
    max_concurrent: int = 5
    playwright_headless: bool = True
    
    # Storage
    db_path: str = "scraper_runs.db"


def load_schema(schema_path: str) -> RecordSchema:
    """Load record schema from JSON file."""
    with open(schema_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Convert simple dict format to FieldSchema list
    if isinstance(data, dict) and "fields" not in data:
        fields = []
        for name, type_str in data.items():
            fields.append(FieldSchema(
                name=name,
                type=FieldType(type_str),
                synonyms=[]
            ))
        return RecordSchema(fields=fields)
    
    return RecordSchema(**data)


def load_site_hints(hints_path: str, target_domain: str) -> Optional[SiteHints]:
    """Load site hints from YAML file for a specific domain."""
    if not os.path.exists(hints_path):
        return None
    
    with open(hints_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    
    # Find matching domain
    for domain, hints in data.get("sites", {}).items():
        if domain in target_domain or target_domain in domain:
            return SiteHints(**hints)
    
    return None


def save_schema(schema: RecordSchema, output_path: str) -> None:
    """Save schema to JSON file."""
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(schema.to_json_types(), f, indent=2)

