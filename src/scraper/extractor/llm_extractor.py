"""LLM-based extraction and normalization."""

import json
from typing import Any, Dict, Optional

import structlog
from anthropic import AsyncAnthropic
from openai import AsyncOpenAI

from scraper.config import ScraperConfig
from scraper.models import RecordSchema
from scraper.utils import clean_text, truncate_text

logger = structlog.get_logger()


SYSTEM_PROMPT = """You extract structured person records from messy directory HTML.
Return STRICT JSON matching the provided schema.
If a field is missing, use an empty string.
Do not add extra keys. Do not include explanations."""


class LLMExtractor:
    """Extract and normalize records using LLM."""
    
    def __init__(self, config: ScraperConfig, schema: RecordSchema):
        self.config = config
        self.schema = schema
        self.calls_made = 0
        self.total_cost = 0.0
        
        # Initialize client based on provider
        if config.llm_provider == "openai" and config.llm_api_key:
            self.client = AsyncOpenAI(api_key=config.llm_api_key)
            self.provider = "openai"
        elif config.llm_provider == "anthropic" and config.llm_api_key:
            self.client = AsyncAnthropic(api_key=config.llm_api_key)
            self.provider = "anthropic"
        else:
            self.client = None
            self.provider = None
    
    def is_enabled(self) -> bool:
        """Check if LLM extraction is enabled."""
        return self.client is not None
    
    async def extract(self, text: str, page_url: str) -> Optional[Dict[str, Any]]:
        """Extract record from text using LLM.
        
        Args:
            text: Cleaned text from page
            page_url: URL of the page
            
        Returns:
            Extracted record or None if budget exceeded or extraction failed
        """
        if not self.is_enabled():
            return None
        
        if self.calls_made >= self.config.llm_budget:
            logger.warning("llm_budget_exceeded", budget=self.config.llm_budget)
            return None
        
        # Truncate text to avoid excessive tokens
        text = truncate_text(text, max_length=4000)
        
        # Build user prompt
        user_prompt = self._build_prompt(text)
        
        try:
            if self.provider == "openai":
                result = await self._extract_openai(user_prompt)
            elif self.provider == "anthropic":
                result = await self._extract_anthropic(user_prompt)
            else:
                return None
            
            if result:
                result["page_url"] = page_url
                self.calls_made += 1
                return result
            
        except Exception as e:
            logger.error("llm_extraction_failed", error=str(e), url=page_url)
        
        return None
    
    async def normalize_field(self, field_name: str, value: str) -> str:
        """Normalize a single field value using LLM.
        
        Useful for cleaning up messy values, splitting names, etc.
        """
        if not self.is_enabled() or self.calls_made >= self.config.llm_budget:
            return value
        
        prompt = f"""Normalize this {field_name} field value:
        
Input: "{value}"

Return only the cleaned/normalized value, nothing else."""
        
        try:
            if self.provider == "openai":
                response = await self.client.chat.completions.create(
                    model=self.config.llm_model or "gpt-4-turbo-preview",
                    messages=[
                        {"role": "system", "content": "You normalize and clean data fields."},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0,
                    max_tokens=100,
                )
                self.calls_made += 1
                return response.choices[0].message.content.strip()
            
            elif self.provider == "anthropic":
                response = await self.client.messages.create(
                    model=self.config.llm_model or "claude-3-sonnet-20240229",
                    max_tokens=100,
                    temperature=0,
                    system="You normalize and clean data fields.",
                    messages=[{"role": "user", "content": prompt}],
                )
                self.calls_made += 1
                return response.content[0].text.strip()
        
        except Exception as e:
            logger.error("llm_normalization_failed", field=field_name, error=str(e))
        
        return value
    
    def _build_prompt(self, text: str) -> str:
        """Build extraction prompt."""
        schema_json = self.schema.to_json_types()
        
        return f"""SCHEMA (JSON keys/types):
{json.dumps(schema_json, indent=2)}

EXTRACT from this TEXT (already DOM-cleaned):
\"\"\"
{text}
\"\"\"

Return ONLY JSON with the schema keys."""
    
    async def _extract_openai(self, prompt: str) -> Optional[Dict[str, Any]]:
        """Extract using OpenAI."""
        response = await self.client.chat.completions.create(
            model=self.config.llm_model or "gpt-4-turbo-preview",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=self.config.llm_temperature,
            max_tokens=self.config.llm_max_tokens,
        )
        
        content = response.choices[0].message.content
        if not content:
            return None
        
        # Parse JSON
        try:
            data = json.loads(content)
            return data
        except json.JSONDecodeError as e:
            logger.error("llm_json_parse_failed", error=str(e), content=content)
            return None
    
    async def _extract_anthropic(self, prompt: str) -> Optional[Dict[str, Any]]:
        """Extract using Anthropic."""
        response = await self.client.messages.create(
            model=self.config.llm_model or "claude-3-sonnet-20240229",
            max_tokens=self.config.llm_max_tokens,
            temperature=self.config.llm_temperature,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        
        content = response.content[0].text
        if not content:
            return None
        
        # Try to extract JSON from response
        try:
            # Sometimes Claude wraps JSON in markdown
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            elif "```" in content:
                content = content.split("```")[1].split("```")[0]
            
            data = json.loads(content.strip())
            return data
        except (json.JSONDecodeError, IndexError) as e:
            logger.error("llm_json_parse_failed", error=str(e), content=content)
            return None


async def hybrid_extract(
    text: str,
    page_url: str,
    schema: RecordSchema,
    llm_extractor: Optional[LLMExtractor],
    heuristic_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Hybrid extraction: use heuristics first, LLM for missing fields.
    
    Args:
        text: Page text
        page_url: URL
        schema: Record schema
        llm_extractor: Optional LLM extractor
        heuristic_data: Data already extracted via heuristics
        
    Returns:
        Combined record
    """
    record = heuristic_data.copy()
    
    # Check which required fields are missing
    missing_fields = []
    for field in schema.fields:
        if schema.is_required(field.name):
            if field.name not in record or not record[field.name]:
                missing_fields.append(field.name)
    
    # If we have missing required fields and LLM is available, try LLM
    if missing_fields and llm_extractor and llm_extractor.is_enabled():
        logger.debug("using_llm_for_missing_fields", fields=missing_fields, url=page_url)
        
        llm_data = await llm_extractor.extract(text, page_url)
        if llm_data:
            # Fill in missing fields from LLM
            for field in missing_fields:
                if field in llm_data and llm_data[field]:
                    record[field] = llm_data[field]
    
    return record

