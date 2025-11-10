"""Sixtyfour API client for lead enrichment."""

import asyncio
import hashlib
from typing import Any, Dict, List, Optional

import httpx
import structlog
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from scraper.config import ScraperConfig
from scraper.models import EnrichmentRequest, EnrichmentResult

logger = structlog.get_logger()


class SixtyfourClient:
    """Client for Sixtyfour lead enrichment API."""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.endpoint = config.sixtyfour_endpoint
        self.api_key = config.sixtyfour_api_key
        self.batch_size = config.sixtyfour_batch_size
        self.client: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self) -> "SixtyfourClient":
        """Async context manager entry."""
        self.client = httpx.AsyncClient(
            timeout=600.0,  # 10 minutes - API takes 5-10 min per request
            headers={
                "x-api-key": self.api_key,  # Correct auth header
                "Content-Type": "application/json",
            },
        )
        return self
    
    async def __aexit__(self, *args) -> None:
        """Async context manager exit."""
        if self.client:
            await self.client.aclose()
    
    async def enrich_batch(
        self,
        records: List[Dict[str, Any]],
    ) -> List[EnrichmentResult]:
        """Enrich a batch of records.
        
        Note: Sixtyfour API processes one lead at a time and takes 5-10 minutes per request.
        
        Args:
            records: List of records to enrich
            
        Returns:
            List of enrichment results
        """
        results = []
        
        # Process one at a time (API limitation)
        for i, record in enumerate(records):
            logger.info("enriching_record", index=i+1, total=len(records), name=record.get("name"))
            result = await self._enrich_single(record)
            results.append(result)
            
            # Small delay between requests
            if i < len(records) - 1:
                await asyncio.sleep(2)
        
        return results
    
    @retry(
        stop=stop_after_attempt(2),  # Only 2 retries since each takes 5-10 min
        wait=wait_exponential(multiplier=2, min=10, max=60),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TimeoutException)),
    )
    async def _enrich_single(
        self,
        record: Dict[str, Any],
    ) -> EnrichmentResult:
        """Enrich a single record with retries.
        
        Sixtyfour API format:
        Request: {
            "lead_info": {...},  # Initial data
            "struct": {field: description, ...}  # Fields to enrich
        }
        
        Response: {
            "notes": "...",
            "structured_data": {...},
            "references": {...},
            "confidence_score": 9.5
        }
        """
        if not self.client:
            raise RuntimeError("Client not initialized")
        
        # Build lead_info from record
        lead_info = {
            "name": record.get("name", ""),
            "company": "Stanford University",  # Could extract from location/org
            "title": record.get("title", ""),
            "location": record.get("location", ""),
        }
        
        # Add email if available
        if record.get("email"):
            lead_info["email"] = record["email"]
        
        # Add LinkedIn or profile URL if available
        if record.get("page_url"):
            lead_info["profile_url"] = record["page_url"]
        
        # Define what fields we want enriched
        struct = {
            "name": "The individual's full name",
            "email": "The individual's email address",
            "phone": "The individual's phone number",
            "company": "The company/institution the individual is associated with",
            "title": "The individual's job title or position",
            "linkedin": "LinkedIn URL for the person",
            "website": "Personal or company website URL",
            "location": "The individual's location",
            "research_areas": "Academic research areas and interests (if applicable)",
            "publications": "Notable publications or research output (if applicable)",
        }
        
        # Generate idempotency key
        record_key = hashlib.sha256(
            f"{record.get('name', '')}_{record.get('page_url', '')}".encode()
        ).hexdigest()[:16]
        
        logger.info(
            "enriching_lead",
            name=record.get("name"),
            idempotency_key=record_key,
        )
        
        try:
            response = await self.client.post(
                self.endpoint,
                json={
                    "lead_info": lead_info,
                    "struct": struct,
                },
                headers={"Idempotency-Key": record_key},
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Extract enriched data from structured_data field
            enriched_data = data.get("structured_data", {})
            notes = data.get("notes", "")
            confidence_score = data.get("confidence_score", 0.0)
            references = data.get("references", {})
            
            # Add metadata
            enriched_data["_sixtyfour_notes"] = notes
            enriched_data["_sixtyfour_confidence"] = confidence_score
            enriched_data["_sixtyfour_sources"] = list(references.keys())
            
            result = EnrichmentResult(
                original_data=record,
                enriched_data=enriched_data,
                enrichment_success=True,
                enrichment_fields=list(enriched_data.keys()),
            )
            
            logger.info("lead_enriched", name=record.get("name"), confidence=confidence_score)
            return result
        
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                logger.warning("rate_limited", status=429)
                raise
            elif e.response.status_code >= 500:
                logger.error("server_error", status=e.response.status_code)
                raise
            else:
                logger.error("enrichment_failed", status=e.response.status_code, error=str(e))
                return EnrichmentResult(
                    original_data=record,
                    enriched_data={},
                    enrichment_success=False,
                    error=f"HTTP {e.response.status_code}: {str(e)}",
                )
        except Exception as e:
            logger.error("enrichment_error", name=record.get("name"), error=str(e))
            return EnrichmentResult(
                original_data=record,
                enriched_data={},
                enrichment_success=False,
                error=str(e),
            )
    
    async def enrich_single(
        self,
        record: Dict[str, Any],
    ) -> EnrichmentResult:
        """Enrich a single record.
        
        Args:
            record: Record to enrich
            
        Returns:
            Enrichment result
        """
        return await self._enrich_single(record)

