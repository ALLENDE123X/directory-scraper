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
    """Client for Sixtyfour lead enrichment API.
    
    Supports both synchronous and asynchronous enrichment endpoints.
    The async endpoint is recommended for production use as enrichment
    takes 5-10 minutes per lead.
    """
    
    def __init__(self, config: ScraperConfig, use_async: bool = True):
        self.config = config
        self.endpoint = config.sixtyfour_endpoint
        self.api_key = config.sixtyfour_api_key
        self.batch_size = config.sixtyfour_batch_size
        self.use_async = use_async
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
        Uses async endpoint by default for better performance with long-running requests.
        
        Args:
            records: List of records to enrich
            
        Returns:
            List of enrichment results
        """
        if self.use_async:
            return await self._enrich_batch_async(records)
        else:
            return await self._enrich_batch_sync(records)
    
    async def _enrich_batch_sync(
        self,
        records: List[Dict[str, Any]],
    ) -> List[EnrichmentResult]:
        """Enrich records using synchronous endpoint (blocking)."""
        results = []
        
        for i, record in enumerate(records):
            logger.info("enriching_record", index=i+1, total=len(records), name=record.get("name"))
            result = await self._enrich_single_sync(record)
            results.append(result)
            
            if i < len(records) - 1:
                await asyncio.sleep(2)
        
        return results
    
    async def _enrich_batch_async(
        self,
        records: List[Dict[str, Any]],
    ) -> List[EnrichmentResult]:
        """Enrich records using async endpoint (non-blocking with polling).
        
        This submits all enrichment jobs, then polls for completion.
        Much faster for batch processing.
        """
        # Submit all jobs
        task_ids = []
        for i, record in enumerate(records):
            logger.info("submitting_enrichment", index=i+1, total=len(records), name=record.get("name"))
            task_id = await self._submit_enrichment_job(record)
            if task_id:
                task_ids.append((record, task_id))
            else:
                task_ids.append((record, None))
            
            # Small delay to avoid rate limiting on submission
            if i < len(records) - 1:
                await asyncio.sleep(0.5)
        
        # Poll for all results
        results = []
        for i, (record, task_id) in enumerate(task_ids):
            if task_id:
                logger.info("polling_enrichment", index=i+1, total=len(task_ids), name=record.get("name"))
                result = await self._poll_for_result(record, task_id)
            else:
                result = EnrichmentResult(
                    original_data=record,
                    enriched_data={},
                    enrichment_success=False,
                    error="Failed to submit enrichment job",
                )
            results.append(result)
        
        return results
    
    async def _submit_enrichment_job(
        self,
        record: Dict[str, Any],
    ) -> Optional[str]:
        """Submit an enrichment job to the async endpoint.
        
        Returns:
            Task ID if successful, None otherwise
        """
        if not self.client:
            raise RuntimeError("Client not initialized")
        
        # Build request payload
        lead_info = self._build_lead_info(record)
        struct = self._build_struct()
        
        # Generate idempotency key
        record_key = hashlib.sha256(
            f"{record.get('name', '')}_{record.get('page_url', '')}".encode()
        ).hexdigest()[:16]
        
        try:
            # Submit to async endpoint
            async_endpoint = self.endpoint.replace("/enrich-lead", "/enrich-lead-async")
            response = await self.client.post(
                async_endpoint,
                json={
                    "lead_info": lead_info,
                    "struct": struct,
                },
                headers={"Idempotency-Key": record_key},
            )
            response.raise_for_status()
            
            data = response.json()
            task_id = data.get("task_id")
            
            if task_id:
                logger.info("job_submitted", name=record.get("name"), task_id=task_id)
                return task_id
            else:
                logger.error("no_task_id", name=record.get("name"))
                return None
        
        except Exception as e:
            logger.error("submit_failed", name=record.get("name"), error=str(e))
            return None
    
    async def _poll_for_result(
        self,
        record: Dict[str, Any],
        task_id: str,
        max_wait_seconds: int = 900,  # 15 minutes max
        poll_interval: int = 10,  # Poll every 10 seconds
    ) -> EnrichmentResult:
        """Poll for enrichment result from async endpoint.
        
        Args:
            record: Original record
            task_id: Task ID from submission
            max_wait_seconds: Maximum time to wait for completion
            poll_interval: Seconds between polls
            
        Returns:
            EnrichmentResult
        """
        if not self.client:
            raise RuntimeError("Client not initialized")
        
        status_endpoint = f"https://api.sixtyfour.ai/job-status/{task_id}"
        start_time = asyncio.get_event_loop().time()
        
        while True:
            try:
                response = await self.client.get(status_endpoint)
                response.raise_for_status()
                
                data = response.json()
                status = data.get("status")
                
                if status == "completed":
                    logger.info("job_completed", name=record.get("name"), task_id=task_id)
                    result_data = data.get("result", {})
                    return self._parse_enrichment_result(record, result_data)
                
                elif status == "failed":
                    error = data.get("error", "Unknown error")
                    logger.error("job_failed", name=record.get("name"), task_id=task_id, error=error)
                    return EnrichmentResult(
                        original_data=record,
                        enriched_data={},
                        enrichment_success=False,
                        error=f"Job failed: {error}",
                    )
                
                elif status in ["pending", "processing"]:
                    # Check timeout
                    elapsed = asyncio.get_event_loop().time() - start_time
                    if elapsed > max_wait_seconds:
                        logger.error("polling_timeout", name=record.get("name"), task_id=task_id)
                        return EnrichmentResult(
                            original_data=record,
                            enriched_data={},
                            enrichment_success=False,
                            error=f"Polling timeout after {max_wait_seconds}s",
                        )
                    
                    # Wait before next poll
                    await asyncio.sleep(poll_interval)
                
                else:
                    logger.error("unknown_status", name=record.get("name"), task_id=task_id, status=status)
                    return EnrichmentResult(
                        original_data=record,
                        enriched_data={},
                        enrichment_success=False,
                        error=f"Unknown status: {status}",
                    )
            
            except Exception as e:
                logger.error("polling_error", name=record.get("name"), task_id=task_id, error=str(e))
                return EnrichmentResult(
                    original_data=record,
                    enriched_data={},
                    enrichment_success=False,
                    error=f"Polling error: {str(e)}",
                )
    
    def _build_lead_info(self, record: Dict[str, Any]) -> Dict[str, str]:
        """Build lead_info payload from record."""
        lead_info = {
            "name": record.get("name", ""),
            "company": "Stanford University",  # Could extract from location/org
            "title": record.get("title", ""),
            "location": record.get("location", ""),
        }
        
        if record.get("email"):
            lead_info["email"] = record["email"]
        
        if record.get("page_url"):
            lead_info["profile_url"] = record["page_url"]
        
        return lead_info
    
    def _build_struct(self) -> Dict[str, str]:
        """Build struct payload for enrichment fields."""
        return {
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
    
    def _parse_enrichment_result(
        self,
        record: Dict[str, Any],
        result_data: Dict[str, Any],
    ) -> EnrichmentResult:
        """Parse enrichment result from API response."""
        enriched_data = result_data.get("structured_data", {})
        notes = result_data.get("notes", "")
        confidence_score = result_data.get("confidence_score", 0.0)
        references = result_data.get("references", {})
        
        # Add metadata
        enriched_data["_sixtyfour_notes"] = notes
        enriched_data["_sixtyfour_confidence"] = confidence_score
        enriched_data["_sixtyfour_sources"] = list(references.keys())
        
        return EnrichmentResult(
            original_data=record,
            enriched_data=enriched_data,
            enrichment_success=True,
            enrichment_fields=list(enriched_data.keys()),
        )
    
    @retry(
        stop=stop_after_attempt(2),  # Only 2 retries since each takes 5-10 min
        wait=wait_exponential(multiplier=2, min=10, max=60),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TimeoutException)),
    )
    async def _enrich_single_sync(
        self,
        record: Dict[str, Any],
    ) -> EnrichmentResult:
        """Enrich a single record with retries using synchronous endpoint.
        
        This method blocks for 5-10 minutes per request.
        Consider using async mode for better performance.
        """
        if not self.client:
            raise RuntimeError("Client not initialized")
        
        lead_info = self._build_lead_info(record)
        struct = self._build_struct()
        
        # Generate idempotency key
        record_key = hashlib.sha256(
            f"{record.get('name', '')}_{record.get('page_url', '')}".encode()
        ).hexdigest()[:16]
        
        logger.info(
            "enriching_lead_sync",
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
            result = self._parse_enrichment_result(record, data)
            
            logger.info("lead_enriched_sync", name=record.get("name"))
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
        if self.use_async:
            task_id = await self._submit_enrichment_job(record)
            if task_id:
                return await self._poll_for_result(record, task_id)
            else:
                return EnrichmentResult(
                    original_data=record,
                    enriched_data={},
                    enrichment_success=False,
                    error="Failed to submit enrichment job",
                )
        else:
            return await self._enrich_single_sync(record)

