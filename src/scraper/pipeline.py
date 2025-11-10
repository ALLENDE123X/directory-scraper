"""Main scraping pipeline orchestration."""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

import structlog

from scraper.config import ScraperConfig, load_site_hints
from scraper.extractor.field_resolvers import resolve_all_fields
from scraper.extractor.heuristics import HeuristicExtractor, extract_from_item
from scraper.extractor.llm_extractor import LLMExtractor, hybrid_extract
from scraper.fetcher import SmartFetcher
from scraper.models import (
    ExtractedRecord,
    PageTask,
    PaginationStrategy,
    RecordSchema,
    RunMetadata,
    SiteHints,
)
from scraper.pagination import (
    PaginationDetector,
    extract_list_items,
    extract_detail_links,
)
from scraper.storage import RunHistory, RecordWriter, deduplicate_records
from scraper.utils import (
    check_robots_txt,
    extract_domain,
    generate_run_id,
    is_person_url,
)

logger = structlog.get_logger()


class ScraperPipeline:
    """Main scraping pipeline orchestrator."""
    
    def __init__(
        self,
        config: ScraperConfig,
        schema: RecordSchema,
        site_hints: Optional[SiteHints] = None,
    ):
        self.config = config
        self.schema = schema
        self.site_hints = site_hints
        self.run_id = generate_run_id()
        
        # Statistics
        self.metadata = RunMetadata(
            run_id=self.run_id,
            started_at=datetime.utcnow(),
            start_url="",
            schema=schema,
            site_hints=site_hints,
        )
        
        # Extracted records
        self.records: List[ExtractedRecord] = []
        self.seen_record_ids = set()
    
    async def run(
        self,
        start_url: str,
        output_path: str,
        max_pages: Optional[int] = None,
        max_runtime: Optional[int] = None,
        use_llm: bool = False,
        force: bool = False,
    ) -> RunMetadata:
        """Run the scraping pipeline.
        
        Args:
            start_url: Starting URL to crawl
            output_path: Path to write output
            max_pages: Maximum pages to fetch
            max_runtime: Maximum runtime in seconds
            use_llm: Enable LLM extraction
            force: Force re-scraping even if already done
            
        Returns:
            Run metadata with statistics
        """
        self.metadata.start_url = start_url
        self.metadata.params = {
            "max_pages": max_pages,
            "max_runtime": max_runtime,
            "use_llm": use_llm,
            "force": force,
        }
        
        logger.info(
            "pipeline_started",
            run_id=self.run_id,
            start_url=start_url,
            max_pages=max_pages,
        )
        
        # Check robots.txt
        if self.config.respect_robots:
            if not check_robots_txt(start_url, self.config.user_agent):
                logger.warning("robots_disallowed", url=start_url)
                self.metadata.errors.append("Disallowed by robots.txt")
                return self.metadata
        
        # Initialize components
        async with (
            SmartFetcher(self.config) as fetcher,
            RunHistory(self.config.db_path) as history,
        ):
            # Create run record
            await history.create_run(self.metadata)
            
            # Initialize LLM extractor if enabled
            llm_extractor = None
            if use_llm:
                llm_extractor = LLMExtractor(self.config, self.schema)
                if not llm_extractor.is_enabled():
                    logger.warning("llm_requested_but_not_configured")
            
            # Load site hints if not provided
            if not self.site_hints:
                domain = extract_domain(start_url)
                # Try to load from default hints file
                import os
                hints_path = os.path.join(os.getcwd(), "examples", "sites.yml")
                if os.path.exists(hints_path):
                    self.site_hints = load_site_hints(hints_path, domain)
            
            # Run crawl
            try:
                await self._crawl(
                    start_url=start_url,
                    fetcher=fetcher,
                    history=history,
                    llm_extractor=llm_extractor,
                    max_pages=max_pages,
                    max_runtime=max_runtime,
                    force=force,
                )
            except Exception as e:
                logger.error("pipeline_failed", error=str(e), exc_info=True)
                self.metadata.errors.append(str(e))
            
            # Finalize
            self.metadata.completed_at = datetime.utcnow()
            duration = (self.metadata.completed_at - self.metadata.started_at).total_seconds()
            self.metadata.duration_ms = int(duration * 1000)
            
            if llm_extractor:
                self.metadata.llm_calls = llm_extractor.calls_made
            
            # Deduplicate records
            records_data = [r.data for r in self.records]
            unique_records = deduplicate_records(records_data, ["page_url", "name"])
            
            self.metadata.records_extracted = len(records_data)
            self.metadata.records_valid = len(unique_records)
            
            # Write output
            await RecordWriter.write_jsonl(unique_records, output_path)
            
            # Update run history
            await history.update_run(self.metadata)
        
        logger.info(
            "pipeline_completed",
            run_id=self.run_id,
            records=len(unique_records),
            pages=self.metadata.pages_fetched,
            duration_sec=duration,
        )
        
        return self.metadata
    
    async def _crawl(
        self,
        start_url: str,
        fetcher: SmartFetcher,
        history: RunHistory,
        llm_extractor: Optional[LLMExtractor],
        max_pages: Optional[int],
        max_runtime: Optional[int],
        force: bool,
    ) -> None:
        """Crawl pages and extract records."""
        # Initialize pagination detector
        detector = PaginationDetector(start_url)
        
        # Queue of pages to process
        task_queue: List[PageTask] = [PageTask(url=start_url, page_num=1)]
        processed = 0
        start_time = datetime.utcnow()
        
        # Determine if we need Playwright
        use_playwright = (
            self.site_hints.requires_js if self.site_hints else False
        )
        
        while task_queue and (not max_pages or processed < max_pages):
            # Check runtime limit
            if max_runtime:
                elapsed = (datetime.utcnow() - start_time).total_seconds()
                if elapsed > max_runtime:
                    logger.warning("max_runtime_exceeded", elapsed=elapsed)
                    break
            
            # Get next task
            task = task_queue.pop(0)
            
            # Check if already processed (idempotency)
            if not force:
                if await history.is_task_completed(self.run_id, task.task_id):
                    logger.debug("task_already_completed", task_id=task.task_id)
                    continue
            
            # Fetch page
            try:
                html, status, tree = await fetcher.fetch(
                    task.url,
                    use_playwright=use_playwright,
                    wait_selector=self.site_hints.wait_selector if self.site_hints else None,
                )
                
                self.metadata.pages_fetched += 1
                processed += 1
                
                await history.log_event(
                    self.run_id,
                    "page_fetched",
                    task.task_id,
                    {"url": task.url, "status": status},
                )
                
                logger.info(
                    "page_fetched",
                    url=task.url,
                    status=status,
                    page_num=task.page_num,
                    processed=processed,
                )
                
            except Exception as e:
                logger.error("page_fetch_failed", url=task.url, error=str(e))
                self.metadata.pages_failed += 1
                continue
            
            # Decide extraction strategy based on URL and depth
            if task.depth == 0:
                # This is a list page - extract items and pagination
                await self._extract_from_list_page(
                    tree=tree,
                    page_url=task.url,
                    detector=detector,
                    task_queue=task_queue,
                    llm_extractor=llm_extractor,
                )
            else:
                # This is a detail page - extract full record
                await self._extract_from_detail_page(
                    tree=tree,
                    page_url=task.url,
                    llm_extractor=llm_extractor,
                )
    
    async def _extract_from_list_page(
        self,
        tree,
        page_url: str,
        detector: PaginationDetector,
        task_queue: List[PageTask],
        llm_extractor: Optional[LLMExtractor],
    ) -> None:
        """Extract records from a list page."""
        # Extract list items (person cards)
        item_selector = (
            self.site_hints.list_item_selector if self.site_hints else None
        )
        items = extract_list_items(tree, item_selector)
        
        logger.debug("extracted_list_items", count=len(items), url=page_url)
        
        # Extract records from items
        for item in items:
            try:
                record_data = extract_from_item(item, self.schema, page_url)
                
                # Create extracted record
                record = ExtractedRecord(
                    data=record_data,
                    source_url=page_url,
                    extraction_method="heuristic",
                    confidence=0.8,
                )
                
                # Deduplicate
                if record.record_id not in self.seen_record_ids:
                    self.seen_record_ids.add(record.record_id)
                    self.records.append(record)
                
            except Exception as e:
                logger.error("item_extraction_failed", error=str(e))
        
        # Check if items have detail page links
        link_selector = (
            self.site_hints.profile_link_selector if self.site_hints else None
        )
        detail_links = extract_detail_links(tree, page_url, link_selector)
        
        # If we found detail links, queue them for processing
        if detail_links:
            logger.debug("found_detail_links", count=len(detail_links))
            for link in detail_links[:50]:  # Limit detail pages per list page
                if is_person_url(link):
                    task = PageTask(url=link, parent_url=page_url, depth=1)
                    task_queue.append(task)
        
        # Detect and extract pagination
        strategy = (
            self.site_hints.pagination_strategy
            if self.site_hints and self.site_hints.pagination_strategy
            else detector.detect_strategy(tree)
        )
        
        logger.debug("pagination_strategy", strategy=strategy.value)
        
        # Extract next pages
        next_tasks = detector.extract_next_pages(tree, page_url, strategy)
        task_queue.extend(next_tasks)
    
    async def _extract_from_detail_page(
        self,
        tree,
        page_url: str,
        llm_extractor: Optional[LLMExtractor],
    ) -> None:
        """Extract full record from a detail/profile page."""
        page_text = tree.text() or ""
        
        # Use specialized field resolvers
        record_data = resolve_all_fields(tree, page_url)
        
        # If LLM is enabled, use hybrid extraction for missing fields
        if llm_extractor:
            record_data = await hybrid_extract(
                text=page_text,
                page_url=page_url,
                schema=self.schema,
                llm_extractor=llm_extractor,
                heuristic_data=record_data,
            )
        
        # Create extracted record
        record = ExtractedRecord(
            data=record_data,
            source_url=page_url,
            extraction_method="hybrid" if llm_extractor else "heuristic",
            confidence=0.9,
        )
        
        # Deduplicate
        if record.record_id not in self.seen_record_ids:
            self.seen_record_ids.add(record.record_id)
            self.records.append(record)
            
            logger.debug("extracted_detail_record", url=page_url, name=record_data.get("name"))

