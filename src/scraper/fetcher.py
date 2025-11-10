"""HTTP fetching with httpx and Playwright."""

import asyncio
from typing import Optional

import httpx
import structlog
from playwright.async_api import async_playwright, Page, Browser
from selectolax.parser import HTMLParser
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from scraper.config import ScraperConfig
from scraper.utils import random_user_agent

logger = structlog.get_logger()


class RateLimiter:
    """Token bucket rate limiter."""
    
    def __init__(self, rate: float):
        """Initialize rate limiter.
        
        Args:
            rate: Requests per second
        """
        self.rate = rate
        self.tokens = rate
        self.last_update = asyncio.get_event_loop().time()
        self.lock = asyncio.Lock()
    
    async def acquire(self) -> None:
        """Acquire token, waiting if necessary."""
        async with self.lock:
            now = asyncio.get_event_loop().time()
            elapsed = now - self.last_update
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            if self.tokens < 1:
                wait_time = (1 - self.tokens) / self.rate
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1


class HTTPFetcher:
    """Fast HTTP fetcher using httpx for static pages."""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.client: Optional[httpx.AsyncClient] = None
        self.rate_limiter = RateLimiter(config.rate_limit)
    
    async def __aenter__(self) -> "HTTPFetcher":
        """Async context manager entry."""
        self.client = httpx.AsyncClient(
            timeout=self.config.timeout,
            follow_redirects=True,
            headers={"User-Agent": self.config.user_agent},
        )
        return self
    
    async def __aexit__(self, *args) -> None:
        """Async context manager exit."""
        if self.client:
            await self.client.aclose()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, asyncio.TimeoutError)),
    )
    async def fetch(self, url: str) -> tuple[str, int]:
        """Fetch URL and return HTML content and status code.
        
        Args:
            url: URL to fetch
            
        Returns:
            Tuple of (html_content, status_code)
        """
        await self.rate_limiter.acquire()
        
        logger.info("fetching", url=url, method="httpx")
        
        if not self.client:
            raise RuntimeError("HTTPFetcher not initialized")
        
        response = await self.client.get(url)
        response.raise_for_status()
        
        return response.text, response.status_code
    
    def parse(self, html: str) -> HTMLParser:
        """Parse HTML with selectolax."""
        return HTMLParser(html)


class PlaywrightFetcher:
    """Playwright fetcher for JS-rendered pages."""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.rate_limiter = RateLimiter(config.rate_limit)
    
    async def __aenter__(self) -> "PlaywrightFetcher":
        """Async context manager entry."""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.config.playwright_headless
        )
        return self
    
    async def __aexit__(self, *args) -> None:
        """Async context manager exit."""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    async def fetch(
        self,
        url: str,
        wait_selector: Optional[str] = None,
        scroll_to_bottom: bool = False,
    ) -> tuple[str, int]:
        """Fetch URL with Playwright and return HTML.
        
        Args:
            url: URL to fetch
            wait_selector: Optional CSS selector to wait for
            scroll_to_bottom: Whether to scroll to bottom (for infinite scroll)
            
        Returns:
            Tuple of (html_content, status_code)
        """
        await self.rate_limiter.acquire()
        
        logger.info("fetching", url=url, method="playwright")
        
        if not self.browser:
            raise RuntimeError("PlaywrightFetcher not initialized")
        
        page = await self.browser.new_page(
            user_agent=random_user_agent(),
        )
        
        try:
            response = await page.goto(url, wait_until="domcontentloaded")
            status_code = response.status if response else 200
            
            # Wait for specific selector if provided
            if wait_selector:
                await page.wait_for_selector(wait_selector, timeout=10000)
            
            # Scroll to bottom if requested (for infinite scroll)
            if scroll_to_bottom:
                await self._scroll_to_bottom(page)
            
            html = await page.content()
            return html, status_code
        finally:
            await page.close()
    
    async def _scroll_to_bottom(self, page: Page, max_scrolls: int = 10) -> None:
        """Scroll to bottom of page to trigger lazy loading."""
        last_height = await page.evaluate("document.body.scrollHeight")
        scrolls = 0
        
        while scrolls < max_scrolls:
            # Scroll to bottom
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1)  # Wait for content to load
            
            # Check if page height changed
            new_height = await page.evaluate("document.body.scrollHeight")
            if new_height == last_height:
                break
            
            last_height = new_height
            scrolls += 1
        
        logger.debug("scrolled_to_bottom", scrolls=scrolls)
    
    def parse(self, html: str) -> HTMLParser:
        """Parse HTML with selectolax."""
        return HTMLParser(html)


class SmartFetcher:
    """Smart fetcher that chooses between HTTP and Playwright based on hints."""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.http_fetcher: Optional[HTTPFetcher] = None
        self.playwright_fetcher: Optional[PlaywrightFetcher] = None
    
    async def __aenter__(self) -> "SmartFetcher":
        """Async context manager entry."""
        self.http_fetcher = await HTTPFetcher(self.config).__aenter__()
        self.playwright_fetcher = await PlaywrightFetcher(self.config).__aenter__()
        return self
    
    async def __aexit__(self, *args) -> None:
        """Async context manager exit."""
        if self.http_fetcher:
            await self.http_fetcher.__aexit__(*args)
        if self.playwright_fetcher:
            await self.playwright_fetcher.__aexit__(*args)
    
    async def fetch(
        self,
        url: str,
        use_playwright: bool = False,
        wait_selector: Optional[str] = None,
        scroll_to_bottom: bool = False,
    ) -> tuple[str, int, HTMLParser]:
        """Fetch and parse URL.
        
        Args:
            url: URL to fetch
            use_playwright: Force Playwright usage
            wait_selector: CSS selector to wait for (Playwright only)
            scroll_to_bottom: Scroll to bottom (Playwright only)
            
        Returns:
            Tuple of (html, status_code, parsed_tree)
        """
        if use_playwright:
            if not self.playwright_fetcher:
                raise RuntimeError("Playwright fetcher not initialized")
            html, status = await self.playwright_fetcher.fetch(
                url, wait_selector, scroll_to_bottom
            )
            tree = self.playwright_fetcher.parse(html)
        else:
            if not self.http_fetcher:
                raise RuntimeError("HTTP fetcher not initialized")
            html, status = await self.http_fetcher.fetch(url)
            tree = self.http_fetcher.parse(html)
        
        return html, status, tree

