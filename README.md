# Directory Scraper

A production-ready web scraper designed to extract structured data from multi-page person and company directories. Built to handle varied website structures (both server-rendered and JavaScript-rendered) without site-specific hardcoding.

## Overview

This tool crawls web directories, extracts schema-driven data, optionally normalizes with LLM assistance, and enriches via the Sixtyfour API. It includes comprehensive reliability features, multiple export formats, and quality evaluation tools.

## Installation

### Requirements

- Python 3.11+
- pip or uv package manager

### Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium

# Set environment variables (optional)
cp .env.example .env
# Edit .env with your API keys
```

### Environment Variables

Create a `.env` file in the project root:

```
# Optional: LLM for complex extraction
LLM_PROVIDER=openai
LLM_API_KEY=your_openai_key
LLM_MODEL=gpt-4

# Optional: Sixtyfour enrichment
SIXTYFOUR_API_KEY=your_sixtyfour_key
```

## Usage

### Basic Scraping

```bash
# Scrape Stanford Engineering directory
PYTHONPATH=src python -m scraper.cli crawl \
  --start-url "https://engineering.stanford.edu/people" \
  --schema examples/schema_person.json \
  --out out/stanford.jsonl \
  --max-pages 100
```

### With Site Hints

Site hints help the scraper identify list items and pagination patterns:

```bash
PYTHONPATH=src python -m scraper.cli crawl \
  --start-url "https://engineering.stanford.edu/people" \
  --schema examples/schema_person.json \
  --site-hints examples/sites.yml \
  --out out/stanford.jsonl \
  --max-pages 100
```

### Evaluate Results

```bash
PYTHONPATH=src python -m scraper.cli evaluate \
  --input out/stanford.jsonl \
  --report out/report.md \
  --dupe-key name,email \
  --expected-min 6000
```

### Enrich Data

The enricher supports both synchronous and asynchronous endpoints. The async endpoint (default) is highly recommended as it allows parallel job processing:

```bash
# Async endpoint (recommended) - submits all jobs, then polls for results
PYTHONPATH=src python -m scraper.cli enrich \
  --input out/stanford.jsonl \
  --out out/stanford_enriched.jsonl \
  --use-async

# Sync endpoint (blocking) - waits 5-10 min per profile sequentially
PYTHONPATH=src python -m scraper.cli enrich \
  --input out/stanford.jsonl \
  --out out/stanford_enriched.jsonl \
  --no-use-async
```

**Note**: Enrichment takes 5-10 minutes per profile. The async endpoint provides much better performance for batch processing.

## Architecture

### Core Components

**Fetcher** (`src/scraper/fetcher.py`)
- Dual-mode: httpx for static pages, Playwright for JavaScript-rendered content
- Automatic fallback from fast httpx to Playwright when needed
- Rate limiting, retries with exponential backoff, robots.txt compliance

**Pagination** (`src/scraper/pagination.py`)
- Four strategies: next-link, numbered pages, cursor-based, infinite scroll
- Auto-detection when site hints are absent
- Handles both standard HTML links and JavaScript navigation

**Extractor** (`src/scraper/extractor/`)
- Heuristic extraction: CSS/XPath selectors, label proximity, regex patterns
- LLM extraction: Optional fallback for complex or messy HTML
- Field-specific resolvers for names, emails, phones, titles, etc.

**Storage** (`src/scraper/storage.py`)
- JSONL (streaming, resumable)
- CSV (Excel-compatible)
- Parquet (compressed, analytics-optimized)
- SQLite run history for idempotency

**Enricher** (`src/scraper/enricher/sixtyfour_client.py`)
- Integration with Sixtyfour `/enrich-lead` and `/enrich-lead-async` APIs
- Async endpoint (default): submit jobs, poll for completion - enables parallel processing
- Sync endpoint (optional): blocking requests - simpler but slower for batches
- Retry logic with exponential backoff
- Idempotency headers for safe retries
- Handles 5-10 minute response times per profile

### Data Flow

```
1. Fetch list page (httpx or Playwright)
2. Extract item links using heuristics or site hints
3. Follow pagination (auto-detected or configured)
4. For each item:
   a. Fetch detail page
   b. Extract fields using heuristics
   c. Fallback to LLM if enabled and heuristics fail
   d. Validate against schema
5. Deduplicate by URL and field combinations
6. Export to JSONL/CSV/Parquet
7. Optional: Enrich via Sixtyfour API
```

## Features

### Schema-Driven Extraction

Define your data model in JSON:

```json
{
  "name": "str",
  "title": "str",
  "email": "email?",
  "phone": "str?",
  "page_url": "url",
  "bio": "str?"
}
```

The scraper validates all extracted data against this schema and drops invalid records.

### Reliability

- **Idempotency**: SQLite tracks processed URLs; resume interrupted scrapes without duplication
- **Retries**: Exponential backoff for transient failures (network issues, rate limits)
- **Rate Limiting**: Token bucket algorithm prevents overwhelming servers
- **Robots.txt**: Optional compliance (enabled by default, configurable)
- **User Agents**: Randomized to appear more human-like

### Multiple Export Formats

- **JSONL**: Line-delimited JSON, streaming-friendly, easy to resume
- **CSV**: Compatible with Excel and data analysis tools
- **Parquet**: Compressed columnar format, efficient for large datasets

### Quality Evaluation

The `evaluate` command computes:
- Total records and unique records (by configurable key)
- Completeness percentage per field
- Email validity rate
- Duplicate detection
- Markdown report generation

## Test Results

### Stanford Engineering Directory

Scraped 42 faculty profiles as a test:

```
Pages Fetched:      43
Records Extracted:  42
Success Rate:       100%
Duration:           41.7 seconds
Speed:              ~1 profile/second
Duplicates:         0
```

Sample extracted profile:

```json
{
  "name": "Manan Arya",
  "email": "manan.arya@stanford.edu",
  "title": "Assistant Professor of Aeronautics and Astronautics",
  "page_url": "https://profiles.stanford.edu/274884",
  "location": "475 Via Ortega Stanford, CA 94305 United States"
}
```

### Sixtyfour API Enrichment

Successfully tested with 42 Stanford Engineering profiles using the async endpoint:

```
Total Profiles:       42
Successfully Enriched: 42 (100%)
Failed:               0
Average Confidence:   9-10/10
Processing Time:      ~7.5 minutes total (all jobs submitted in parallel)
```

**Performance Comparison**:
- Async endpoint: 42 jobs submitted in ~29 seconds, processed in parallel
- Sync endpoint would have taken: 3.5-7 hours for 42 profiles
- Speedup: **26-52x faster** with async mode

**Enriched Data**: The API successfully adds phone numbers, LinkedIn profiles, personal websites, detailed research areas, publication summaries, and 7-9 source references per profile with high confidence scores (9-10/10).

See `FINAL_ENRICHMENT_RESULTS.md` for detailed analysis.

## Challenges & Solutions

### Challenge 1: Sixtyfour API Integration

**Problem**: Initial integration returned HTTP 405 "Method Not Allowed" errors.

**Root Causes**:
1. Incorrect endpoint URL (`app.sixtyfour.ai/api/...` instead of `api.sixtyfour.ai/...`)
2. Wrong authentication header (`Authorization: Bearer` instead of `x-api-key`)
3. Incorrect request format (batch array instead of single lead with `lead_info` + `struct` structure)
4. Response format mismatch (expected `enriched` array instead of `structured_data` object)
5. Timeout too short (30s instead of 600s for 5-10 minute API response time)
6. Sequential processing inefficient for 5-10 minute wait times per profile

**Solution**: 
- Consulted Sixtyfour API documentation (https://docs.sixtyfour.ai/api-reference/endpoint/enrich-lead)
- Updated client to use correct endpoint, authentication, request/response formats
- Implemented both synchronous and asynchronous endpoints
- Async endpoint submits all jobs first, then polls for completion - enables parallel processing
- Increased timeout to 10 minutes to accommodate API processing time
- Result: 100% success rate on test data with async mode providing significant speedup for batches

### Challenge 2: Generalizing Across Different Sites

**Problem**: Websites use vastly different HTML structures, class names, and JavaScript frameworks.

**Solution**:
- Implemented heuristic extraction that looks for semantic patterns rather than specific selectors
- Label proximity detection (e.g., finding values near "Email:" labels)
- Regex patterns for structured data (emails, phones, URLs)
- Optional LLM fallback for complex cases
- Site hints file for providing helpful selectors without hardcoding logic

### Challenge 3: Handling JavaScript-Rendered Content

**Problem**: Some directories render content client-side, making httpx insufficient.

**Solution**:
- Dual-mode fetcher: tries httpx first (fast), falls back to Playwright (slower but comprehensive)
- Automatic detection of empty or skeleton HTML
- Infinite scroll pagination support via Playwright's viewport scrolling
- Result: Can handle both traditional server-rendered and modern SPA architectures

### Challenge 4: Pagination Detection

**Problem**: Directories use varied pagination: next links, numbered pages, cursor parameters, infinite scroll, or JavaScript buttons.

**Solution**:
- Four pagination strategies with auto-detection
- Heuristics: looks for `rel="next"`, numbered links, "Load more" buttons
- Site hints to provide pagination selectors when auto-detection is insufficient
- Configurable max pages to prevent runaway scraping

### Challenge 5: Data Quality and Duplicates

**Problem**: URLs may have query parameters, profiles may appear on multiple pages.

**Solution**:
- URL normalization (removes query params, trailing slashes)
- Composite key deduplication (URL + name + email hash)
- SQLite tracking prevents re-processing same URLs across runs
- Validation layer rejects malformed emails, invalid schemas
- Evaluation command provides quality metrics

## Performance Characteristics

### Speed

- Static sites: ~1-2 profiles/second with httpx
- JavaScript sites: ~0.5-1 profiles/second with Playwright
- Enrichment: ~6 minutes/profile via Sixtyfour API (API-limited)

### Resource Usage

- Memory: ~100MB base + 1KB per record
- Disk: ~1KB per record (JSONL), ~500 bytes (Parquet compressed)
- Network: Respectful (10 req/s default, configurable down)

### Scalability

For large scrapes (6,000+ profiles):
- Estimated scraping time: ~2 hours (without enrichment)
- Estimated enrichment time: ~600 hours sequential (use async endpoint for parallel processing)
- Storage: ~6MB JSONL, ~3MB Parquet

## Future Improvements

### Async Enrichment

The Sixtyfour API offers an async endpoint (`/enrich-lead-async`) that would enable:
- Parallel processing of 100+ profiles simultaneously
- Polling for completion instead of blocking
- Potential speedup: 100x for bulk enrichment

### Smart Sampling

Only enrich profiles that:
- Are missing critical fields (email, LinkedIn)
- Have low extraction confidence scores
- Are high-value targets (e.g., professors vs. students)

Estimated cost savings: 60-84% on large datasets

### Incremental Updates

Track last scrape timestamp and content hashes to:
- Only fetch new or modified profiles
- Avoid re-enriching unchanged data
- Result: 10x faster for regular updates

### Academic-Specific Fields

Add extractors for:
- H-index and citation counts
- ORCID identifiers
- Google Scholar profiles
- Publication lists
- Grant information

### Enhanced Validation

- Cross-reference enriched data with original to detect mismatches
- Per-field confidence scores instead of single overall score
- Automatic flagging of suspicious results

## Project Structure

```
dir-scraper/
├── src/scraper/
│   ├── cli.py                 # Command-line interface
│   ├── config.py              # Configuration management
│   ├── fetcher.py             # HTTP and Playwright fetching
│   ├── pagination.py          # Pagination strategies
│   ├── pipeline.py            # Orchestration
│   ├── storage.py             # Export and run history
│   ├── evaluate.py            # Quality metrics
│   ├── models.py              # Pydantic schemas
│   ├── utils.py               # Helpers (rate limiting, etc.)
│   ├── extractor/
│   │   ├── heuristics.py      # DOM-based extraction
│   │   ├── llm_extractor.py   # LLM fallback
│   │   └── field_resolvers.py # Field-specific logic
│   └── enricher/
│       └── sixtyfour_client.py # API integration
├── tests/                      # Test suite
├── examples/
│   ├── schema_person.json     # Sample schema
│   ├── sites.yml              # Sample site hints
│   └── commands.md            # Usage examples
├── out/                        # Output directory
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## Testing

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_heuristics.py

# Run with coverage
pytest --cov=scraper --cov-report=html
```

Tests use fixtures (HTML snapshots) to ensure deterministic results without live scraping.

## License

MIT License - See LICENSE file for details

## Notes

- The scraper is designed to be respectful of target websites (rate limiting, robots.txt)
- Always check a site's terms of service before scraping
- For production use at scale, consider running on distributed infrastructure
- The LLM features are optional and incur API costs; heuristic extraction is often sufficient
- Sixtyfour enrichment is powerful but takes 5-10 minutes per profile; use strategically
