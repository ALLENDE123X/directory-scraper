# Example Commands

This document provides copy-paste commands for common scraping tasks.

## Stanford Engineering Directory Crawl

Full crawl of the Stanford School of Engineering directory:

```bash
dirscrape crawl \
  --start-url "https://engineering.stanford.edu/people" \
  --schema examples/schema_person.json \
  --site-hints examples/sites.yml \
  --out out/stanford.jsonl \
  --max-pages 2000 \
  --llm off
```

## Evaluate Results

Evaluate the scraped Stanford data with quality checks:

```bash
dirscrape evaluate \
  --input out/stanford.jsonl \
  --expected-min 6200 \
  --dupe-key name,email \
  --report out/stanford_eval.md \
  --stanford-check
```

## Enrich Sample

Enrich 100 records using the Sixtyfour API:

```bash
export SIXTYFOUR_API_KEY="your-api-key-here"

dirscrape enrich \
  --input out/stanford.jsonl \
  --sample 100 \
  --endpoint https://app.sixtyfour.ai/api/enrich-lead \
  --api-key $SIXTYFOUR_API_KEY \
  --out out/enriched.jsonl
```

## Quick Start Examples

### Simple Crawl (No Site Hints)

Let the scraper auto-detect pagination and structure:

```bash
dirscrape crawl \
  --start-url "https://example.edu/faculty" \
  --schema examples/schema_person.json \
  --out out/results.jsonl \
  --max-pages 100
```

### Crawl with LLM Enhancement

Use GPT-4 to fill in missing fields:

```bash
export LLM_PROVIDER=openai
export LLM_API_KEY="your-openai-api-key"

dirscrape crawl \
  --start-url "https://example.edu/faculty" \
  --schema examples/schema_person.json \
  --out out/results.jsonl \
  --llm on \
  --max-pages 50
```

### Export to Different Formats

The scraper supports JSONL, CSV, and Parquet:

```bash
# JSONL (default, streaming-friendly)
dirscrape crawl ... --out out/results.jsonl

# CSV (Excel-compatible)
dirscrape crawl ... --out out/results.csv

# Parquet (efficient for large datasets)
dirscrape crawl ... --out out/results.parquet
```

### Rate Limiting

Control request rate to be respectful:

```bash
dirscrape crawl \
  --start-url "https://example.edu/faculty" \
  --schema examples/schema_person.json \
  --out out/results.jsonl \
  --rate-limit 2.0  # 2 requests per second
```

### Force Re-scraping

Skip idempotency checks and re-scrape everything:

```bash
dirscrape crawl \
  --start-url "https://example.edu/faculty" \
  --schema examples/schema_person.json \
  --out out/results.jsonl \
  --force
```

## Batch Processing

Process multiple directories:

```bash
#!/bin/bash

# Define directories to scrape
URLS=(
  "https://engineering.stanford.edu/people"
  "https://www.eecs.mit.edu/people/faculty-advisors/"
  "https://eecs.berkeley.edu/people/faculty"
)

# Scrape each
for url in "${URLS[@]}"; do
  domain=$(echo "$url" | cut -d'/' -f3)
  echo "Scraping $domain..."
  
  dirscrape crawl \
    --start-url "$url" \
    --schema examples/schema_person.json \
    --site-hints examples/sites.yml \
    --out "out/${domain}.jsonl" \
    --max-pages 500
  
  # Evaluate
  dirscrape evaluate \
    --input "out/${domain}.jsonl" \
    --report "out/${domain}_eval.md"
done
```

## Development/Testing

Test with small page limits:

```bash
dirscrape crawl \
  --start-url "https://engineering.stanford.edu/people" \
  --schema examples/schema_person.json \
  --out out/test.jsonl \
  --max-pages 5 \
  --max-runtime 60
```

## Check Version

```bash
dirscrape version
```

