.PHONY: install test lint format clean run-stanford

install:
	pip install -r requirements.txt
	playwright install chromium

test:
	pytest tests/ -v

test-cov:
	pytest tests/ --cov=scraper --cov-report=html --cov-report=term

lint:
	ruff check src/
	mypy src/

format:
	black src/ tests/
	ruff check --fix src/

clean:
	rm -rf out/*.jsonl out/*.csv out/*.parquet
	rm -rf *.db
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .ruff_cache
	rm -rf htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

run-stanford:
	dirscrape crawl \
		--start-url "https://engineering.stanford.edu/people" \
		--schema examples/schema_person.json \
		--site-hints examples/sites.yml \
		--out out/stanford.jsonl \
		--max-pages 2000 \
		--llm off

eval-stanford:
	dirscrape evaluate \
		--input out/stanford.jsonl \
		--expected-min 6200 \
		--dupe-key name,email \
		--report out/stanford_eval.md \
		--stanford-check

enrich-sample:
	dirscrape enrich \
		--input out/stanford.jsonl \
		--sample 100 \
		--out out/enriched.jsonl

