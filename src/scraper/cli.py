"""CLI interface for the directory scraper."""

import asyncio
import json
from pathlib import Path
from typing import List, Optional

import structlog
import typer
from rich.console import Console
from rich.table import Table

from scraper.config import ScraperConfig, load_schema
from scraper.enricher.sixtyfour_client import SixtyfourClient
from scraper.evaluate import evaluate_records, check_stanford_profile_count
from scraper.pipeline import ScraperPipeline
from scraper.storage import RecordReader, RecordWriter

# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

app = typer.Typer(
    name="dirscrape",
    help="Generalized directory scraper with LLM-assisted normalization",
)
console = Console()


@app.command()
def crawl(
    start_url: str = typer.Option(..., help="Starting URL to crawl"),
    schema: str = typer.Option(..., help="Path to schema JSON file"),
    out: str = typer.Option(..., help="Output file path (.jsonl, .csv, or .parquet)"),
    site_hints: Optional[str] = typer.Option(None, help="Path to site hints YAML"),
    max_pages: Optional[int] = typer.Option(None, help="Maximum pages to fetch"),
    max_runtime: Optional[int] = typer.Option(None, help="Maximum runtime in seconds"),
    llm: str = typer.Option("off", help="Enable LLM extraction (on/off)"),
    rate_limit: Optional[float] = typer.Option(None, help="Requests per second"),
    respect_robots: bool = typer.Option(True, help="Respect robots.txt"),
    force: bool = typer.Option(False, help="Force re-scraping"),
) -> None:
    """Crawl a directory and extract structured records."""
    console.print(f"[bold blue]Starting crawl of {start_url}[/bold blue]")
    
    # Load configuration
    config = ScraperConfig(respect_robots=respect_robots)
    if rate_limit:
        config.rate_limit = rate_limit
    
    # Load schema
    try:
        record_schema = load_schema(schema)
        console.print(f"[green]Loaded schema with {len(record_schema.fields)} fields[/green]")
    except Exception as e:
        console.print(f"[red]Error loading schema: {e}[/red]")
        raise typer.Exit(1)
    
    # Load site hints if provided
    site_hints_obj = None
    if site_hints:
        from scraper.config import load_site_hints
        from scraper.utils import extract_domain
        
        try:
            domain = extract_domain(start_url)
            site_hints_obj = load_site_hints(site_hints, domain)
            if site_hints_obj:
                console.print(f"[green]Loaded site hints for {domain}[/green]")
        except Exception as e:
            console.print(f"[yellow]Warning: Could not load site hints: {e}[/yellow]")
    
    # Create pipeline
    pipeline = ScraperPipeline(config, record_schema, site_hints_obj)
    
    # Run pipeline
    use_llm = llm.lower() in ["on", "yes", "true", "1"]
    
    try:
        metadata = asyncio.run(
            pipeline.run(
                start_url=start_url,
                output_path=out,
                max_pages=max_pages,
                max_runtime=max_runtime,
                use_llm=use_llm,
                force=force,
            )
        )
        
        # Print summary
        console.print("\n[bold green]✓ Crawl completed[/bold green]")
        
        table = Table(title="Run Statistics")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="magenta")
        
        table.add_row("Run ID", metadata.run_id)
        table.add_row("Pages Fetched", str(metadata.pages_fetched))
        table.add_row("Pages Failed", str(metadata.pages_failed))
        table.add_row("Records Extracted", str(metadata.records_extracted))
        table.add_row("Records Valid", str(metadata.records_valid))
        table.add_row("Duration", f"{metadata.duration_ms / 1000:.1f}s")
        
        if use_llm:
            table.add_row("LLM Calls", str(metadata.llm_calls))
        
        console.print(table)
        
        if metadata.errors:
            console.print("\n[yellow]Errors encountered:[/yellow]")
            for error in metadata.errors:
                console.print(f"  - {error}")
        
        console.print(f"\n[bold]Output written to: {out}[/bold]")
        
    except Exception as e:
        console.print(f"[red]Error during crawl: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def evaluate(
    input: str = typer.Option(..., help="Input file path (.jsonl, .csv, or .parquet)"),
    report: str = typer.Option(..., help="Output report path (.md or .json)"),
    expected_min: Optional[int] = typer.Option(None, help="Expected minimum record count"),
    expected_max: Optional[int] = typer.Option(None, help="Expected maximum record count"),
    dupe_key: str = typer.Option("name,email", help="Comma-separated duplicate detection keys"),
    stanford_check: bool = typer.Option(False, help="Check Stanford profile count (~6297)"),
) -> None:
    """Evaluate scraped data quality and completeness."""
    console.print(f"[bold blue]Evaluating {input}[/bold blue]")
    
    # Load records
    try:
        if input.endswith(".jsonl"):
            records = asyncio.run(RecordReader.read_jsonl(input))
        elif input.endswith(".csv"):
            records = RecordReader.read_csv(input)
        elif input.endswith(".parquet"):
            records = RecordReader.read_parquet(input)
        else:
            console.print("[red]Unsupported file format. Use .jsonl, .csv, or .parquet[/red]")
            raise typer.Exit(1)
        
        console.print(f"[green]Loaded {len(records)} records[/green]")
    except Exception as e:
        console.print(f"[red]Error loading records: {e}[/red]")
        raise typer.Exit(1)
    
    # Evaluate
    dupe_keys = [k.strip() for k in dupe_key.split(",")]
    evaluator = evaluate_records(
        records,
        expected_min=expected_min,
        expected_max=expected_max,
        dupe_keys=dupe_keys,
    )
    
    # Stanford check
    if stanford_check:
        passed = check_stanford_profile_count(len(records))
        if passed:
            console.print("[green]✓ Stanford profile count check passed[/green]")
        else:
            console.print("[yellow]⚠ Stanford profile count outside expected range[/yellow]")
    
    # Print summary
    console.print("\n" + evaluator._generate_summary())
    
    # Write report
    try:
        if report.endswith(".md"):
            content = evaluator.to_markdown()
            Path(report).write_text(content, encoding="utf-8")
        else:
            report_data = evaluator.evaluate()
            Path(report).write_text(json.dumps(report_data, indent=2), encoding="utf-8")
        
        console.print(f"\n[bold]Report written to: {report}[/bold]")
    except Exception as e:
        console.print(f"[red]Error writing report: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def enrich(
    input: str = typer.Option(..., help="Input file path (.jsonl, .csv, or .parquet)"),
    out: str = typer.Option(..., help="Output file path"),
    sample: Optional[int] = typer.Option(None, help="Sample N records to enrich"),
    endpoint: Optional[str] = typer.Option(None, help="Sixtyfour API endpoint"),
    api_key: Optional[str] = typer.Option(None, help="Sixtyfour API key"),
    batch_size: int = typer.Option(25, help="Batch size for API requests"),
    use_async: bool = typer.Option(True, help="Use async endpoint (recommended, non-blocking)"),
) -> None:
    """Enrich records using Sixtyfour API.
    
    By default uses the async endpoint which submits jobs and polls for results.
    This is much faster for batch processing as jobs run in parallel.
    Use --no-use-async for the synchronous endpoint (blocks for 5-10 min per record).
    """
    console.print(f"[bold blue]Enriching records from {input}[/bold blue]")
    
    if use_async:
        console.print("[green]Using async endpoint (submit + poll)[/green]")
    else:
        console.print("[yellow]Using sync endpoint (blocking)[/yellow]")
    
    # Load records
    try:
        if input.endswith(".jsonl"):
            records = asyncio.run(RecordReader.read_jsonl(input))
        elif input.endswith(".csv"):
            records = RecordReader.read_csv(input)
        elif input.endswith(".parquet"):
            records = RecordReader.read_parquet(input)
        else:
            console.print("[red]Unsupported file format[/red]")
            raise typer.Exit(1)
        
        console.print(f"[green]Loaded {len(records)} records[/green]")
    except Exception as e:
        console.print(f"[red]Error loading records: {e}[/red]")
        raise typer.Exit(1)
    
    # Sample if requested
    if sample and sample < len(records):
        import random
        records = random.sample(records, sample)
        console.print(f"[yellow]Sampled {sample} records for enrichment[/yellow]")
    
    # Configure client
    config = ScraperConfig()
    if endpoint:
        config.sixtyfour_endpoint = endpoint
    if api_key:
        config.sixtyfour_api_key = api_key
    config.sixtyfour_batch_size = batch_size
    
    if not config.sixtyfour_api_key:
        console.print("[red]Sixtyfour API key not configured[/red]")
        console.print("Set SIXTYFOUR_API_KEY env var or use --api-key flag")
        raise typer.Exit(1)
    
    # Enrich
    async def run_enrichment():
        async with SixtyfourClient(config, use_async=use_async) as client:
            results = await client.enrich_batch(records)
            return results
    
    try:
        with console.status("[bold green]Enriching records..."):
            results = asyncio.run(run_enrichment())
        
        # Collect enriched records
        enriched_records = []
        success_count = 0
        enrichment_fields = set()
        
        for result in results:
            if result.enrichment_success:
                success_count += 1
                enrichment_fields.update(result.enrichment_fields)
                # Merge enriched data
                merged = {**result.original_data, **result.enriched_data}
                enriched_records.append(merged)
            else:
                enriched_records.append(result.original_data)
        
        # Write output
        if out.endswith(".jsonl"):
            asyncio.run(RecordWriter.write_jsonl(enriched_records, out))
        elif out.endswith(".csv"):
            RecordWriter.write_csv(enriched_records, out)
        elif out.endswith(".parquet"):
            RecordWriter.write_parquet(enriched_records, out)
        
        # Print summary
        console.print(f"\n[bold green]✓ Enrichment completed[/bold green]")
        console.print(f"Total records: {len(results)}")
        console.print(f"Successfully enriched: {success_count}")
        console.print(f"Success rate: {success_count / len(results) * 100:.1f}%")
        
        if enrichment_fields:
            console.print(f"\nEnriched fields: {', '.join(sorted(enrichment_fields))}")
        
        # Sample enriched data
        if success_count > 0:
            console.print("\n[bold]Sample enriched record:[/bold]")
            for result in results:
                if result.enrichment_success:
                    console.print(json.dumps(result.enriched_data, indent=2)[:500])
                    break
        
        console.print(f"\n[bold]Output written to: {out}[/bold]")
        
    except Exception as e:
        console.print(f"[red]Error during enrichment: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def version() -> None:
    """Show version information."""
    from scraper import __version__
    
    console.print(f"dirscrape version {__version__}")


if __name__ == "__main__":
    app()

