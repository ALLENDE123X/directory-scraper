"""Storage for scraped records and run history."""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiofiles
import aiosqlite
import orjson
import pandas as pd
import structlog

from scraper.models import ExtractedRecord, RunMetadata

logger = structlog.get_logger()


class RunHistory:
    """SQLite-based run history for idempotency and audit."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None
    
    async def __aenter__(self) -> "RunHistory":
        """Async context manager entry."""
        self.conn = await aiosqlite.connect(self.db_path)
        await self._init_schema()
        return self
    
    async def __aexit__(self, *args) -> None:
        """Async context manager exit."""
        if self.conn:
            await self.conn.close()
    
    async def _init_schema(self) -> None:
        """Initialize database schema."""
        if not self.conn:
            return
        
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                start_url TEXT NOT NULL,
                schema_json TEXT NOT NULL,
                pages_fetched INTEGER DEFAULT 0,
                pages_failed INTEGER DEFAULT 0,
                records_extracted INTEGER DEFAULT 0,
                records_valid INTEGER DEFAULT 0,
                records_invalid INTEGER DEFAULT 0,
                llm_calls INTEGER DEFAULT 0,
                llm_cost REAL DEFAULT 0.0,
                duration_ms INTEGER,
                params_json TEXT,
                errors_json TEXT
            )
        """)
        
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                kind TEXT NOT NULL,
                key TEXT NOT NULL,
                meta_json TEXT,
                FOREIGN KEY (run_id) REFERENCES runs(run_id)
            )
        """)
        
        await self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_run_id ON events(run_id)
        """)
        
        await self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_key ON events(key)
        """)
        
        await self.conn.commit()
    
    async def create_run(self, metadata: RunMetadata) -> None:
        """Create new run record."""
        if not self.conn:
            return
        
        await self.conn.execute(
            """
            INSERT INTO runs (
                run_id, started_at, start_url, schema_json, params_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                metadata.run_id,
                metadata.started_at.isoformat(),
                metadata.start_url,
                json.dumps(metadata.schema.to_json_types()),
                json.dumps(metadata.params),
            ),
        )
        await self.conn.commit()
    
    async def update_run(self, metadata: RunMetadata) -> None:
        """Update run with final stats."""
        if not self.conn:
            return
        
        await self.conn.execute(
            """
            UPDATE runs SET
                completed_at = ?,
                pages_fetched = ?,
                pages_failed = ?,
                records_extracted = ?,
                records_valid = ?,
                records_invalid = ?,
                llm_calls = ?,
                llm_cost = ?,
                duration_ms = ?,
                errors_json = ?
            WHERE run_id = ?
            """,
            (
                metadata.completed_at.isoformat() if metadata.completed_at else None,
                metadata.pages_fetched,
                metadata.pages_failed,
                metadata.records_extracted,
                metadata.records_valid,
                metadata.records_invalid,
                metadata.llm_calls,
                metadata.llm_cost,
                metadata.duration_ms,
                json.dumps(metadata.errors),
                metadata.run_id,
            ),
        )
        await self.conn.commit()
    
    async def log_event(
        self,
        run_id: str,
        kind: str,
        key: str,
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log an event (page fetch, extraction, etc.)."""
        if not self.conn:
            return
        
        await self.conn.execute(
            """
            INSERT INTO events (run_id, timestamp, kind, key, meta_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                run_id,
                datetime.utcnow().isoformat(),
                kind,
                key,
                json.dumps(meta) if meta else None,
            ),
        )
        await self.conn.commit()
    
    async def is_task_completed(self, run_id: str, task_id: str) -> bool:
        """Check if a task was already completed in this run."""
        if not self.conn:
            return False
        
        cursor = await self.conn.execute(
            "SELECT COUNT(*) FROM events WHERE run_id = ? AND key = ?",
            (run_id, task_id),
        )
        row = await cursor.fetchone()
        return (row[0] if row else 0) > 0


class RecordWriter:
    """Write extracted records to various formats."""
    
    @staticmethod
    async def write_jsonl(records: List[Dict[str, Any]], output_path: str) -> None:
        """Write records to JSONL file."""
        async with aiofiles.open(output_path, "wb") as f:
            for record in records:
                line = orjson.dumps(record) + b"\n"
                await f.write(line)
        
        logger.info("wrote_jsonl", path=output_path, count=len(records))
    
    @staticmethod
    async def append_jsonl(record: Dict[str, Any], output_path: str) -> None:
        """Append single record to JSONL file."""
        async with aiofiles.open(output_path, "ab") as f:
            line = orjson.dumps(record) + b"\n"
            await f.write(line)
    
    @staticmethod
    def write_csv(records: List[Dict[str, Any]], output_path: str) -> None:
        """Write records to CSV file."""
        if not records:
            return
        
        df = pd.DataFrame(records)
        df.to_csv(output_path, index=False, encoding="utf-8")
        
        logger.info("wrote_csv", path=output_path, count=len(records))
    
    @staticmethod
    def write_parquet(records: List[Dict[str, Any]], output_path: str) -> None:
        """Write records to Parquet file."""
        if not records:
            return
        
        df = pd.DataFrame(records)
        df.to_parquet(output_path, index=False, engine="pyarrow")
        
        logger.info("wrote_parquet", path=output_path, count=len(records))


class RecordReader:
    """Read records from various formats."""
    
    @staticmethod
    async def read_jsonl(input_path: str) -> List[Dict[str, Any]]:
        """Read records from JSONL file."""
        records = []
        async with aiofiles.open(input_path, "rb") as f:
            async for line in f:
                if line.strip():
                    record = orjson.loads(line)
                    records.append(record)
        
        logger.info("read_jsonl", path=input_path, count=len(records))
        return records
    
    @staticmethod
    def read_csv(input_path: str) -> List[Dict[str, Any]]:
        """Read records from CSV file."""
        df = pd.read_csv(input_path)
        records = df.to_dict("records")
        
        logger.info("read_csv", path=input_path, count=len(records))
        return records
    
    @staticmethod
    def read_parquet(input_path: str) -> List[Dict[str, Any]]:
        """Read records from Parquet file."""
        df = pd.read_parquet(input_path)
        records = df.to_dict("records")
        
        logger.info("read_parquet", path=input_path, count=len(records))
        return records


def deduplicate_records(
    records: List[Dict[str, Any]],
    key_fields: List[str],
) -> List[Dict[str, Any]]:
    """Deduplicate records based on key fields.
    
    Args:
        records: List of records
        key_fields: Fields to use for deduplication
        
    Returns:
        Deduplicated list
    """
    seen = set()
    unique_records = []
    
    for record in records:
        # Build key from specified fields
        key_parts = []
        for field in key_fields:
            value = record.get(field, "")
            if value:
                key_parts.append(str(value).lower().strip())
        
        if key_parts:
            key = "|".join(key_parts)
            if key not in seen:
                seen.add(key)
                unique_records.append(record)
        else:
            # If no key fields present, keep the record
            unique_records.append(record)
    
    logger.info(
        "deduplicated_records",
        original=len(records),
        unique=len(unique_records),
        duplicates=len(records) - len(unique_records),
    )
    
    return unique_records

