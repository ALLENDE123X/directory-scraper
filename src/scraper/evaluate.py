"""Evaluation metrics for scraped data."""

import re
from typing import Any, Dict, List, Optional

import structlog

from scraper.utils import validate_email

logger = structlog.get_logger()


class EvaluationReport:
    """Evaluation report for scraped data."""
    
    def __init__(self, records: List[Dict[str, Any]], dupe_keys: Optional[List[str]] = None):
        self.records = records
        self.dupe_keys = dupe_keys or ["name", "email"]
        self.total_count = len(records)
        self.unique_count = 0
        self.duplicate_count = 0
        self.field_completeness: Dict[str, float] = {}
        self.field_validity: Dict[str, float] = {}
        self.warnings: List[str] = []
    
    def evaluate(self) -> Dict[str, Any]:
        """Run evaluation and return report."""
        self._evaluate_duplicates()
        self._evaluate_completeness()
        self._evaluate_validity()
        self._check_thresholds()
        
        report = {
            "total_records": self.total_count,
            "unique_records": self.unique_count,
            "duplicates": self.duplicate_count,
            "duplicate_rate": self.duplicate_count / self.total_count if self.total_count > 0 else 0,
            "field_completeness": self.field_completeness,
            "field_validity": self.field_validity,
            "warnings": self.warnings,
            "summary": self._generate_summary(),
        }
        
        return report
    
    def _evaluate_duplicates(self) -> None:
        """Evaluate duplicate records."""
        seen = set()
        duplicates = 0
        
        for record in self.records:
            key_parts = []
            for field in self.dupe_keys:
                value = record.get(field, "")
                if value:
                    key_parts.append(str(value).lower().strip())
            
            if key_parts:
                key = "|".join(key_parts)
                if key in seen:
                    duplicates += 1
                else:
                    seen.add(key)
        
        self.unique_count = len(seen)
        self.duplicate_count = duplicates
        
        if duplicates > 0:
            dup_rate = duplicates / self.total_count * 100
            self.warnings.append(
                f"Found {duplicates} duplicates ({dup_rate:.1f}%) based on {self.dupe_keys}"
            )
    
    def _evaluate_completeness(self) -> None:
        """Evaluate field completeness (non-empty rate)."""
        if not self.records:
            return
        
        # Get all fields
        all_fields = set()
        for record in self.records:
            all_fields.update(record.keys())
        
        # Calculate completeness per field
        for field in all_fields:
            non_empty = 0
            for record in self.records:
                value = record.get(field, "")
                if value and str(value).strip():
                    non_empty += 1
            
            completeness = non_empty / self.total_count * 100
            self.field_completeness[field] = completeness
            
            # Warn on key fields with low completeness
            if field in ["name", "page_url"] and completeness < 90:
                self.warnings.append(
                    f"Low completeness for '{field}': {completeness:.1f}%"
                )
    
    def _evaluate_validity(self) -> None:
        """Evaluate field validity (format correctness)."""
        if not self.records:
            return
        
        # Email validity
        if "email" in self.field_completeness:
            valid_emails = 0
            total_emails = 0
            
            for record in self.records:
                email = record.get("email", "")
                if email and str(email).strip():
                    total_emails += 1
                    if validate_email(str(email)):
                        valid_emails += 1
            
            if total_emails > 0:
                validity = valid_emails / total_emails * 100
                self.field_validity["email"] = validity
                
                if validity < 95:
                    self.warnings.append(
                        f"Email validity: {validity:.1f}% ({valid_emails}/{total_emails})"
                    )
        
        # URL validity
        for field in ["page_url", "linkedin_url"]:
            if field in self.field_completeness:
                valid_urls = 0
                total_urls = 0
                
                for record in self.records:
                    url = record.get(field, "")
                    if url and str(url).strip():
                        total_urls += 1
                        if re.match(r'^https?://', str(url)):
                            valid_urls += 1
                
                if total_urls > 0:
                    validity = valid_urls / total_urls * 100
                    self.field_validity[field] = validity
    
    def _check_thresholds(self) -> None:
        """Check against expected thresholds."""
        # Check for suspiciously low record counts
        if self.total_count < 10:
            self.warnings.append(
                f"Very low record count: {self.total_count} (expected more for directory scraping)"
            )
    
    def _generate_summary(self) -> str:
        """Generate human-readable summary."""
        lines = []
        lines.append(f"Total Records: {self.total_count}")
        lines.append(f"Unique Records: {self.unique_count}")
        
        if self.duplicate_count > 0:
            dup_rate = self.duplicate_count / self.total_count * 100
            lines.append(f"Duplicates: {self.duplicate_count} ({dup_rate:.1f}%)")
        
        lines.append("\nField Completeness:")
        for field, pct in sorted(self.field_completeness.items(), key=lambda x: -x[1]):
            lines.append(f"  {field}: {pct:.1f}%")
        
        if self.field_validity:
            lines.append("\nField Validity:")
            for field, pct in sorted(self.field_validity.items()):
                lines.append(f"  {field}: {pct:.1f}%")
        
        if self.warnings:
            lines.append("\nWarnings:")
            for warning in self.warnings:
                lines.append(f"  ⚠ {warning}")
        
        return "\n".join(lines)
    
    def to_markdown(self) -> str:
        """Generate markdown report."""
        lines = []
        lines.append("# Scraper Evaluation Report")
        lines.append("")
        lines.append("## Overview")
        lines.append("")
        lines.append(f"- **Total Records**: {self.total_count}")
        lines.append(f"- **Unique Records**: {self.unique_count}")
        lines.append(f"- **Duplicates**: {self.duplicate_count}")
        
        if self.total_count > 0:
            dup_rate = self.duplicate_count / self.total_count * 100
            lines.append(f"- **Duplicate Rate**: {dup_rate:.2f}%")
        
        lines.append("")
        lines.append("## Field Completeness")
        lines.append("")
        lines.append("| Field | Completeness |")
        lines.append("|-------|--------------|")
        
        for field, pct in sorted(self.field_completeness.items(), key=lambda x: -x[1]):
            status = "✅" if pct >= 90 else "⚠️" if pct >= 50 else "❌"
            lines.append(f"| {field} | {pct:.1f}% {status} |")
        
        if self.field_validity:
            lines.append("")
            lines.append("## Field Validity")
            lines.append("")
            lines.append("| Field | Validity |")
            lines.append("|-------|----------|")
            
            for field, pct in sorted(self.field_validity.items()):
                status = "✅" if pct >= 95 else "⚠️" if pct >= 80 else "❌"
                lines.append(f"| {field} | {pct:.1f}% {status} |")
        
        if self.warnings:
            lines.append("")
            lines.append("## Warnings")
            lines.append("")
            for warning in self.warnings:
                lines.append(f"- ⚠️ {warning}")
        
        return "\n".join(lines)


def evaluate_records(
    records: List[Dict[str, Any]],
    expected_min: Optional[int] = None,
    expected_max: Optional[int] = None,
    dupe_keys: Optional[List[str]] = None,
) -> EvaluationReport:
    """Evaluate scraped records and generate report.
    
    Args:
        records: List of scraped records
        expected_min: Minimum expected record count (warning if below)
        expected_max: Maximum expected record count (warning if above)
        dupe_keys: Fields to use for duplicate detection
        
    Returns:
        Evaluation report
    """
    evaluator = EvaluationReport(records, dupe_keys)
    report_data = evaluator.evaluate()
    
    # Check count thresholds
    if expected_min and len(records) < expected_min:
        tolerance = abs(len(records) - expected_min)
        pct = tolerance / expected_min * 100
        evaluator.warnings.append(
            f"Record count {len(records)} is below expected minimum {expected_min} "
            f"(difference: {tolerance}, {pct:.1f}%)"
        )
    
    if expected_max and len(records) > expected_max:
        tolerance = abs(len(records) - expected_max)
        pct = tolerance / expected_max * 100
        evaluator.warnings.append(
            f"Record count {len(records)} is above expected maximum {expected_max} "
            f"(difference: {tolerance}, {pct:.1f}%)"
        )
    
    return evaluator


def check_stanford_profile_count(count: int, tolerance_pct: float = 5.0) -> bool:
    """Check if Stanford profile count is within expected range.
    
    Args:
        count: Actual profile count
        tolerance_pct: Tolerance percentage (default 5%)
        
    Returns:
        True if within tolerance, False otherwise
    """
    expected = 6297
    tolerance = int(expected * tolerance_pct / 100)
    min_expected = expected - tolerance
    max_expected = expected + tolerance
    
    in_range = min_expected <= count <= max_expected
    
    if in_range:
        logger.info(
            "stanford_count_check_passed",
            count=count,
            expected=expected,
            tolerance=tolerance,
        )
    else:
        logger.warning(
            "stanford_count_check_failed",
            count=count,
            expected=expected,
            min=min_expected,
            max=max_expected,
        )
    
    return in_range

