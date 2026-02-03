"""Import SQP data from CSV/Excel exports."""

import csv
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .models import SQPRecord, WeeklySnapshot
from .parsers import _normalize_columns, _parse_int, _parse_float


def import_csv(
    file_path: str | Path,
    asin: str,
    week_date: date | None = None,
) -> WeeklySnapshot:
    """Import SQP data from a CSV file.

    Handles Amazon SQP export format which has a metadata row at the top.

    Args:
        file_path: Path to CSV file
        asin: Parent ASIN for this data
        week_date: Week date (auto-detected from metadata/filename if not provided)

    Returns:
        WeeklySnapshot with parsed records
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    with open(file_path, "r", encoding="utf-8-sig") as f:
        lines = f.readlines()

    if not lines:
        raise ValueError(f"Empty CSV file: {file_path}")

    # Check if first line is Amazon metadata row
    first_line = lines[0].strip()
    metadata_asin = None
    metadata_date = None

    if first_line.startswith("ASIN") or "Reporting Range" in first_line:
        # Parse metadata row
        metadata_asin, metadata_date = _parse_amazon_metadata(first_line)
        # Remove metadata row, keep headers and data
        lines = lines[1:]

    # Use metadata values as fallbacks
    if week_date is None:
        week_date = metadata_date or _extract_date_from_filename(file_path.name)

    # Parse remaining lines as CSV
    from io import StringIO
    csv_content = StringIO("".join(lines))
    reader = csv.DictReader(csv_content)
    rows = list(reader)

    return _parse_rows(rows, asin, week_date)


def import_excel(
    file_path: str | Path,
    asin: str,
    week_date: date | None = None,
    sheet_name: str | int = 0,
) -> WeeklySnapshot:
    """Import SQP data from an Excel file.

    Requires openpyxl to be installed.

    Args:
        file_path: Path to Excel file (.xlsx)
        asin: Parent ASIN for this data
        week_date: Week date (auto-detected from filename if not provided)
        sheet_name: Sheet name or index to read

    Returns:
        WeeklySnapshot with parsed records
    """
    try:
        import openpyxl
    except ImportError:
        raise ImportError("openpyxl is required for Excel import. Run: pip install openpyxl")

    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"Excel file not found: {file_path}")

    if week_date is None:
        week_date = _extract_date_from_filename(file_path.name)

    wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)

    if isinstance(sheet_name, int):
        ws = wb.worksheets[sheet_name]
    else:
        ws = wb[sheet_name]

    # Read all rows
    rows_iter = ws.iter_rows(values_only=True)
    headers = [str(h).strip() if h else "" for h in next(rows_iter)]

    rows = []
    for row in rows_iter:
        row_dict = {}
        for i, value in enumerate(row):
            if i < len(headers) and headers[i]:
                row_dict[headers[i]] = value
        if any(row_dict.values()):
            rows.append(row_dict)

    wb.close()

    return _parse_rows(rows, asin, week_date)


def import_folder(
    folder_path: str | Path,
    asin: str,
) -> list[WeeklySnapshot]:
    """Import all CSV/Excel files from a folder.

    Expects files named with dates (e.g., "SQP_2025-01-15.csv").

    Args:
        folder_path: Path to folder containing export files
        asin: Parent ASIN for this data

    Returns:
        List of WeeklySnapshots, one per file
    """
    folder_path = Path(folder_path)

    if not folder_path.is_dir():
        raise NotADirectoryError(f"Not a directory: {folder_path}")

    snapshots = []

    # Find all CSV and Excel files
    files = list(folder_path.glob("*.csv")) + list(folder_path.glob("*.xlsx"))

    for file_path in sorted(files):
        try:
            if file_path.suffix.lower() == ".csv":
                snapshot = import_csv(file_path, asin)
            else:
                snapshot = import_excel(file_path, asin)

            if snapshot.records:
                snapshots.append(snapshot)
                print(f"  Imported {file_path.name}: {len(snapshot.records)} keywords")
        except Exception as e:
            print(f"  Warning: Failed to import {file_path.name}: {e}")

    return snapshots


def _parse_rows(
    rows: list[dict[str, Any]],
    asin: str,
    week_date: date,
) -> WeeklySnapshot:
    """Parse rows into WeeklySnapshot."""
    snapshot = WeeklySnapshot(asin=asin, week_date=week_date)

    for row in rows:
        normalized = _normalize_columns(row)

        search_query = normalized.get("search_query", "")
        if not search_query:
            continue

        record = SQPRecord(
            search_query=str(search_query).strip(),
            asin=asin,
            week_date=week_date,
            search_volume=_parse_int(normalized.get("search_volume", 0)),
            search_score=_parse_float(normalized.get("search_score", 0)) or 0.0,
            impressions_total=_parse_int(normalized.get("impressions_total", 0)),
            impressions_asin=_parse_int(normalized.get("impressions_asin", 0)),
            impressions_share=_parse_float(normalized.get("impressions_share", 0)) or 0.0,
            clicks_total=_parse_int(normalized.get("clicks_total", 0)),
            clicks_asin=_parse_int(normalized.get("clicks_asin", 0)),
            clicks_share=_parse_float(normalized.get("clicks_share", 0)) or 0.0,
            purchases_total=_parse_int(normalized.get("purchases_total", 0)),
            purchases_asin=_parse_int(normalized.get("purchases_asin", 0)),
            purchases_share=_parse_float(normalized.get("purchases_share", 0)) or 0.0,
            asin_price=_parse_float(normalized.get("asin_price")),
            market_price=_parse_float(normalized.get("market_price")),
        )

        snapshot.records.append(record)

    return snapshot


def _parse_amazon_metadata(line: str) -> tuple[str | None, date | None]:
    """Parse Amazon SQP export metadata row.

    Example line:
    ASIN or Product=["B08MWWJ3HG"],Reporting Range=["Weekly"],Select week=["Week 5 | 2026-01-25 - 2026-01-31 2026"]

    Returns:
        Tuple of (asin, week_date) - either may be None if not found
    """
    asin = None
    week_date = None

    # Extract ASIN
    asin_match = re.search(r'ASIN[^=]*=\["?([A-Z0-9]{10})"?\]', line)
    if asin_match:
        asin = asin_match.group(1)

    # Extract date from "Select week" or similar
    # Format: "Week 5 | 2026-01-25 - 2026-01-31 2026"
    date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})\s*-\s*(\d{4})-(\d{2})-(\d{2})', line)
    if date_match:
        # Use start date of the week
        week_date = date(
            int(date_match.group(1)),
            int(date_match.group(2)),
            int(date_match.group(3))
        )

    return asin, week_date


def _extract_date_from_filename(filename: str) -> date:
    """Try to extract a date from filename.

    Supports formats:
    - SQP_2025-01-15.csv
    - Week_2026_01_31.csv (underscore separated)
    - SQP-2025-05.csv (year-week)
    - report_20250115.csv
    - 2025-01-15_SQP.xlsx
    """
    # Try YYYY-MM-DD format (with dashes)
    match = re.search(r"(\d{4})-(\d{2})-(\d{2})", filename)
    if match:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))

    # Try YYYY_MM_DD format (with underscores)
    match = re.search(r"(\d{4})_(\d{2})_(\d{2})", filename)
    if match:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))

    # Try YYYYMMDD format
    match = re.search(r"(\d{4})(\d{2})(\d{2})", filename)
    if match:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))

    # Try YYYY-WW (year-week) format
    match = re.search(r"(\d{4})-(\d{2})", filename)
    if match:
        year = int(match.group(1))
        week = int(match.group(2))
        # Convert year-week to date (Monday of that week)
        return date.fromisocalendar(year, week, 1)

    # Default to today
    return date.today()
