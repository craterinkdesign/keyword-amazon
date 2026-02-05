#!/usr/bin/env python3
"""Quarterly keyword tracker for Amazon SQP analysis.

Tracks top 10 keywords per ASIN in a single consolidated tab per quarter.
Reads active ASINs from the master sheet automatically.
Detects keyword placement in title/backend and alerts on changes.

Usage:
    # Start new quarter (all active ASINs)
    python -m sqp_analyzer.commands.quarterly_tracker --start

    # Weekly update (all active ASINs)
    python -m sqp_analyzer.commands.quarterly_tracker --update
"""

import argparse
import gzip
import json
import sys
import time
from datetime import date, timedelta
from typing import Any

import requests
from sp_api.api import Reports
from sp_api.base import Marketplaces

from ..config import load_config, AppConfig, Thresholds
from ..models import (
    SQPRecord,
    WeeklySnapshot,
    ListingContent,
    QuarterlyKeyword,
    RankStatus,
    DiagnosticType,
)
from ..sheets.client import SheetsClient
from .fetch_listing import get_listing_content


# Constants
TOP_KEYWORDS_COUNT = 10
METRICS_PER_WEEK = 6  # Vol, Imp%, Clk%, Pur%, Opp, Rank
STATIC_COLS = 5  # ASIN, Rank, Keyword, In Title, In Backend
VOLUME_DROP_THRESHOLD = 0.30
DASHBOARD_TAB_NAME = "Dashboard"
RANK_SEVERITY = {"top_3": 3, "page_1_high": 2, "page_1_low": 1, "invisible": 0}


def get_credentials() -> dict:
    """Load SP-API credentials from environment."""
    config = load_config()
    return {
        "refresh_token": config.sp_api.refresh_token,
        "lwa_app_id": config.sp_api.client_id,
        "lwa_client_secret": config.sp_api.client_secret,
    }


def get_current_quarter() -> tuple[int, int]:
    """Get current quarter (Q1-Q4) and year.

    Returns:
        Tuple of (quarter_number, year)
    """
    today = date.today()
    quarter = (today.month - 1) // 3 + 1
    return quarter, today.year


def get_week_in_quarter() -> int:
    """Get week number within the current quarter (1-13).

    Returns:
        Week number (1-13)
    """
    today = date.today()
    quarter = (today.month - 1) // 3 + 1

    # First day of quarter
    quarter_start_month = (quarter - 1) * 3 + 1
    quarter_start = date(today.year, quarter_start_month, 1)

    # Calculate week number
    days_since_start = (today - quarter_start).days
    week_num = days_since_start // 7 + 1
    return min(week_num, 13)


def get_consolidated_tab_name(quarter: int | None = None) -> str:
    """Generate consolidated tab name for quarterly tracker.

    Args:
        quarter: Quarter number (1-4), defaults to current

    Returns:
        Tab name like 'Q1'
    """
    if quarter is None:
        quarter, _ = get_current_quarter()

    return f"Q{quarter}"


def get_last_complete_week() -> tuple[date, date]:
    """Get the last complete week (Sunday to Saturday)."""
    today = date.today()
    days_since_saturday = (today.weekday() + 2) % 7
    if days_since_saturday == 0:
        days_since_saturday = 7
    last_saturday = today - timedelta(days=days_since_saturday)
    last_sunday = last_saturday - timedelta(days=6)
    return last_sunday, last_saturday


def get_quarter_weeks() -> list[tuple[int, date, date]]:
    """Get all complete weeks in the current quarter up to now.

    Returns:
        List of (week_num, start_date, end_date) tuples
    """
    today = date.today()
    quarter = (today.month - 1) // 3 + 1
    year = today.year

    # First day of quarter
    quarter_start_month = (quarter - 1) * 3 + 1
    quarter_start = date(year, quarter_start_month, 1)

    # Find first Sunday of the quarter (or before if quarter starts mid-week)
    days_to_sunday = (6 - quarter_start.weekday()) % 7
    if days_to_sunday == 0 and quarter_start.weekday() != 6:
        days_to_sunday = 7
    first_sunday = quarter_start + timedelta(days=days_to_sunday)

    # If quarter starts after Sunday, use the Sunday before
    if first_sunday > quarter_start + timedelta(days=6):
        first_sunday = first_sunday - timedelta(days=7)

    # Get last complete Saturday
    days_since_saturday = (today.weekday() + 2) % 7
    if days_since_saturday == 0:
        days_since_saturday = 7
    last_saturday = today - timedelta(days=days_since_saturday)

    weeks = []
    week_num = 1
    current_sunday = first_sunday

    while current_sunday + timedelta(days=6) <= last_saturday:
        week_end = current_sunday + timedelta(days=6)
        weeks.append((week_num, current_sunday, week_end))
        current_sunday = current_sunday + timedelta(days=7)
        week_num += 1
        if week_num > 13:
            break

    return weeks


def fetch_sqp_report(
    credentials: dict, asin: str, start_date: date, end_date: date
) -> dict | None:
    """Request and wait for SQP report.

    Args:
        credentials: SP-API credentials
        asin: ASIN to fetch data for
        start_date: Start date (must be Sunday)
        end_date: End date

    Returns:
        Report data dict or None if failed
    """
    report = Reports(credentials=credentials, marketplace=Marketplaces.US)

    print(f"  Requesting SQP report for {asin}...")
    print(f"  Period: {start_date} to {end_date}")

    # Create report request
    res = report.create_report(
        reportType="GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT",
        marketplaceIds=["ATVPDKIKX0DER"],
        reportOptions={
            "reportPeriod": "WEEK",
            "asin": asin,
        },
        dataStartTime=f"{start_date}T00:00:00Z",
        dataEndTime=f"{end_date}T23:59:59Z",
    )

    report_id = res.payload.get("reportId")
    print(f"  Report ID: {report_id}")
    print("  Waiting for report to complete...")

    # Wait for completion (max 60 minutes)
    max_wait = 3600
    start_time = time.time()
    check_interval = 30

    while time.time() - start_time < max_wait:
        res = report.get_report(reportId=report_id)
        status = res.payload.get("processingStatus")
        doc_id = res.payload.get("reportDocumentId")

        if status == "DONE" and doc_id:
            # Download report
            doc_res = report.get_report_document(
                reportDocumentId=doc_id, download=False
            )
            url = doc_res.payload.get("url")

            response = requests.get(url)
            if doc_res.payload.get("compressionAlgorithm") == "GZIP":
                data = gzip.decompress(response.content).decode("utf-8")
            else:
                data = response.text

            return json.loads(data)

        elif status == "FATAL":
            if doc_id:
                doc_res = report.get_report_document(
                    reportDocumentId=doc_id, download=False
                )
                url = doc_res.payload.get("url")
                response = requests.get(url)
                data = gzip.decompress(response.content).decode("utf-8")
                error_data = json.loads(data)
                print(f"  [ERROR] {error_data.get('errorDetails', 'Unknown error')}")
            return None

        elif status == "CANCELLED":
            print("  [CANCELLED] Report was cancelled")
            return None

        elapsed = int(time.time() - start_time)
        print(f"  [{elapsed // 60}m {elapsed % 60}s] Status: {status}")
        time.sleep(check_interval)

    print(f"  [TIMEOUT] Report did not complete within {max_wait // 60} minutes")
    return None


def parse_report_to_snapshot(report_data: dict) -> WeeklySnapshot | None:
    """Parse report data into WeeklySnapshot with SQPRecords."""
    if "errorDetails" in report_data:
        print(f"  Report error: {report_data['errorDetails']}")
        return None

    spec = report_data.get("reportSpecification", {})
    start_date_str = spec.get("dataStartTime", "")[:10]

    try:
        week_date = date.fromisoformat(start_date_str)
    except ValueError:
        week_date = date.today()

    entries = report_data.get("dataByAsin", [])
    if not entries:
        print("  No data in report")
        return None

    first_asin = entries[0].get("asin", "UNKNOWN")

    records = []
    for entry in entries:
        asin = entry.get("asin", first_asin)
        sq = entry.get("searchQueryData", {})
        imp = entry.get("impressionData", {})
        clk = entry.get("clickData", {})
        pur = entry.get("purchaseData", {})

        record = SQPRecord(
            search_query=sq.get("searchQuery", "") or "",
            asin=asin,
            week_date=week_date,
            search_volume=sq.get("searchQueryVolume", 0) or 0,
            search_score=sq.get("searchQueryScore", 0) or 0,
            impressions_total=imp.get("totalImpressions", 0) or 0,
            impressions_asin=imp.get("asinImpressions", 0) or 0,
            impressions_share=imp.get("asinImpressionShare", 0) or 0,
            clicks_total=clk.get("totalClicks", 0) or 0,
            clicks_asin=clk.get("asinClicks", 0) or 0,
            clicks_share=clk.get("asinClickShare", 0) or 0,
            purchases_total=pur.get("totalPurchases", 0) or 0,
            purchases_asin=pur.get("asinPurchases", 0) or 0,
            purchases_share=pur.get("asinPurchaseShare", 0) or 0,
        )
        records.append(record)

    return WeeklySnapshot(
        asin=first_asin,
        week_date=week_date,
        records=records,
    )


def get_rank_status(imp_share: float, thresholds: Thresholds) -> RankStatus:
    """Determine rank status from impression share."""
    if imp_share >= thresholds.rank_top_3_threshold:
        return RankStatus.TOP_3
    elif imp_share >= thresholds.rank_page_1_high_threshold:
        return RankStatus.PAGE_1_HIGH
    elif imp_share >= thresholds.rank_page_1_low_threshold:
        return RankStatus.PAGE_1_LOW
    else:
        return RankStatus.INVISIBLE


def get_diagnostic_type(record: SQPRecord, thresholds: Thresholds) -> DiagnosticType:
    """Determine diagnostic type for a keyword."""
    # Ghost: High volume, no impressions
    if (
        record.search_volume >= thresholds.ghost_min_volume
        and record.impressions_share < thresholds.ghost_max_imp_share
    ):
        return DiagnosticType.GHOST

    # Window Shopper: Seen but not clicked
    if (
        record.impressions_share >= thresholds.window_shopper_min_imp_share
        and record.clicks_share < thresholds.window_shopper_max_click_share
    ):
        return DiagnosticType.WINDOW_SHOPPER

    # Price Problem: Clicked but not bought
    if (
        record.impressions_share >= thresholds.price_problem_min_imp_share
        and record.clicks_share > 0
        and record.purchases_share == 0
    ):
        return DiagnosticType.PRICE_PROBLEM

    return DiagnosticType.HEALTHY


def calculate_opportunity_score(record: SQPRecord, diagnostic: DiagnosticType) -> float:
    """Calculate opportunity score for a keyword.

    Higher score = more opportunity for improvement.
    """
    # Base score from volume
    volume_score = min(record.search_volume / 10000, 1.0) * 40

    # Multiplier based on diagnostic type
    diagnostic_multiplier = {
        DiagnosticType.GHOST: 2.0,  # High priority - not ranking at all
        DiagnosticType.WINDOW_SHOPPER: 1.5,  # Medium priority - not converting views
        DiagnosticType.PRICE_PROBLEM: 1.3,  # Lower priority - may need pricing changes
        DiagnosticType.HEALTHY: 0.5,  # Low priority - already performing
    }

    multiplier = diagnostic_multiplier.get(diagnostic, 1.0)

    # Penalty for already having high market share
    share_penalty = record.purchases_share * 0.5

    score = (volume_score * multiplier) - share_penalty
    return max(0, min(100, round(score, 1)))


def get_top_keywords(
    snapshot: WeeklySnapshot, count: int = TOP_KEYWORDS_COUNT
) -> list[SQPRecord]:
    """Get top keywords by volume that have purchases.

    Args:
        snapshot: Weekly snapshot with all records
        count: Number of top keywords to return

    Returns:
        List of top SQPRecords sorted by search volume
    """
    # Filter to keywords with at least some purchase share
    with_purchases = [r for r in snapshot.records if r.purchases_share > 0]

    # Sort by volume (descending)
    sorted_records = sorted(with_purchases, key=lambda r: r.search_volume, reverse=True)

    return sorted_records[:count]


def build_headers(weeks: list[str]) -> list[str]:
    """Build header row for quarterly tracker.

    Args:
        weeks: List of week labels (e.g., ['W01', 'W02'])

    Returns:
        Complete header row
    """
    headers = ["ASIN", "Rank", "Keyword", "In Title", "In Backend"]

    for week in weeks:
        headers.extend(
            [
                f"{week} Vol",
                f"{week} Imp%",
                f"{week} Clk%",
                f"{week} Pur%",
                f"{week} Opp",
                f"{week} Rank",
            ]
        )

    headers.append("Alert")
    return headers


def build_asin_separator_row(asin: str, product_name: str, num_cols: int) -> list:
    """Build a separator row for an ASIN group.

    Format: [asin, "", product_name, "", "", ...]
    Detectable by: col 0 has value, col 1 (Rank) is empty.
    """
    row = [""] * num_cols
    row[0] = asin
    row[2] = product_name
    return row


def is_asin_separator_row(row: list) -> bool:
    """Check if a row is an ASIN separator row.

    Separator rows have ASIN in col 0 and empty Rank in col 1.
    """
    if len(row) < 2:
        return False
    return bool(row[0]) and not row[1]


def parse_consolidated_sheet(
    all_values: list[list],
) -> dict[str, dict[str, Any]]:
    """Parse a consolidated quarterly tracker sheet into per-ASIN data.

    Args:
        all_values: All rows from the sheet (including header row)

    Returns:
        Dict mapping ASIN -> {"name": str, "keywords": list[dict], "raw_rows": list[list]}
        Each keyword dict has: rank, keyword, in_title, in_backend, row_data (full row)
    """
    if len(all_values) < 2:
        return {}

    headers = all_values[0]
    data_rows = all_values[1:]

    result: dict[str, dict[str, Any]] = {}
    current_asin = None

    for row in data_rows:
        # Pad row to header length
        padded = row + [""] * (len(headers) - len(row))

        if is_asin_separator_row(padded):
            current_asin = padded[0]
            result[current_asin] = {
                "name": padded[2],
                "keywords": [],
                "raw_rows": [padded],  # Include separator
            }
        elif current_asin and current_asin in result:
            # Data row: col 0 = ASIN, col 1 = Rank, col 2 = Keyword,
            # col 3 = In Title, col 4 = In Backend
            result[current_asin]["keywords"].append(
                {
                    "rank": padded[1],
                    "keyword": padded[2],
                    "in_title": padded[3] == "YES",
                    "in_backend": padded[4] == "YES",
                    "row_data": padded,
                }
            )
            result[current_asin]["raw_rows"].append(padded)

    return result


def extract_week_metrics(row_data: list, week_index: int) -> dict[str, Any] | None:
    """Extract 6 metric values for a given week from a raw row.

    Args:
        row_data: Full row from the consolidated sheet
        week_index: 0-based week index (0 = first week)

    Returns:
        Dict with volume, imp_share, click_share, purchase_share, opportunity_score,
        rank_status — or None if data is missing/empty.
    """
    start = STATIC_COLS + week_index * METRICS_PER_WEEK
    end = start + METRICS_PER_WEEK

    if end > len(row_data):
        return None

    raw = row_data[start:end]

    # Skip if all values are empty or dashes
    if all(str(v).strip() in ("", "-") for v in raw):
        return None

    def to_float(val: Any) -> float | None:
        s = str(val).strip()
        if s in ("", "-"):
            return None
        try:
            return float(s)
        except ValueError:
            return None

    volume = to_float(raw[0])
    if volume is None:
        return None

    return {
        "volume": volume,
        "imp_share": to_float(raw[1]),
        "click_share": to_float(raw[2]),
        "purchase_share": to_float(raw[3]),
        "opportunity_score": to_float(raw[4]),
        "rank_status": str(raw[5]).strip() if raw[5] else None,
    }


def detect_drastic_changes(
    asin: str,
    asin_data: dict[str, Any],
    num_weeks: int,
) -> list[dict[str, Any]]:
    """Scan all keywords for one ASIN, return flagged entries with reasons.

    Compares the last two weeks for volume drops and rank downgrades,
    and checks the Alert column for placement drops.

    Args:
        asin: The ASIN identifier
        asin_data: Parsed ASIN data from parse_consolidated_sheet
        num_weeks: Number of weeks in the tracker

    Returns:
        List of dicts with: keyword, rank, reasons (list[str]),
        curr_vol, curr_rank, prev_vol, prev_rank
    """
    flagged = []

    for kw_info in asin_data["keywords"]:
        row = kw_info["row_data"]
        reasons: list[str] = []

        curr_metrics = None
        prev_metrics = None

        if num_weeks >= 2:
            curr_metrics = extract_week_metrics(row, num_weeks - 1)
            prev_metrics = extract_week_metrics(row, num_weeks - 2)

        curr_vol = curr_metrics["volume"] if curr_metrics else None
        prev_vol = prev_metrics["volume"] if prev_metrics else None
        curr_rank = curr_metrics["rank_status"] if curr_metrics else None
        prev_rank = prev_metrics["rank_status"] if prev_metrics else None

        # Check volume drop
        if curr_vol is not None and prev_vol is not None and prev_vol > 0:
            drop_pct = (prev_vol - curr_vol) / prev_vol
            if drop_pct >= VOLUME_DROP_THRESHOLD:
                pct_display = round(drop_pct * 100)
                reasons.append(
                    f"Volume -{pct_display}% ({int(prev_vol)} -> {int(curr_vol)})"
                )

        # Check rank downgrade
        if (
            curr_rank
            and prev_rank
            and curr_rank in RANK_SEVERITY
            and prev_rank in RANK_SEVERITY
        ):
            if RANK_SEVERITY[curr_rank] < RANK_SEVERITY[prev_rank]:
                reasons.append(f"Rank: {prev_rank} -> {curr_rank}")

        # Check placement drops from Alert column (last column)
        alert_val = str(row[-1]).strip() if row else ""
        if "DROPPED FROM TITLE" in alert_val:
            reasons.append("DROPPED FROM TITLE")
        if "DROPPED FROM BACKEND" in alert_val:
            reasons.append("DROPPED FROM BACKEND")

        if reasons:
            flagged.append(
                {
                    "keyword": kw_info["keyword"],
                    "rank": kw_info["rank"],
                    "reasons": reasons,
                    "curr_vol": curr_vol,
                    "curr_rank": curr_rank,
                    "prev_vol": prev_vol,
                    "prev_rank": prev_rank,
                }
            )

    return flagged


def build_asin_summary(
    asin: str,
    asin_data: dict[str, Any],
    num_weeks: int,
    flagged_keywords: list[dict[str, Any]],
) -> list[Any]:
    """Produce one summary row for the ASIN summary section.

    Counts keywords by rank status (using most recent week), picks top alert,
    and assigns a health label.

    Args:
        asin: The ASIN identifier
        asin_data: Parsed ASIN data
        num_weeks: Number of weeks in the tracker
        flagged_keywords: List of flagged keyword dicts from detect_drastic_changes

    Returns:
        Row: [ASIN, Product Name, # Top 3, # Page 1 High, # Page 1 Low,
              # Invisible, Top Alert, Health]
    """
    counts = {"top_3": 0, "page_1_high": 0, "page_1_low": 0, "invisible": 0}
    total_kw = len(asin_data["keywords"])

    for kw_info in asin_data["keywords"]:
        row = kw_info["row_data"]
        if num_weeks >= 1:
            metrics = extract_week_metrics(row, num_weeks - 1)
            if metrics and metrics["rank_status"] in counts:
                counts[metrics["rank_status"]] += 1
            else:
                counts["invisible"] += 1
        else:
            counts["invisible"] += 1

    # Pick top alert (first reason of first flagged keyword)
    top_alert = ""
    if flagged_keywords:
        top_alert = flagged_keywords[0]["reasons"][0]

    # Determine health label
    if flagged_keywords:
        health = "AT RISK"
    elif total_kw > 0 and (counts["invisible"] / total_kw) > 0.5:
        health = "WEAK"
    elif total_kw > 0 and ((counts["top_3"] + counts["page_1_high"]) / total_kw) > 0.5:
        health = "STRONG"
    else:
        health = "OK"

    return [
        asin,
        asin_data["name"],
        counts["top_3"],
        counts["page_1_high"],
        counts["page_1_low"],
        counts["invisible"],
        top_alert,
        health,
    ]


def build_dashboard(
    quarter_data: dict[str, dict[str, Any]],
    num_weeks: int,
) -> tuple[list[list[str]], list[list[Any]], list[list[Any]]]:
    """Orchestrator: build both dashboard sections.

    Args:
        quarter_data: Parsed data from parse_consolidated_sheet
        num_weeks: Number of weeks in the tracker

    Returns:
        Tuple of (summary_headers, summary_rows, flagged_headers_and_rows)
        where flagged_headers_and_rows includes the header row followed by data rows.
    """
    summary_headers = [
        "ASIN",
        "Product Name",
        "# Top 3",
        "# Page 1 High",
        "# Page 1 Low",
        "# Invisible",
        "Top Alert",
        "Health",
    ]
    flagged_headers = [
        "ASIN",
        "Keyword",
        "Rank",
        "Reasons",
        "Curr Vol",
        "Curr Rank",
        "Prev Vol",
        "Prev Rank",
    ]

    summary_rows: list[list[Any]] = []
    flagged_rows: list[list[Any]] = []

    for asin, asin_data in quarter_data.items():
        flagged = detect_drastic_changes(asin, asin_data, num_weeks)
        summary_row = build_asin_summary(asin, asin_data, num_weeks, flagged)
        summary_rows.append(summary_row)

        for f in flagged:
            flagged_rows.append(
                [
                    asin,
                    f["keyword"],
                    f["rank"],
                    " | ".join(f["reasons"]),
                    f["curr_vol"] if f["curr_vol"] is not None else "",
                    f["curr_rank"] or "",
                    f["prev_vol"] if f["prev_vol"] is not None else "",
                    f["prev_rank"] or "",
                ]
            )

    return summary_headers, summary_rows, [flagged_headers] + flagged_rows


def generate_dashboard(sheets: SheetsClient, tab_name: str) -> None:
    """Read Q tab, build dashboard data, write Dashboard tab.

    Args:
        sheets: SheetsClient instance
        tab_name: Q tab name to read from (e.g., 'Q1')
    """
    all_values = sheets.get_quarterly_tracker(tab_name)
    if not all_values or len(all_values) < 2:
        print("[INFO] No data in Q tab, skipping dashboard generation")
        return

    quarter_data = parse_consolidated_sheet(all_values)
    if not quarter_data:
        print("[INFO] No ASIN data parsed, skipping dashboard generation")
        return

    # Determine number of weeks from headers
    headers = all_values[0]
    week_count = sum(1 for h in headers if h.endswith(" Vol"))

    summary_headers, summary_rows, flagged_section = build_dashboard(
        quarter_data, week_count
    )

    # Build combined sheet: summary section, blank row, flagged section
    dashboard_rows: list[list[Any]] = []
    dashboard_rows.append(summary_headers)
    dashboard_rows.extend(summary_rows)
    dashboard_rows.append([])  # blank separator row
    dashboard_rows.extend(flagged_section)

    # Write using the same sheets client method
    sheets.write_quarterly_tracker(
        DASHBOARD_TAB_NAME, dashboard_rows[0], dashboard_rows[1:]
    )
    print(
        f"  Dashboard written with {len(summary_rows)} ASINs, "
        f"{len(flagged_section) - 1} flagged keywords"
    )


def check_keyword_alerts(
    keyword: str,
    current_listing: ListingContent | None,
    previous_in_title: bool,
    previous_in_backend: bool,
) -> list[str]:
    """Check for alerts when keyword drops from title/backend.

    Args:
        keyword: The keyword to check
        current_listing: Current listing content
        previous_in_title: Whether keyword was previously in title
        previous_in_backend: Whether keyword was previously in backend

    Returns:
        List of alert messages
    """
    alerts = []

    if current_listing is None:
        return alerts

    in_title, in_backend = current_listing.contains_keyword(keyword)

    if previous_in_title and not in_title:
        alerts.append("DROPPED FROM TITLE")

    if previous_in_backend and not in_backend:
        alerts.append("DROPPED FROM BACKEND")

    return alerts


def _build_asin_keywords(
    asin: str,
    weekly_snapshots: dict[str, WeeklySnapshot],
    week_labels: list[str],
    listing: ListingContent | None,
    config: AppConfig,
) -> list[QuarterlyKeyword]:
    """Build QuarterlyKeyword list for a single ASIN from weekly snapshots.

    Args:
        asin: ASIN being tracked
        weekly_snapshots: Map of week_label -> WeeklySnapshot
        week_labels: Ordered list of week labels
        listing: Optional listing content for placement detection
        config: App configuration for thresholds

    Returns:
        List of QuarterlyKeyword (up to TOP_KEYWORDS_COUNT)
    """
    current_week_label = week_labels[-1]

    # Use most recent week to determine top keywords
    latest_snapshot = (
        weekly_snapshots[current_week_label]
        if current_week_label in weekly_snapshots
        else list(weekly_snapshots.values())[-1]
    )

    top_keywords = get_top_keywords(latest_snapshot)
    if not top_keywords:
        return []

    quarterly_keywords: list[QuarterlyKeyword] = []

    for rank, latest_record in enumerate(top_keywords, 1):
        keyword = latest_record.search_query
        keyword_lower = keyword.lower()

        # Check keyword placement
        in_title = False
        in_backend = False
        if listing:
            in_title, in_backend = listing.contains_keyword(keyword)

        # Gather metrics for all weeks
        weekly_metrics = {}
        for week_label in week_labels:
            if week_label in weekly_snapshots:
                snapshot = weekly_snapshots[week_label]
                record = None
                for r in snapshot.records:
                    if r.search_query.lower() == keyword_lower:
                        record = r
                        break

                if record:
                    diagnostic = get_diagnostic_type(record, config.thresholds)
                    opp_score = calculate_opportunity_score(record, diagnostic)
                    rank_status = get_rank_status(
                        record.impressions_share, config.thresholds
                    )

                    weekly_metrics[week_label] = {
                        "volume": record.search_volume,
                        "imp_share": round(record.impressions_share, 1),
                        "click_share": round(record.clicks_share, 1),
                        "purchase_share": round(record.purchases_share, 1),
                        "opportunity_score": opp_score,
                        "rank_status": rank_status.value,
                    }
                else:
                    weekly_metrics[week_label] = {
                        "volume": "-",
                        "imp_share": "-",
                        "click_share": "-",
                        "purchase_share": "-",
                        "opportunity_score": "-",
                        "rank_status": "invisible",
                    }
            else:
                weekly_metrics[week_label] = {
                    "volume": "-",
                    "imp_share": "-",
                    "click_share": "-",
                    "purchase_share": "-",
                    "opportunity_score": "-",
                    "rank_status": "-",
                }

        qk = QuarterlyKeyword(
            asin=asin,
            rank=rank,
            keyword=keyword,
            in_title=in_title,
            in_backend=in_backend,
            weekly_metrics=weekly_metrics,
            alerts=[],
        )
        quarterly_keywords.append(qk)

    return quarterly_keywords


def start_quarter(config: AppConfig) -> bool:
    """Initialize consolidated quarterly tracker with all active ASINs.

    Reads active ASINs from the master sheet, fetches SQP data for each,
    and writes a single consolidated tab (e.g., 'Q1') with all ASINs.

    Args:
        config: App configuration

    Returns:
        True if successful
    """
    credentials = get_credentials()
    quarter, year = get_current_quarter()
    tab_name = get_consolidated_tab_name(quarter)

    # Get all complete weeks in the quarter so far
    quarter_weeks = get_quarter_weeks()
    if not quarter_weeks:
        print("[ERROR] No complete weeks in quarter yet")
        return False

    week_labels = [f"W{w[0]:02d}" for w in quarter_weeks]
    current_week_label = week_labels[-1]

    # Read active ASINs from master sheet
    sheets = SheetsClient(config.sheets)
    asin_list = sheets.get_active_asins()

    if not asin_list:
        print("[ERROR] No active ASINs found in master list")
        return False

    print(f"\n{'=' * 60}")
    print(f"Starting Q{quarter} {year} consolidated tracker")
    print(f"Tab name: {tab_name}")
    print(f"Active ASINs: {len(asin_list)}")
    print(
        f"Fetching weeks: {week_labels[0]} through {current_week_label} ({len(quarter_weeks)} weeks)"
    )
    print("=" * 60)

    # Build headers
    headers = build_headers(week_labels)
    num_cols = len(headers)
    all_rows: list[list] = []

    for asin_info in asin_list:
        asin = asin_info["asin"]
        sku = asin_info.get("sku", "")
        product_name = asin_info.get("name", "")

        print(f"\n--- Processing {asin} ({product_name}) ---")

        # Fetch SQP data for all weeks
        weekly_snapshots: dict[str, WeeklySnapshot] = {}
        for week_num, start_date, end_date in quarter_weeks:
            week_label = f"W{week_num:02d}"
            print(f"  Fetching {week_label} ({start_date} to {end_date})")

            report_data = fetch_sqp_report(credentials, asin, start_date, end_date)
            if report_data:
                snapshot = parse_report_to_snapshot(report_data)
                if snapshot:
                    weekly_snapshots[week_label] = snapshot
                    print(f"    Loaded {len(snapshot.records)} keywords")
                else:
                    print(f"    [WARNING] Could not parse {week_label} data")
            else:
                print(f"    [WARNING] Could not fetch {week_label} data")

        if not weekly_snapshots:
            print(f"  [WARNING] No SQP data for {asin}, skipping")
            continue

        # Fetch listing content
        listing = None
        if sku and config.sp_api.seller_id:
            listing = get_listing_content(config.sp_api.seller_id, sku)

        # Build keywords for this ASIN
        quarterly_keywords = _build_asin_keywords(
            asin, weekly_snapshots, week_labels, listing, config
        )

        if not quarterly_keywords:
            print(f"  [WARNING] No keywords with purchase data for {asin}")
            continue

        # Add separator row + keyword rows
        all_rows.append(build_asin_separator_row(asin, product_name, num_cols))
        all_rows.extend(qk.to_row(week_labels) for qk in quarterly_keywords)

        print(f"  Added {len(quarterly_keywords)} keywords")

    if not all_rows:
        print("[ERROR] No keyword data for any ASIN")
        return False

    # Write to Google Sheets
    print(f"\n  Writing to Google Sheets: {tab_name}")
    sheets.write_quarterly_tracker(tab_name, headers, all_rows)

    # Generate dashboard
    generate_dashboard(sheets, tab_name)

    # Print summary
    print(f"\n{'=' * 60}")
    print(f"Quarterly tracker initialized for Q{quarter} {year}")
    print(f"Weeks included: {week_labels[0]} - {current_week_label}")
    print(f"ASINs tracked: {len(asin_list)}")
    print(f"{'=' * 60}")

    print(
        f"\nView results: https://docs.google.com/spreadsheets/d/{config.sheets.spreadsheet_id}"
    )
    return True


def update_week(config: AppConfig) -> bool:
    """Update consolidated quarterly tracker with new week's metrics.

    Reads the existing consolidated sheet, fetches new data for all active ASINs,
    and rebuilds the sheet. New ASINs get full initialization; existing ASINs
    get the current week merged in.

    Args:
        config: App configuration

    Returns:
        True if successful
    """
    credentials = get_credentials()
    quarter, year = get_current_quarter()
    tab_name = get_consolidated_tab_name(quarter)
    week_num = get_week_in_quarter()
    week_label = f"W{week_num:02d}"

    print(f"\n{'=' * 60}")
    print(f"Updating Q{quarter} {year} consolidated tracker")
    print(f"Tab name: {tab_name}")
    print(f"Week: {week_label}")
    print("=" * 60)

    # Read active ASINs from master sheet
    sheets = SheetsClient(config.sheets)
    asin_list = sheets.get_active_asins()

    if not asin_list:
        print("[ERROR] No active ASINs found in master list")
        return False

    # Read existing tracker data
    existing_data = sheets.get_quarterly_tracker(tab_name)

    if not existing_data:
        print(f"[INFO] No existing tracker {tab_name}, running full start instead")
        return start_quarter(config)

    headers = existing_data[0]
    existing_asins = parse_consolidated_sheet(existing_data)

    # Determine existing weeks from headers
    existing_weeks = []
    for h in headers:
        if h.endswith(" Vol"):
            week = h.replace(" Vol", "")
            if week not in existing_weeks:
                existing_weeks.append(week)

    # Add current week if not present
    all_weeks = existing_weeks.copy()
    if week_label not in all_weeks:
        all_weeks.append(week_label)
    all_weeks.sort()

    # Build new headers and all rows
    new_headers = build_headers(all_weeks)
    num_cols = len(new_headers)
    all_rows: list[list] = []
    alerts_found: list[tuple[str, str, str]] = []  # (asin, keyword, alert)

    for asin_info in asin_list:
        asin = asin_info["asin"]
        sku = asin_info.get("sku", "")
        product_name = asin_info.get("name", "")

        print(f"\n--- Processing {asin} ({product_name}) ---")

        if asin in existing_asins:
            # UPDATE PATH: ASIN exists in sheet, merge new week data
            print(f"  Updating existing ASIN with {week_label} data")

            # Fetch current week's SQP data
            start_date, end_date = get_last_complete_week()
            report_data = fetch_sqp_report(credentials, asin, start_date, end_date)

            snapshot = None
            if report_data:
                snapshot = parse_report_to_snapshot(report_data)

            current_data = {}
            if snapshot:
                current_data = {r.search_query.lower(): r for r in snapshot.records}

            # Fetch listing content
            listing = None
            if sku and config.sp_api.seller_id:
                listing = get_listing_content(config.sp_api.seller_id, sku)

            # Rebuild rows for this ASIN
            asin_data = existing_asins[asin]
            all_rows.append(
                build_asin_separator_row(
                    asin, asin_data["name"] or product_name, num_cols
                )
            )

            for kw_info in asin_data["keywords"]:
                keyword = kw_info["keyword"]
                keyword_lower = keyword.lower()
                record = current_data.get(keyword_lower)
                old_row = kw_info["row_data"]

                # Check keyword placement
                current_in_title = kw_info["in_title"]
                current_in_backend = kw_info["in_backend"]

                if listing:
                    current_in_title, current_in_backend = listing.contains_keyword(
                        keyword
                    )
                    alerts = check_keyword_alerts(
                        keyword, listing, kw_info["in_title"], kw_info["in_backend"]
                    )
                    if alerts:
                        alerts_found.extend((asin, keyword, a) for a in alerts)

                # Build new row
                row = [
                    asin,
                    kw_info["rank"],
                    keyword,
                    "YES" if current_in_title else "NO",
                    "YES" if current_in_backend else "NO",
                ]

                # Add metrics for each week
                for week in all_weeks:
                    if week == week_label and record:
                        diagnostic = get_diagnostic_type(record, config.thresholds)
                        opp_score = calculate_opportunity_score(record, diagnostic)
                        rank_status = get_rank_status(
                            record.impressions_share, config.thresholds
                        )
                        row.extend(
                            [
                                record.search_volume,
                                round(record.impressions_share, 1),
                                round(record.clicks_share, 1),
                                round(record.purchases_share, 1),
                                opp_score,
                                rank_status.value,
                            ]
                        )
                    elif week == week_label:
                        row.extend(["-", "-", "-", "-", "-", "invisible"])
                    else:
                        # Preserve existing data from old row
                        week_start_idx = None
                        for i, h in enumerate(headers):
                            if h == f"{week} Vol":
                                week_start_idx = i
                                break

                        if week_start_idx is not None and week_start_idx + 6 <= len(
                            old_row
                        ):
                            row.extend(old_row[week_start_idx : week_start_idx + 6])
                        else:
                            row.extend(["", "", "", "", "", ""])

                # Add alerts
                kw_alerts = [
                    a for aa, kw, a in alerts_found if aa == asin and kw == keyword
                ]
                row.append(" | ".join(kw_alerts) if kw_alerts else "")

                all_rows.append(row)

            print(f"  Updated {len(asin_data['keywords'])} keywords")

        else:
            # START PATH: New ASIN, fetch all weeks
            print("  New ASIN, fetching all weeks")

            quarter_weeks = get_quarter_weeks()
            weekly_snapshots: dict[str, WeeklySnapshot] = {}

            for wk_num, start_date, end_date in quarter_weeks:
                wk_label = f"W{wk_num:02d}"
                report_data = fetch_sqp_report(credentials, asin, start_date, end_date)
                if report_data:
                    snap = parse_report_to_snapshot(report_data)
                    if snap:
                        weekly_snapshots[wk_label] = snap

            if not weekly_snapshots:
                print(f"  [WARNING] No SQP data for new ASIN {asin}, skipping")
                continue

            listing = None
            if sku and config.sp_api.seller_id:
                listing = get_listing_content(config.sp_api.seller_id, sku)

            quarterly_keywords = _build_asin_keywords(
                asin, weekly_snapshots, all_weeks, listing, config
            )

            if not quarterly_keywords:
                print(f"  [WARNING] No keywords with purchase data for {asin}")
                continue

            all_rows.append(build_asin_separator_row(asin, product_name, num_cols))
            all_rows.extend(qk.to_row(all_weeks) for qk in quarterly_keywords)

            print(f"  Added {len(quarterly_keywords)} keywords")

    if not all_rows:
        print("[ERROR] No keyword data for any ASIN")
        return False

    # Write updated data
    print(f"\n  Writing updated data to {tab_name}")
    sheets.write_quarterly_tracker(tab_name, new_headers, all_rows)

    # Generate dashboard
    generate_dashboard(sheets, tab_name)

    # Print summary
    print(f"\n{'=' * 60}")
    print(f"Updated {tab_name} with {week_label} data")
    print("=" * 60)

    if alerts_found:
        print(f"\n[ALERTS] {len(alerts_found)} placement changes detected:")
        for asin, keyword, alert in alerts_found:
            print(f"  - {asin} / {keyword}: {alert}")

    print(
        f"\nView results: https://docs.google.com/spreadsheets/d/{config.sheets.spreadsheet_id}"
    )
    return True


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser."""
    parser = argparse.ArgumentParser(
        description="Quarterly keyword tracker for Amazon SQP analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Start new quarter (all active ASINs from master sheet)
    python -m sqp_analyzer.commands.quarterly_tracker --start

    # Weekly update (all active ASINs)
    python -m sqp_analyzer.commands.quarterly_tracker --update
        """,
    )
    parser.add_argument(
        "--start",
        action="store_true",
        help="Start new quarter tracker with top 10 keywords for all active ASINs",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Update tracker with new week's data for all active ASINs",
    )
    parser.add_argument(
        "--test-sheets",
        action="store_true",
        help="Test Google Sheets connection",
    )
    return parser


def main() -> int:
    """Main entry point."""
    parser = create_parser()
    args = parser.parse_args()

    # Load config
    try:
        config = load_config()
    except Exception as e:
        print(f"[ERROR] Failed to load configuration: {e}")
        return 1

    # Test sheets connection
    if args.test_sheets:
        print("Testing Google Sheets connection...")
        sheets = SheetsClient(config.sheets)
        if sheets.test_connection():
            print("[SUCCESS] Connected to Google Sheets")
            return 0
        else:
            print("[FAILED] Could not connect to Google Sheets")
            return 1

    if args.start:
        return 0 if start_quarter(config) else 1
    elif args.update:
        return 0 if update_week(config) else 1

    # No action specified
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
