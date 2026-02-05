#!/usr/bin/env python3
"""Quarterly keyword tracker for Amazon SQP analysis.

Tracks top 10 keywords per ASIN, locked for the quarter.
Detects keyword placement in title/backend and alerts on changes.

Usage:
    # Start new quarter for an ASIN
    python -m sqp_analyzer.commands.quarterly_tracker --start --asin B0CSH12L5P

    # Weekly update (run every week)
    python -m sqp_analyzer.commands.quarterly_tracker --update --asin B0CSH12L5P

    # Update all ASINs from master list
    python -m sqp_analyzer.commands.quarterly_tracker --update-all
"""

import argparse
import gzip
import json
import sys
import time
from datetime import date, timedelta

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


def get_tab_name(asin: str, quarter: int | None = None, year: int | None = None) -> str:
    """Generate tab name for quarterly tracker.

    Args:
        asin: ASIN to track
        quarter: Quarter number (1-4), defaults to current
        year: Year, defaults to current

    Returns:
        Tab name like 'Q1-B0CSH12L5P'
    """
    if quarter is None or year is None:
        q, y = get_current_quarter()
        quarter = quarter or q
        year = year or y

    return f"Q{quarter}-{asin}"


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
    headers = ["Rank", "Keyword", "In Title", "In Backend"]

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


def start_quarter(config: AppConfig, asin: str, sku: str | None = None) -> bool:
    """Initialize quarterly tracker with top 10 keywords and all weeks so far.

    Args:
        config: App configuration
        asin: ASIN to track
        sku: Optional SKU for listing lookup (if not provided, won't check placement)

    Returns:
        True if successful
    """
    credentials = get_credentials()
    quarter, year = get_current_quarter()
    tab_name = get_tab_name(asin, quarter, year)

    # Get all complete weeks in the quarter so far
    quarter_weeks = get_quarter_weeks()
    if not quarter_weeks:
        print("[ERROR] No complete weeks in quarter yet")
        return False

    week_labels = [f"W{w[0]:02d}" for w in quarter_weeks]
    current_week_label = week_labels[-1]

    print(f"\n{'=' * 60}")
    print(f"Starting Q{quarter} {year} tracker for ASIN: {asin}")
    print(f"Tab name: {tab_name}")
    print(
        f"Fetching weeks: {week_labels[0]} through {current_week_label} ({len(quarter_weeks)} weeks)"
    )
    print("=" * 60)

    # Fetch SQP data for all weeks
    weekly_snapshots: dict[str, WeeklySnapshot] = {}

    for week_num, start_date, end_date in quarter_weeks:
        week_label = f"W{week_num:02d}"
        print(f"\n--- Fetching {week_label} ({start_date} to {end_date}) ---")

        report_data = fetch_sqp_report(credentials, asin, start_date, end_date)
        if report_data:
            snapshot = parse_report_to_snapshot(report_data)
            if snapshot:
                weekly_snapshots[week_label] = snapshot
                print(f"  Loaded {len(snapshot.records)} keywords")
            else:
                print(f"  [WARNING] Could not parse {week_label} data")
        else:
            print(f"  [WARNING] Could not fetch {week_label} data")

    if not weekly_snapshots:
        print("[ERROR] Failed to fetch any SQP data")
        return False

    # Use most recent week to determine top keywords
    latest_snapshot = (
        weekly_snapshots[current_week_label]
        if current_week_label in weekly_snapshots
        else list(weekly_snapshots.values())[-1]
    )

    # Get top 10 keywords
    top_keywords = get_top_keywords(latest_snapshot)
    if not top_keywords:
        print("[ERROR] No keywords with purchase data found")
        return False

    print(
        f"\n  Selected top {len(top_keywords)} keywords by volume from {current_week_label}"
    )

    # Fetch listing content if SKU provided
    listing = None
    if sku and config.sp_api.seller_id:
        print(f"\n  Fetching listing content for SKU: {sku}")
        listing = get_listing_content(config.sp_api.seller_id, sku)
        if listing:
            print(f"  Title: {listing.title[:50]}...")
        else:
            print("  [WARNING] Could not fetch listing content")

    # Build quarterly keywords with metrics for all weeks
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
                # Find this keyword in the snapshot
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
                    # Keyword not in this week's data
                    weekly_metrics[week_label] = {
                        "volume": "-",
                        "imp_share": "-",
                        "click_share": "-",
                        "purchase_share": "-",
                        "opportunity_score": "-",
                        "rank_status": "invisible",
                    }
            else:
                # No data for this week
                weekly_metrics[week_label] = {
                    "volume": "-",
                    "imp_share": "-",
                    "click_share": "-",
                    "purchase_share": "-",
                    "opportunity_score": "-",
                    "rank_status": "-",
                }

        qk = QuarterlyKeyword(
            rank=rank,
            keyword=keyword,
            in_title=in_title,
            in_backend=in_backend,
            weekly_metrics=weekly_metrics,
            alerts=[],
        )
        quarterly_keywords.append(qk)

    # Build headers and rows
    headers = build_headers(week_labels)
    rows = [qk.to_row(week_labels) for qk in quarterly_keywords]

    # Write to Google Sheets
    print(f"\n  Writing to Google Sheets: {tab_name}")
    sheets = SheetsClient(config.sheets)
    sheets.write_quarterly_tracker(tab_name, headers, rows)

    # Print summary
    print(f"\n{'=' * 60}")
    print(f"Quarterly tracker initialized for Q{quarter} {year}")
    print(f"Weeks included: {week_labels[0]} - {current_week_label}")
    print(f"{'=' * 60}")
    print(f"\n{'Rank':<4} {'Keyword':<35} {'Title':>6} {'Back':>6} {'Vol':>6}")
    print("-" * 65)
    for qk in quarterly_keywords:
        title_mark = "YES" if qk.in_title else "NO"
        backend_mark = "YES" if qk.in_backend else "NO"
        vol = qk.weekly_metrics.get(current_week_label, {}).get("volume", 0)
        print(
            f"{qk.rank:<4} {qk.keyword[:33]:<35} {title_mark:>6} {backend_mark:>6} {vol:>6}"
        )

    print(
        f"\nView results: https://docs.google.com/spreadsheets/d/{config.sheets.spreadsheet_id}"
    )
    return True


def update_week(config: AppConfig, asin: str, sku: str | None = None) -> bool:
    """Update quarterly tracker with new week's metrics.

    Args:
        config: App configuration
        asin: ASIN to update
        sku: Optional SKU for listing lookup

    Returns:
        True if successful
    """
    credentials = get_credentials()
    quarter, year = get_current_quarter()
    tab_name = get_tab_name(asin, quarter, year)
    week_num = get_week_in_quarter()
    week_label = f"W{week_num:02d}"

    print(f"\n{'=' * 60}")
    print(f"Updating Q{quarter} {year} tracker for ASIN: {asin}")
    print(f"Tab name: {tab_name}")
    print(f"Week: {week_label}")
    print("=" * 60)

    # Read existing tracker data
    sheets = SheetsClient(config.sheets)
    existing_data = sheets.get_quarterly_tracker(tab_name)

    if not existing_data:
        print(f"[ERROR] Quarterly tracker {tab_name} not found")
        print("Run with --start first to initialize the quarter")
        return False

    headers = existing_data[0]
    data_rows = existing_data[1:]

    # Check if this week already exists
    if any(week_label in h for h in headers):
        print(f"[WARNING] Week {week_label} already has data. Updating...")

    # Parse existing keywords
    existing_keywords = []
    for row in data_rows:
        if len(row) >= 4:
            existing_keywords.append(
                {
                    "rank": row[0],
                    "keyword": row[1],
                    "prev_in_title": row[2] == "YES",
                    "prev_in_backend": row[3] == "YES",
                }
            )

    if not existing_keywords:
        print("[ERROR] No keywords found in tracker")
        return False

    # Fetch new SQP data
    start_date, end_date = get_last_complete_week()
    report_data = fetch_sqp_report(credentials, asin, start_date, end_date)

    if not report_data:
        print("[ERROR] Failed to fetch SQP data")
        return False

    snapshot = parse_report_to_snapshot(report_data)
    if not snapshot:
        print("[ERROR] Failed to parse SQP report")
        return False

    # Build lookup for current week's data
    current_data = {r.search_query.lower(): r for r in snapshot.records}

    # Fetch current listing content
    listing = None
    if sku and config.sp_api.seller_id:
        print(f"\n  Fetching listing content for SKU: {sku}")
        listing = get_listing_content(config.sp_api.seller_id, sku)

    # Determine which weeks already exist
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

    # Build new rows with updated data
    new_rows = []
    alerts_found = []

    for existing in existing_keywords:
        keyword = existing["keyword"]
        keyword_lower = keyword.lower()

        # Check if keyword is in current data
        record = current_data.get(keyword_lower)

        # Check keyword placement
        current_in_title = existing["prev_in_title"]
        current_in_backend = existing["prev_in_backend"]

        if listing:
            current_in_title, current_in_backend = listing.contains_keyword(keyword)

            # Check for alerts
            alerts = check_keyword_alerts(
                keyword,
                listing,
                existing["prev_in_title"],
                existing["prev_in_backend"],
            )
            if alerts:
                alerts_found.extend([(keyword, a) for a in alerts])

        # Build row
        row = [
            existing["rank"],
            keyword,
            "YES" if current_in_title else "NO",
            "YES" if current_in_backend else "NO",
        ]

        # Add metrics for each week
        for week in all_weeks:
            if week == week_label and record:
                # Current week - use fresh data
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
                # Current week but no data for this keyword
                row.extend(["-", "-", "-", "-", "-", "invisible"])
            else:
                # Previous week - preserve existing data
                # Find the column indices for this week
                week_start_idx = None
                for i, h in enumerate(headers):
                    if h == f"{week} Vol":
                        week_start_idx = i
                        break

                if week_start_idx is not None:
                    # Get the row index for this keyword
                    row_idx = None
                    for i, data_row in enumerate(data_rows):
                        if len(data_row) > 1 and data_row[1] == keyword:
                            row_idx = i
                            break

                    if row_idx is not None and week_start_idx + 6 <= len(
                        data_rows[row_idx]
                    ):
                        row.extend(
                            data_rows[row_idx][week_start_idx : week_start_idx + 6]
                        )
                    else:
                        row.extend(["", "", "", "", "", ""])
                else:
                    row.extend(["", "", "", "", "", ""])

        # Add alerts
        keyword_alerts = [a for kw, a in alerts_found if kw == keyword]
        row.append(" | ".join(keyword_alerts) if keyword_alerts else "")

        new_rows.append(row)

    # Build new headers
    new_headers = build_headers(all_weeks)

    # Write updated data
    print(f"\n  Writing updated data to {tab_name}")
    sheets.write_quarterly_tracker(tab_name, new_headers, new_rows)

    # Print summary
    print(f"\n{'=' * 60}")
    print(f"Updated {tab_name} with {week_label} data")
    print("=" * 60)

    if alerts_found:
        print(f"\n[ALERTS] {len(alerts_found)} placement changes detected:")
        for keyword, alert in alerts_found:
            print(f"  - {keyword}: {alert}")

    print(
        f"\nView results: https://docs.google.com/spreadsheets/d/{config.sheets.spreadsheet_id}"
    )
    return True


def update_all(config: AppConfig) -> bool:
    """Update all active ASINs from master list.

    Args:
        config: App configuration

    Returns:
        True if all updates successful
    """
    print(f"\n{'=' * 60}")
    print("Updating all active ASINs")
    print("=" * 60)

    sheets = SheetsClient(config.sheets)
    asins = sheets.get_active_asins()

    if not asins:
        print("[ERROR] No active ASINs found in master list")
        return False

    print(f"Found {len(asins)} active ASINs")

    success_count = 0
    for asin_info in asins:
        asin = asin_info["asin"]
        sku = asin_info.get("sku", "")

        try:
            # Check if quarterly tracker exists
            quarter, year = get_current_quarter()
            tab_name = get_tab_name(asin, quarter, year)

            if sheets.get_quarterly_tracker(tab_name):
                # Tracker exists, update it
                if update_week(config, asin, sku if sku else None):
                    success_count += 1
            else:
                # No tracker, start new quarter
                print(f"\n  No tracker found for {asin}, starting new quarter...")
                if start_quarter(config, asin, sku if sku else None):
                    success_count += 1

        except Exception as e:
            print(f"\n[ERROR] Failed to update {asin}: {e}")
            continue

    print(f"\n{'=' * 60}")
    print(f"Completed: {success_count}/{len(asins)} ASINs updated")
    print("=" * 60)

    return success_count == len(asins)


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser."""
    parser = argparse.ArgumentParser(
        description="Quarterly keyword tracker for Amazon SQP analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Start new quarter for an ASIN
    python -m sqp_analyzer.commands.quarterly_tracker --start --asin B0CSH12L5P

    # Start with SKU for title/backend detection
    python -m sqp_analyzer.commands.quarterly_tracker --start --asin B0CSH12L5P --sku YOUR-SKU

    # Weekly update
    python -m sqp_analyzer.commands.quarterly_tracker --update --asin B0CSH12L5P

    # Update all ASINs from master list
    python -m sqp_analyzer.commands.quarterly_tracker --update-all
        """,
    )
    parser.add_argument(
        "--start",
        action="store_true",
        help="Start new quarter tracker with top 10 keywords",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Update existing tracker with new week's data",
    )
    parser.add_argument(
        "--update-all",
        action="store_true",
        help="Update all active ASINs from master list",
    )
    parser.add_argument(
        "--asin",
        type=str,
        help="ASIN to track",
    )
    parser.add_argument(
        "--sku",
        type=str,
        help="SKU for listing content lookup (title/backend detection)",
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

    # Update all ASINs
    if args.update_all:
        return 0 if update_all(config) else 1

    # Single ASIN operations require --asin
    if args.start or args.update:
        if not args.asin:
            print("[ERROR] --asin is required for --start and --update")
            return 1

        asin = args.asin.upper()
        sku = args.sku

        if args.start:
            return 0 if start_quarter(config, asin, sku) else 1
        elif args.update:
            return 0 if update_week(config, asin, sku) else 1

    # No action specified
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
