#!/usr/bin/env python3
"""Analyze SQP data and write results to Google Sheets.

This command fetches a completed SQP report from SP-API, runs diagnostic
and placement analysis, and writes results to Google Sheets.

Usage:
    python -m sqp_analyzer.commands.analyze_sqp --report-id 129706020488
"""

import argparse
import gzip
import json
import sys
from datetime import date

import requests
from sp_api.api import Reports
from sp_api.base import Marketplaces

from ..analyzers.diagnostic import DiagnosticAnalyzer
from ..analyzers.placement import PlacementRecommender
from ..config import load_config
from ..models import SQPRecord, WeeklySnapshot
from ..sheets.client import SheetsClient


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser."""
    parser = argparse.ArgumentParser(
        description="Analyze SQP data and write to Google Sheets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Analyze completed report and write to sheets
    python -m sqp_analyzer.commands.analyze_sqp --report-id 129706020488

    # Test sheets connection only
    python -m sqp_analyzer.commands.analyze_sqp --test-sheets
        """,
    )
    parser.add_argument(
        "--report-id",
        type=str,
        help="Completed report ID to analyze",
    )
    parser.add_argument(
        "--test-sheets",
        action="store_true",
        help="Test Google Sheets connection only",
    )
    return parser


def get_credentials() -> dict:
    """Load SP-API credentials from environment."""
    config = load_config()
    return {
        "refresh_token": config.sp_api.refresh_token,
        "lwa_app_id": config.sp_api.client_id,
        "lwa_client_secret": config.sp_api.client_secret,
    }


def fetch_report_data(credentials: dict, report_id: str) -> dict | None:
    """Fetch completed report data from SP-API."""
    report = Reports(credentials=credentials, marketplace=Marketplaces.US)

    # Check report status
    res = report.get_report(reportId=report_id)
    status = res.payload.get("processingStatus")
    doc_id = res.payload.get("reportDocumentId")

    if status != "DONE":
        print(f"Report {report_id} is not ready: {status}")
        return None

    if not doc_id:
        print(f"Report {report_id} has no document ID")
        return None

    # Download report
    doc_res = report.get_report_document(reportDocumentId=doc_id, download=False)
    url = doc_res.payload.get("url")

    response = requests.get(url)
    if doc_res.payload.get("compressionAlgorithm") == "GZIP":
        data = gzip.decompress(response.content).decode("utf-8")
    else:
        data = response.text

    return json.loads(data)


def parse_report_to_snapshot(report_data: dict) -> WeeklySnapshot | None:
    """Parse report data into WeeklySnapshot with SQPRecords."""
    if "errorDetails" in report_data:
        print(f"Report error: {report_data['errorDetails']}")
        return None

    spec = report_data.get("reportSpecification", {})
    start_date_str = spec.get("dataStartTime", "")[:10]

    try:
        week_date = date.fromisoformat(start_date_str)
    except ValueError:
        week_date = date.today()

    entries = report_data.get("dataByAsin", [])
    if not entries:
        print("No data in report")
        return None

    # Get ASIN from first entry
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


def main() -> int:
    """Main entry point."""
    parser = create_parser()
    args = parser.parse_args()

    # Load config
    config = load_config()

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

    if not args.report_id:
        parser.print_help()
        return 1

    # Fetch report data
    print(f"Fetching report {args.report_id}...")
    credentials = get_credentials()
    report_data = fetch_report_data(credentials, args.report_id)

    if not report_data:
        return 1

    # Parse to snapshot
    print("Parsing report data...")
    snapshot = parse_report_to_snapshot(report_data)

    if not snapshot:
        return 1

    print(f"Loaded {len(snapshot.records)} keywords for ASIN {snapshot.asin}")
    print(f"Week: {snapshot.week_date}")

    # Run diagnostic analysis
    print("\nRunning diagnostic analysis...")
    diagnostic_analyzer = DiagnosticAnalyzer(config.thresholds)
    diagnostics = diagnostic_analyzer.analyze(snapshot)
    diag_summary = diagnostic_analyzer.summarize(diagnostics)

    print(f"  Total: {diag_summary['total']}")
    print(f"  Ghost: {diag_summary['ghost']}")
    print(f"  Window Shopper: {diag_summary['window_shopper']}")
    print(f"  Price Problem: {diag_summary['price_problem']}")
    print(f"  Healthy: {diag_summary['healthy']}")

    # Run placement analysis
    print("\nRunning placement analysis...")
    placement_recommender = PlacementRecommender(config.thresholds)
    placements = placement_recommender.analyze(snapshot)
    place_summary = placement_recommender.summarize(placements)

    print(f"  Title: {place_summary['title']}")
    print(f"  Bullets: {place_summary['bullets']}")
    print(f"  Backend: {place_summary['backend']}")
    print(f"  Description: {place_summary['description']}")

    # Write to Google Sheets
    print("\nWriting to Google Sheets...")
    sheets = SheetsClient(config.sheets)

    # Write diagnostics
    diag_dicts = [d.to_dict() for d in diagnostics]
    sheets.write_diagnostics(diag_dicts)
    print(f"  Wrote {len(diag_dicts)} rows to SQP-Diagnostics")

    # Write placements
    place_dicts = [p.to_dict() for p in placements]
    sheets.write_placements(place_dicts)
    print(f"  Wrote {len(place_dicts)} rows to SQP-Placements")

    # Write top opportunities (top 20 by opportunity score)
    top_opportunities = sorted(
        diagnostics,
        key=lambda d: d.opportunity_score,
        reverse=True,
    )[:20]
    opp_dicts = [d.to_dict() for d in top_opportunities]
    sheets.write_opportunity_ranking(opp_dicts)
    print(f"  Wrote {len(opp_dicts)} rows to SQP-TopOpportunities")

    print("\n[SUCCESS] Analysis complete!")
    print(f"View results: https://docs.google.com/spreadsheets/d/{config.sheets.spreadsheet_id}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
