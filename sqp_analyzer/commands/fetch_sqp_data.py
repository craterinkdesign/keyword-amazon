#!/usr/bin/env python3
"""Fetch SQP data from Amazon Brand Analytics API.

This command fetches Search Query Performance data using the SP-API Reports API
and outputs it to the console for validation.

Usage:
    python -m sqp_analyzer.commands.fetch_sqp_data --asin B0CSH12L5P
    python -m sqp_analyzer.commands.fetch_sqp_data --asin B0CSH12L5P --check 129706020488
"""

import argparse
import gzip
import json
import sys
import time
from datetime import date, timedelta

import requests
from decouple import config
from sp_api.api import Reports
from sp_api.base import Marketplaces


def get_credentials() -> dict:
    """Load SP-API credentials from environment."""
    return {
        "refresh_token": config("SP_API_REFRESH_TOKEN"),
        "lwa_app_id": config("SP_API_CLIENT_ID"),
        "lwa_client_secret": config("SP_API_CLIENT_SECRET"),
    }


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser."""
    parser = argparse.ArgumentParser(
        description="Fetch SQP data from Amazon Brand Analytics API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Request new SQP report for an ASIN (Week 5: Jan 25-31)
    python -m sqp_analyzer.commands.fetch_sqp_data --asin B0CSH12L5P

    # Check status of a pending report
    python -m sqp_analyzer.commands.fetch_sqp_data --check 129706020488

    # List recent reports
    python -m sqp_analyzer.commands.fetch_sqp_data --list

    # Test API connection
    python -m sqp_analyzer.commands.fetch_sqp_data --test-connection

Note: Brand Analytics reports can take 30-60 minutes to process.
        """,
    )
    parser.add_argument(
        "--asin",
        type=str,
        help="ASIN to fetch SQP data for",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD, must be Sunday for WEEK period)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--check",
        type=str,
        metavar="REPORT_ID",
        help="Check status of a pending report and download if ready",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List recent SQP reports",
    )
    parser.add_argument(
        "--test-connection",
        action="store_true",
        help="Test API connection only",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Wait for report to complete (can take 30-60 min)",
    )
    return parser


def get_last_complete_week() -> tuple[date, date]:
    """Get the last complete week (Sunday to Saturday)."""
    today = date.today()
    # Find last Saturday
    days_since_saturday = (today.weekday() + 2) % 7
    if days_since_saturday == 0:
        days_since_saturday = 7  # If today is Saturday, use previous week
    last_saturday = today - timedelta(days=days_since_saturday)
    last_sunday = last_saturday - timedelta(days=6)
    return last_sunday, last_saturday


def test_connection(credentials: dict) -> bool:
    """Test the SP-API connection."""
    print("Testing SP-API connection...")
    try:
        report = Reports(credentials=credentials, marketplace=Marketplaces.US)
        res = report.get_reports(
            reportTypes=["GET_MERCHANT_LISTINGS_ALL_DATA"],
            pageSize=1,
        )
        print("\n[SUCCESS] Connected to SP-API")
        return True
    except Exception as e:
        print(f"\n[FAILED] {e}")
        return False


def list_reports(credentials: dict) -> None:
    """List recent SQP reports."""
    report = Reports(credentials=credentials, marketplace=Marketplaces.US)

    print("Recent SQP Reports:")
    print("-" * 80)

    res = report.get_reports(
        reportTypes=["GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT"],
        pageSize=10,
    )

    for r in res.payload.get("reports", []):
        status = r.get("processingStatus")
        rid = r.get("reportId")
        created = r.get("createdTime", "")[:19]
        options = r.get("reportOptions", {})
        asin = options.get("asin", "N/A")

        status_icon = "✓" if status == "DONE" else "✗" if status == "FATAL" else "⏳"
        print(f"{status_icon} {rid} | {status:<12} | ASIN: {asin} | {created}")

        if status == "FATAL" and r.get("reportDocumentId"):
            # Show error
            doc_res = report.get_report_document(
                reportDocumentId=r.get("reportDocumentId"),
                download=False,
            )
            url = doc_res.payload.get("url")
            response = requests.get(url)
            data = gzip.decompress(response.content).decode("utf-8")
            error_data = json.loads(data)
            if "errorDetails" in error_data:
                print(f"    Error: {error_data['errorDetails']}")


def request_report(
    credentials: dict,
    asin: str,
    start_date: date,
    end_date: date,
) -> str:
    """Request a new SQP report."""
    report = Reports(credentials=credentials, marketplace=Marketplaces.US)

    print(f"Requesting SQP report...")
    print(f"  ASIN: {asin}")
    print(f"  Period: {start_date} to {end_date}")

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
    print(f"\n[SUBMITTED] Report ID: {report_id}")
    print("\nNote: Brand Analytics reports typically take 30-60 minutes to process.")
    print(f"Check status with: python -m sqp_analyzer.commands.fetch_sqp_data --check {report_id}")

    return report_id


def check_report(credentials: dict, report_id: str) -> bool:
    """Check report status and download if ready."""
    report = Reports(credentials=credentials, marketplace=Marketplaces.US)

    res = report.get_report(reportId=report_id)
    status = res.payload.get("processingStatus")
    doc_id = res.payload.get("reportDocumentId")

    print(f"Report {report_id}: {status}")

    if status == "DONE" and doc_id:
        download_and_display(report, doc_id)
        return True
    elif status == "FATAL" and doc_id:
        doc_res = report.get_report_document(reportDocumentId=doc_id, download=False)
        url = doc_res.payload.get("url")
        response = requests.get(url)
        data = gzip.decompress(response.content).decode("utf-8")
        error_data = json.loads(data)
        print(f"Error: {error_data.get('errorDetails', 'Unknown error')}")
        return False
    elif status in ("IN_QUEUE", "IN_PROGRESS"):
        print("Report is still processing. Check again later.")
        return False
    else:
        print(f"Unknown status: {status}")
        return False


def download_and_display(report: Reports, doc_id: str) -> None:
    """Download and display report data."""
    doc_res = report.get_report_document(reportDocumentId=doc_id, download=False)
    url = doc_res.payload.get("url")

    response = requests.get(url)
    if doc_res.payload.get("compressionAlgorithm") == "GZIP":
        data = gzip.decompress(response.content).decode("utf-8")
    else:
        data = response.text

    report_data = json.loads(data)

    if "errorDetails" in report_data:
        print(f"Error: {report_data['errorDetails']}")
        return

    # Display report
    spec = report_data.get("reportSpecification", {})
    options = spec.get("reportOptions", {})

    print("\n" + "=" * 80)
    print(f"SQP Report for ASIN: {options.get('asin', 'N/A')}")
    print(f"Period: {spec.get('dataStartTime', '')[:10]} to {spec.get('dataEndTime', '')[:10]}")
    print("=" * 80)

    # Group by ASIN
    by_asin = {}
    for entry in report_data.get("dataByAsin", []):
        asin = entry.get("asin")
        if asin not in by_asin:
            by_asin[asin] = []
        by_asin[asin].append(entry)

    for asin, entries in by_asin.items():
        print(f"\nASIN: {asin} ({len(entries)} search queries)")
        print("-" * 80)

        # Sort by search volume
        entries.sort(
            key=lambda x: x.get("searchQueryData", {}).get("searchQueryVolume", 0) or 0,
            reverse=True,
        )

        print(f"{'Search Query':<45} {'Vol':>6} {'Imp%':>6} {'Clk%':>6} {'Pur%':>6}")
        print("-" * 80)

        for entry in entries:
            sq = entry.get("searchQueryData", {})
            query = (sq.get("searchQuery", "") or "")[:43]
            volume = sq.get("searchQueryVolume", 0) or 0

            imp = entry.get("impressionData", {})
            imp_share = imp.get("asinImpressionShare", 0) or 0

            clk = entry.get("clickData", {})
            clk_share = clk.get("asinClickShare", 0) or 0

            pur = entry.get("purchaseData", {})
            pur_share = pur.get("asinPurchaseShare", 0) or 0

            print(f"{query:<45} {volume:>6} {imp_share:>5.1f}% {clk_share:>5.1f}% {pur_share:>5.1f}%")

    print("\n" + "=" * 80)
    print(f"Total: {len(report_data.get('dataByAsin', []))} search queries")
    print("=" * 80)


def wait_for_report(credentials: dict, report_id: str, max_wait: int = 3600) -> bool:
    """Wait for report to complete."""
    report = Reports(credentials=credentials, marketplace=Marketplaces.US)

    print(f"Waiting for report {report_id} to complete...")
    print("(This can take 30-60 minutes. Press Ctrl+C to cancel.)")

    start_time = time.time()
    check_interval = 30  # seconds

    while time.time() - start_time < max_wait:
        res = report.get_report(reportId=report_id)
        status = res.payload.get("processingStatus")
        doc_id = res.payload.get("reportDocumentId")

        elapsed = int(time.time() - start_time)
        print(f"  [{elapsed//60}m {elapsed%60}s] Status: {status}")

        if status == "DONE" and doc_id:
            print("\n[SUCCESS] Report ready!")
            download_and_display(report, doc_id)
            return True
        elif status == "FATAL":
            print("\n[FAILED] Report failed")
            if doc_id:
                doc_res = report.get_report_document(reportDocumentId=doc_id, download=False)
                url = doc_res.payload.get("url")
                response = requests.get(url)
                data = gzip.decompress(response.content).decode("utf-8")
                error_data = json.loads(data)
                print(f"Error: {error_data.get('errorDetails', 'Unknown error')}")
            return False
        elif status == "CANCELLED":
            print("\n[CANCELLED] Report was cancelled")
            return False

        time.sleep(check_interval)

    print(f"\n[TIMEOUT] Report did not complete within {max_wait//60} minutes")
    return False


def main() -> int:
    """Main entry point."""
    parser = create_parser()
    args = parser.parse_args()

    # Load credentials
    try:
        credentials = get_credentials()
    except Exception as e:
        print(f"[ERROR] Failed to load credentials: {e}")
        return 1

    # Test connection
    if args.test_connection:
        return 0 if test_connection(credentials) else 1

    # List reports
    if args.list:
        list_reports(credentials)
        return 0

    # Check existing report
    if args.check:
        return 0 if check_report(credentials, args.check) else 1

    # Request new report
    if args.asin:
        # Determine date range
        if args.start_date and args.end_date:
            start_date = date.fromisoformat(args.start_date)
            end_date = date.fromisoformat(args.end_date)
        else:
            start_date, end_date = get_last_complete_week()
            print(f"Using last complete week: {start_date} to {end_date}")

        # Validate start date is Sunday
        if start_date.weekday() != 6:  # 6 = Sunday
            print(f"[ERROR] Start date must be a Sunday (got {start_date.strftime('%A')})")
            return 1

        report_id = request_report(credentials, args.asin.upper(), start_date, end_date)

        if args.wait:
            return 0 if wait_for_report(credentials, report_id) else 1

        return 0

    # No action specified
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
