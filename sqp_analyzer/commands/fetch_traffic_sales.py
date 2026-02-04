#!/usr/bin/env python3
"""Fetch Sales and Traffic data from Amazon SP-API.

This command fetches Sales and Traffic Business Report data by child ASIN.

Usage:
    python -m sqp_analyzer.commands.fetch_traffic_sales --asin B0CSH12L5P
    python -m sqp_analyzer.commands.fetch_traffic_sales --check 129706020488
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
        description="Fetch Sales and Traffic data from Amazon SP-API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Request new report for an ASIN (last 7 days, daily by child ASIN)
    python -m sqp_analyzer.commands.fetch_traffic_sales --asin B0CSH12L5P

    # Request weekly granularity
    python -m sqp_analyzer.commands.fetch_traffic_sales --asin B0CSH12L5P --date-granularity WEEK

    # Check status of a pending report
    python -m sqp_analyzer.commands.fetch_traffic_sales --check 129706020488

    # List recent reports
    python -m sqp_analyzer.commands.fetch_traffic_sales --list

Note: Reports typically take 5-15 minutes to process.
        """,
    )
    parser.add_argument(
        "--asin",
        type=str,
        help="ASIN to fetch traffic/sales data for",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--date-granularity",
        type=str,
        choices=["DAY", "WEEK", "MONTH"],
        default="DAY",
        help="Date granularity (default: DAY)",
    )
    parser.add_argument(
        "--asin-granularity",
        type=str,
        choices=["PARENT", "CHILD", "SKU"],
        default="CHILD",
        help="ASIN granularity (default: CHILD)",
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
        help="List recent Sales and Traffic reports",
    )
    parser.add_argument(
        "--test-connection",
        action="store_true",
        help="Test API connection only",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Wait for report to complete",
    )
    return parser


def get_default_date_range() -> tuple[date, date]:
    """Get default date range (last 7 complete days)."""
    today = date.today()
    end_date = today - timedelta(days=1)  # Yesterday
    start_date = end_date - timedelta(days=6)  # 7 days total
    return start_date, end_date


def test_connection(credentials: dict) -> bool:
    """Test the SP-API connection."""
    print("Testing SP-API connection...")
    try:
        report = Reports(credentials=credentials, marketplace=Marketplaces.US)
        report.get_reports(
            reportTypes=["GET_MERCHANT_LISTINGS_ALL_DATA"],
            pageSize=1,
        )
        print("\n[SUCCESS] Connected to SP-API")
        return True
    except Exception as e:
        print(f"\n[FAILED] {e}")
        return False


def list_reports(credentials: dict) -> None:
    """List recent Sales and Traffic reports."""
    report = Reports(credentials=credentials, marketplace=Marketplaces.US)

    print("Recent Sales and Traffic Reports:")
    print("-" * 80)

    res = report.get_reports(
        reportTypes=["GET_SALES_AND_TRAFFIC_REPORT"],
        pageSize=10,
    )

    for r in res.payload.get("reports", []):
        status = r.get("processingStatus")
        rid = r.get("reportId")
        created = r.get("createdTime", "")[:19]
        options = r.get("reportOptions", {})
        asin_gran = options.get("asinGranularity", "N/A")
        date_gran = options.get("dateGranularity", "N/A")

        status_icon = "✓" if status == "DONE" else "✗" if status == "FATAL" else "⏳"
        print(f"{status_icon} {rid} | {status:<12} | {asin_gran}/{date_gran} | {created}")


def request_report(
    credentials: dict,
    start_date: date,
    end_date: date,
    date_granularity: str = "DAY",
    asin_granularity: str = "CHILD",
) -> str:
    """Request a new Sales and Traffic report."""
    report = Reports(credentials=credentials, marketplace=Marketplaces.US)

    print(f"Requesting Sales and Traffic report...")
    print(f"  Period: {start_date} to {end_date}")
    print(f"  Date Granularity: {date_granularity}")
    print(f"  ASIN Granularity: {asin_granularity}")

    res = report.create_report(
        reportType="GET_SALES_AND_TRAFFIC_REPORT",
        marketplaceIds=["ATVPDKIKX0DER"],
        reportOptions={
            "dateGranularity": date_granularity,
            "asinGranularity": asin_granularity,
        },
        dataStartTime=f"{start_date}T00:00:00Z",
        dataEndTime=f"{end_date}T23:59:59Z",
    )

    report_id = res.payload.get("reportId")
    print(f"\n[SUBMITTED] Report ID: {report_id}")
    print("\nNote: Reports typically take 5-15 minutes to process.")
    print(f"Check status with: python -m sqp_analyzer.commands.fetch_traffic_sales --check {report_id}")

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

    print("\n" + "=" * 100)
    print(f"Sales and Traffic Report")
    print(f"Period: {spec.get('dataStartTime', '')[:10]} to {spec.get('dataEndTime', '')[:10]}")
    print(f"Granularity: {options.get('dateGranularity', 'N/A')} / {options.get('asinGranularity', 'N/A')}")
    print("=" * 100)

    # Sales by date
    sales_by_date = report_data.get("salesAndTrafficByDate", [])
    if sales_by_date:
        print(f"\n--- Sales by Date ({len(sales_by_date)} days) ---")
        print(f"{'Date':<12} {'Units':>8} {'Sales':>12} {'Sessions':>10} {'PageViews':>10} {'BuyBox%':>8}")
        print("-" * 70)

        for entry in sales_by_date[:14]:  # Show first 2 weeks
            dt = entry.get("date", "")
            sales = entry.get("salesByDate", {})
            traffic = entry.get("trafficByDate", {})

            units = sales.get("unitsOrdered", 0)
            ordered_sales = sales.get("orderedProductSales", {})
            sales_amt = ordered_sales.get("amount", 0)
            sessions = traffic.get("sessions", 0)
            page_views = traffic.get("pageViews", 0)
            buy_box = traffic.get("buyBoxPercentage", 0)

            print(f"{dt:<12} {units:>8} ${sales_amt:>10.2f} {sessions:>10} {page_views:>10} {buy_box:>7.1f}%")

    # Sales by ASIN
    sales_by_asin = report_data.get("salesAndTrafficByAsin", [])
    if sales_by_asin:
        print(f"\n--- Sales by ASIN ({len(sales_by_asin)} products) ---")
        print(f"{'ASIN':<12} {'SKU':<20} {'Units':>8} {'Sales':>12} {'Sessions':>10} {'BuyBox%':>8}")
        print("-" * 80)

        # Sort by units ordered
        sales_by_asin.sort(
            key=lambda x: x.get("salesByAsin", {}).get("unitsOrdered", 0),
            reverse=True,
        )

        for entry in sales_by_asin[:20]:  # Show top 20
            asin = entry.get("childAsin", entry.get("parentAsin", "N/A"))
            sku = entry.get("sku", "N/A")[:18]
            sales = entry.get("salesByAsin", {})
            traffic = entry.get("trafficByAsin", {})

            units = sales.get("unitsOrdered", 0)
            ordered_sales = sales.get("orderedProductSales", {})
            sales_amt = ordered_sales.get("amount", 0)
            sessions = traffic.get("sessions", 0)
            buy_box = traffic.get("buyBoxPercentage", 0)

            print(f"{asin:<12} {sku:<20} {units:>8} ${sales_amt:>10.2f} {sessions:>10} {buy_box:>7.1f}%")

    print("\n" + "=" * 100)
    print(f"Total: {len(sales_by_date)} date entries, {len(sales_by_asin)} ASIN entries")
    print("=" * 100)


def wait_for_report(credentials: dict, report_id: str, max_wait: int = 1800) -> bool:
    """Wait for report to complete."""
    report = Reports(credentials=credentials, marketplace=Marketplaces.US)

    print(f"Waiting for report {report_id} to complete...")
    print("(Press Ctrl+C to cancel)")

    start_time = time.time()
    check_interval = 15  # seconds

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
    if args.asin or args.start_date:
        # Determine date range
        if args.start_date and args.end_date:
            start_date = date.fromisoformat(args.start_date)
            end_date = date.fromisoformat(args.end_date)
        else:
            start_date, end_date = get_default_date_range()
            print(f"Using last 7 days: {start_date} to {end_date}")

        report_id = request_report(
            credentials,
            start_date,
            end_date,
            date_granularity=args.date_granularity,
            asin_granularity=args.asin_granularity,
        )

        if args.wait:
            return 0 if wait_for_report(credentials, report_id) else 1

        return 0

    # No action specified
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
