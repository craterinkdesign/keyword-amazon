#!/usr/bin/env python3
"""Analyze Traffic and Sales data and write to Google Sheets.

Usage:
    python -m sqp_analyzer.commands.analyze_traffic_sales --report-id 129717020488
"""

import argparse
import gzip
import json
import sys
from datetime import date

import requests
from sp_api.api import Reports
from sp_api.base import Marketplaces

from ..config import load_config
from ..sheets.client import SheetsClient


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser."""
    parser = argparse.ArgumentParser(
        description="Analyze Traffic and Sales data and write to Google Sheets",
    )
    parser.add_argument(
        "--report-id",
        type=str,
        required=True,
        help="Completed report ID to analyze",
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

    res = report.get_report(reportId=report_id)
    status = res.payload.get("processingStatus")
    doc_id = res.payload.get("reportDocumentId")

    if status != "DONE":
        print(f"Report {report_id} is not ready: {status}")
        return None

    if not doc_id:
        print(f"Report {report_id} has no document ID")
        return None

    doc_res = report.get_report_document(reportDocumentId=doc_id, download=False)
    url = doc_res.payload.get("url")

    response = requests.get(url)
    if doc_res.payload.get("compressionAlgorithm") == "GZIP":
        data = gzip.decompress(response.content).decode("utf-8")
    else:
        data = response.text

    return json.loads(data)


def write_to_sheets(config, report_data: dict) -> None:
    """Write traffic and sales data to Google Sheets."""
    sheets = SheetsClient(config.sheets)
    spreadsheet = sheets._get_spreadsheet()

    # Get or create worksheets
    def get_or_create_worksheet(name: str, rows: int = 1000, cols: int = 20):
        try:
            return spreadsheet.worksheet(name)
        except Exception:
            return spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)

    # Write Sales by Date
    sales_by_date = report_data.get("salesAndTrafficByDate", [])
    if sales_by_date:
        ws = get_or_create_worksheet("Traffic-ByDate")
        ws.clear()

        headers = [
            "Date",
            "Units Ordered",
            "Sales ($)",
            "Units Shipped",
            "Orders Shipped",
            "Sessions",
            "Page Views",
            "Buy Box %",
            "Unit Session %",
            "Order Item Session %",
        ]

        rows = [headers]
        for entry in sales_by_date:
            dt = entry.get("date", "")
            sales = entry.get("salesByDate", {})
            traffic = entry.get("trafficByDate", {})

            ordered_sales = sales.get("orderedProductSales", {})

            rows.append([
                dt,
                sales.get("unitsOrdered", 0),
                ordered_sales.get("amount", 0),
                sales.get("unitsShipped", 0),
                sales.get("ordersShipped", 0),
                traffic.get("sessions", 0),
                traffic.get("pageViews", 0),
                traffic.get("buyBoxPercentage", 0),
                traffic.get("unitSessionPercentage", 0),
                traffic.get("orderItemSessionPercentage", 0),
            ])

        ws.update("A1", rows)
        print(f"  Wrote {len(sales_by_date)} rows to Traffic-ByDate")

    # Write Sales by ASIN
    sales_by_asin = report_data.get("salesAndTrafficByAsin", [])
    if sales_by_asin:
        ws = get_or_create_worksheet("Traffic-ByASIN")
        ws.clear()

        headers = [
            "ASIN",
            "Parent ASIN",
            "SKU",
            "Units Ordered",
            "Sales ($)",
            "Units Shipped",
            "Sessions",
            "Page Views",
            "Buy Box %",
            "Unit Session %",
        ]

        # Sort by units ordered
        sales_by_asin.sort(
            key=lambda x: x.get("salesByAsin", {}).get("unitsOrdered", 0),
            reverse=True,
        )

        rows = [headers]
        for entry in sales_by_asin:
            child_asin = entry.get("childAsin", "")
            parent_asin = entry.get("parentAsin", "")
            sku = entry.get("sku", "")
            sales = entry.get("salesByAsin", {})
            traffic = entry.get("trafficByAsin", {})

            ordered_sales = sales.get("orderedProductSales", {})

            rows.append([
                child_asin or parent_asin,
                parent_asin,
                sku,
                sales.get("unitsOrdered", 0),
                ordered_sales.get("amount", 0),
                sales.get("unitsShipped", 0),
                traffic.get("sessions", 0),
                traffic.get("pageViews", 0),
                traffic.get("buyBoxPercentage", 0),
                traffic.get("unitSessionPercentage", 0),
            ])

        ws.update("A1", rows)
        print(f"  Wrote {len(sales_by_asin)} rows to Traffic-ByASIN")


def main() -> int:
    """Main entry point."""
    parser = create_parser()
    args = parser.parse_args()

    config = load_config()

    print(f"Fetching report {args.report_id}...")
    credentials = get_credentials()
    report_data = fetch_report_data(credentials, args.report_id)

    if not report_data:
        return 1

    if "errorDetails" in report_data:
        print(f"Report error: {report_data['errorDetails']}")
        return 1

    spec = report_data.get("reportSpecification", {})
    print(f"Period: {spec.get('dataStartTime', '')[:10]} to {spec.get('dataEndTime', '')[:10]}")

    print("\nWriting to Google Sheets...")
    write_to_sheets(config, report_data)

    print(f"\n[SUCCESS] Data written to Google Sheets")
    print(f"View: https://docs.google.com/spreadsheets/d/{config.sheets.spreadsheet_id}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
