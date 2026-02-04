"""Google Sheets client for reading and writing SQP data."""

from typing import Any
from datetime import date

import gspread
from google.oauth2.service_account import Credentials

from ..config import SheetsConfig


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class SheetsClient:
    """Client for Google Sheets operations."""

    def __init__(self, config: SheetsConfig):
        self.config = config
        self._client: gspread.Client | None = None
        self._spreadsheet: gspread.Spreadsheet | None = None

    def _get_client(self) -> gspread.Client:
        """Get or create authenticated gspread client."""
        if self._client is None:
            credentials = Credentials.from_service_account_file(
                self.config.credentials_path,
                scopes=SCOPES,
            )
            self._client = gspread.authorize(credentials)
        return self._client

    def _get_spreadsheet(self) -> gspread.Spreadsheet:
        """Get or open the spreadsheet."""
        if self._spreadsheet is None:
            client = self._get_client()
            self._spreadsheet = client.open_by_key(self.config.spreadsheet_id)
        return self._spreadsheet

    def read_asins(self) -> list[dict[str, Any]]:
        """Read parent ASINs from the master tab.

        Supports columns:
        - Brand, Product Name, Sheet Name, ASIN, Variation ASIN, Status

        Returns list of dicts with keys: asin, variation_asin, active, name, brand, sheet_name
        """
        spreadsheet = self._get_spreadsheet()
        worksheet = spreadsheet.worksheet(self.config.master_tab_name)
        records = worksheet.get_all_records()

        asins = []
        for record in records:
            # Normalize column names (case-insensitive, replace spaces with underscores)
            normalized = {
                k.lower().strip().replace(" ", "_"): v
                for k, v in record.items()
            }

            asin = normalized.get("asin") or normalized.get("parent_asin", "")
            if not asin:
                continue

            # Check if active - support "Status" column with "Active" value
            # or "Active" column with TRUE/YES/1/Y
            status = normalized.get("status", "")
            active_col = normalized.get("active", "")

            if status:
                active = str(status).upper() == "ACTIVE"
            elif active_col:
                active = str(active_col).upper() in ("TRUE", "YES", "1", "Y", "ACTIVE")
            else:
                active = True  # Default to active if no status column

            asins.append({
                "asin": str(asin).strip().upper(),
                "variation_asin": str(normalized.get("variation_asin", "")).strip().upper(),
                "active": active,
                "name": normalized.get("product_name", normalized.get("name", "")),
                "brand": normalized.get("brand", ""),
                "sheet_name": normalized.get("sheet_name", ""),
            })

        return asins

    def get_active_asins(self) -> list[str]:
        """Get list of active parent ASINs."""
        asins = self.read_asins()
        return [a["asin"] for a in asins if a.get("active", True)]

    def _get_or_create_worksheet(
        self, name: str, rows: int = 1000, cols: int = 20
    ) -> gspread.Worksheet:
        """Get existing worksheet or create new one."""
        spreadsheet = self._get_spreadsheet()
        try:
            return spreadsheet.worksheet(name)
        except gspread.WorksheetNotFound:
            return spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)

    def write_weekly_data(
        self,
        week_date: date,
        data: list[dict[str, Any]],
        headers: list[str] | None = None,
    ) -> None:
        """Write weekly SQP data to a tab.

        Creates tab named SQP-YYYY-WW (e.g., SQP-2025-05).
        """
        year, week_num, _ = week_date.isocalendar()
        tab_name = f"SQP-{year}-{week_num:02d}"

        if headers is None:
            headers = [
                "Search Query",
                "Volume",
                "Score",
                "Imp Total",
                "Imp ASIN",
                "Imp Share",
                "Click Total",
                "Click ASIN",
                "Click Share",
                "Purchase Total",
                "Purchase ASIN",
                "Purchase Share",
                "ASIN Price",
                "Market Price",
            ]

        worksheet = self._get_or_create_worksheet(tab_name, rows=len(data) + 100)

        # Clear existing content and write new
        worksheet.clear()

        if not data:
            worksheet.update("A1", [headers])
            return

        # Build rows
        rows = [headers]
        for record in data:
            row = [record.get(h, "") for h in headers]
            rows.append(row)

        worksheet.update("A1", rows)

    def write_categorized_keywords(
        self,
        tab_name: str,
        keywords: list[dict[str, Any]],
        headers: list[str],
    ) -> None:
        """Write categorized keywords to a specific tab."""
        worksheet = self._get_or_create_worksheet(tab_name, rows=len(keywords) + 100)
        worksheet.clear()

        if not keywords:
            worksheet.update("A1", [headers])
            return

        rows = [headers]
        for kw in keywords:
            row = [kw.get(h, "") for h in headers]
            rows.append(row)

        worksheet.update("A1", rows)

    def write_summary(self, summary_data: list[dict[str, Any]]) -> None:
        """Write summary dashboard to SQP-Summary tab."""
        headers = [
            "ASIN",
            "Product Name",
            "Total Keywords",
            "Bread & Butter",
            "Opportunities",
            "Leaks",
            "Price Flagged",
            "Health Score",
            "Last Updated",
        ]

        worksheet = self._get_or_create_worksheet("SQP-Summary")
        worksheet.clear()

        rows = [headers]
        for record in summary_data:
            rows.append([record.get(h, "") for h in headers])

        worksheet.update("A1", rows)

    def write_trends(self, trends: list[dict[str, Any]]) -> None:
        """Write 12-week trend data to SQP-Trends tab."""
        # Dynamic headers based on weeks available
        base_headers = ["Search Query", "ASIN"]
        week_headers = []

        if trends:
            # Extract week columns from first record
            sample = trends[0]
            week_headers = [k for k in sample.keys() if k.startswith("Week ")]
            week_headers.sort()

        headers = base_headers + week_headers + ["Trend Direction", "Growth %"]

        worksheet = self._get_or_create_worksheet("SQP-Trends", rows=len(trends) + 100)
        worksheet.clear()

        rows = [headers]
        for record in trends:
            row = [record.get(h, "") for h in headers]
            rows.append(row)

        worksheet.update("A1", rows)

    def write_price_flags(self, flags: list[dict[str, Any]]) -> None:
        """Write price competitiveness flags to SQP-PriceFlags tab."""
        headers = [
            "Search Query",
            "ASIN",
            "ASIN Price",
            "Market Price",
            "Price Diff %",
            "Severity",
            "Imp Share",
            "Purchase Share",
        ]

        worksheet = self._get_or_create_worksheet("SQP-PriceFlags")
        worksheet.clear()

        rows = [headers]
        for record in flags:
            rows.append([record.get(h, "") for h in headers])

        worksheet.update("A1", rows)

    def write_diagnostics(self, diagnostics: list[dict[str, Any]]) -> None:
        """Write keyword diagnostics to SQP-Diagnostics tab."""
        headers = [
            "Search Query",
            "ASIN",
            "Diagnostic",
            "Rank Status",
            "Opportunity Score",
            "Volume",
            "Imp Share",
            "Click Share",
            "Purchase Share",
            "Recommended Fix",
        ]

        worksheet = self._get_or_create_worksheet(
            "SQP-Diagnostics", rows=len(diagnostics) + 100
        )
        worksheet.clear()

        rows = [headers]
        for record in diagnostics:
            rows.append([record.get(h, "") for h in headers])

        worksheet.update("A1", rows)

    def write_placements(self, placements: list[dict[str, Any]]) -> None:
        """Write keyword placement recommendations to SQP-Placements tab."""
        headers = [
            "Search Query",
            "ASIN",
            "Placement",
            "Priority",
            "Volume",
            "Click Share",
            "Reasoning",
        ]

        worksheet = self._get_or_create_worksheet(
            "SQP-Placements", rows=len(placements) + 100
        )
        worksheet.clear()

        rows = [headers]
        for record in placements:
            rows.append([record.get(h, "") for h in headers])

        worksheet.update("A1", rows)

    def write_opportunity_ranking(self, opportunities: list[dict[str, Any]]) -> None:
        """Write top opportunities to SQP-TopOpportunities tab."""
        headers = [
            "Rank",
            "Search Query",
            "ASIN",
            "Opportunity Score",
            "Volume",
            "Imp Share",
            "Diagnostic",
            "Recommended Fix",
        ]

        worksheet = self._get_or_create_worksheet(
            "SQP-TopOpportunities", rows=len(opportunities) + 100
        )
        worksheet.clear()

        rows = [headers]
        for i, record in enumerate(opportunities, 1):
            row = [
                i,  # Rank
                record.get("Search Query", ""),
                record.get("ASIN", ""),
                record.get("Opportunity Score", ""),
                record.get("Volume", ""),
                record.get("Imp Share", ""),
                record.get("Diagnostic", ""),
                record.get("Recommended Fix", ""),
            ]
            rows.append(row)

        worksheet.update("A1", rows)

    def test_connection(self) -> bool:
        """Test connection to Google Sheets."""
        try:
            spreadsheet = self._get_spreadsheet()
            _ = spreadsheet.title
            return True
        except Exception:
            return False
