"""Google Sheets client for reading and writing quarterly tracker data."""

from typing import Any

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
        - Brand, Product Name, Sheet Name, ASIN, Variation ASIN, SKU, Status

        Returns list of dicts with keys: asin, variation_asin, sku, active, name, brand, sheet_name
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
                "sku": str(normalized.get("sku", "")).strip(),
                "active": active,
                "name": normalized.get("product_name", normalized.get("name", "")),
                "brand": normalized.get("brand", ""),
                "sheet_name": normalized.get("sheet_name", ""),
            })

        return asins

    def get_active_asins(self) -> list[dict[str, Any]]:
        """Get list of active ASINs with their metadata."""
        asins = self.read_asins()
        return [a for a in asins if a.get("active", True)]

    def _get_or_create_worksheet(
        self, name: str, rows: int = 1000, cols: int = 100
    ) -> gspread.Worksheet:
        """Get existing worksheet or create new one."""
        spreadsheet = self._get_spreadsheet()
        try:
            return spreadsheet.worksheet(name)
        except gspread.WorksheetNotFound:
            return spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)

    def get_quarterly_tracker(self, tab_name: str) -> list[list[Any]] | None:
        """Read existing quarterly tracker data.

        Args:
            tab_name: Tab name (e.g., 'Q1-B0CSH12L5P')

        Returns:
            All values from the tab, or None if tab doesn't exist
        """
        spreadsheet = self._get_spreadsheet()
        try:
            worksheet = spreadsheet.worksheet(tab_name)
            return worksheet.get_all_values()
        except gspread.WorksheetNotFound:
            return None

    def write_quarterly_tracker(
        self,
        tab_name: str,
        headers: list[str],
        rows: list[list[Any]],
    ) -> None:
        """Write quarterly tracker data to a tab.

        Args:
            tab_name: Tab name (e.g., 'Q1-B0CSH12L5P')
            headers: Header row
            rows: Data rows
        """
        # Calculate needed columns (headers + some buffer)
        num_cols = max(len(headers), 100)

        worksheet = self._get_or_create_worksheet(
            tab_name, rows=len(rows) + 50, cols=num_cols
        )
        worksheet.clear()

        all_rows = [headers] + rows
        worksheet.update(values=all_rows, range_name="A1")

    def update_quarterly_tracker_row(
        self,
        tab_name: str,
        row_num: int,
        values: list[Any],
        start_col: int = 1,
    ) -> None:
        """Update a specific row in the quarterly tracker.

        Args:
            tab_name: Tab name
            row_num: Row number (1-indexed)
            values: Values to write
            start_col: Starting column (1-indexed, default A)
        """
        spreadsheet = self._get_spreadsheet()
        worksheet = spreadsheet.worksheet(tab_name)

        # Convert column number to letter
        end_col = start_col + len(values) - 1

        def col_to_letter(col: int) -> str:
            result = ""
            while col > 0:
                col, remainder = divmod(col - 1, 26)
                result = chr(65 + remainder) + result
            return result

        start_letter = col_to_letter(start_col)
        end_letter = col_to_letter(end_col)

        range_name = f"{start_letter}{row_num}:{end_letter}{row_num}"
        worksheet.update(values=[values], range_name=range_name)

    def test_connection(self) -> bool:
        """Test connection to Google Sheets."""
        try:
            spreadsheet = self._get_spreadsheet()
            _ = spreadsheet.title
            return True
        except Exception:
            return False
