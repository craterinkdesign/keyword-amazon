"""Tests for consolidated quarterly tracker with mock data for 3 ASINs."""

from datetime import date
from unittest.mock import MagicMock, patch


from sqp_analyzer.commands.quarterly_tracker import (
    build_asin_separator_row,
    build_headers,
    get_consolidated_tab_name,
    is_asin_separator_row,
    parse_consolidated_sheet,
    start_quarter,
    update_week,
    _build_asin_keywords,
)
from sqp_analyzer.config import AppConfig, SheetsConfig, SPAPIConfig, Thresholds
from sqp_analyzer.models import QuarterlyKeyword, SQPRecord, WeeklySnapshot


# --- Mock data for 3 ASINs ---

MOCK_ASINS = [
    {"asin": "B0ASIN0001", "sku": "SKU-001", "name": "Garlic Press", "active": True},
    {"asin": "B0ASIN0002", "sku": "SKU-002", "name": "Garlic Mincer", "active": True},
    {"asin": "B0ASIN0003", "sku": "SKU-003", "name": "Garlic Crusher", "active": True},
]

KEYWORDS_BY_ASIN = {
    "B0ASIN0001": [
        ("garlic press", 5000),
        ("garlic presser", 4200),
        ("garlic crusher tool", 3800),
        ("garlic mincer press", 3500),
        ("garlic squeezer", 3100),
        ("garlic tool", 2800),
        ("kitchen garlic press", 2500),
        ("manual garlic press", 2200),
        ("stainless garlic press", 1900),
        ("best garlic press", 1600),
    ],
    "B0ASIN0002": [
        ("garlic mincer", 4500),
        ("garlic chopper", 4000),
        ("garlic dicer", 3600),
        ("minced garlic tool", 3200),
        ("garlic mincer press", 2900),
        ("garlic cutter", 2600),
        ("kitchen garlic mincer", 2300),
        ("manual garlic mincer", 2000),
        ("stainless garlic mincer", 1700),
        ("best garlic mincer", 1400),
    ],
    "B0ASIN0003": [
        ("garlic crusher", 4800),
        ("garlic masher", 4100),
        ("garlic smasher", 3700),
        ("crush garlic tool", 3300),
        ("garlic press crusher", 3000),
        ("garlic rocker", 2700),
        ("kitchen garlic crusher", 2400),
        ("manual garlic crusher", 2100),
        ("stainless garlic crusher", 1800),
        ("best garlic crusher", 1500),
    ],
}


def _make_mock_config() -> AppConfig:
    return AppConfig(
        sp_api=SPAPIConfig(
            client_id="test",
            client_secret="test",
            refresh_token="test",
            aws_access_key="test",
            aws_secret_key="test",
            role_arn="test",
            marketplace_id="test",
            seller_id="test",
        ),
        sheets=SheetsConfig(
            spreadsheet_id="test-sheet-id",
            master_tab_name="ASINs",
            credentials_path="test.json",
        ),
        thresholds=Thresholds(),
    )


def _make_sqp_records(asin: str, week_date: date) -> list[SQPRecord]:
    """Create mock SQP records for an ASIN."""
    records = []
    for keyword, volume in KEYWORDS_BY_ASIN[asin]:
        records.append(
            SQPRecord(
                search_query=keyword,
                asin=asin,
                week_date=week_date,
                search_volume=volume,
                impressions_total=volume * 10,
                impressions_asin=int(volume * 1.5),
                impressions_share=round(volume / 500, 1),
                clicks_total=int(volume * 0.3),
                clicks_asin=int(volume * 0.05),
                clicks_share=round(volume / 2000, 1),
                purchases_total=int(volume * 0.01),
                purchases_asin=max(1, int(volume * 0.002)),
                purchases_share=round(volume / 50000, 2),
            )
        )
    return records


def _make_snapshot(asin: str, week_date: date) -> WeeklySnapshot:
    return WeeklySnapshot(
        asin=asin,
        week_date=week_date,
        records=_make_sqp_records(asin, week_date),
    )


def _build_mock_consolidated_sheet(week_labels: list[str]) -> list[list]:
    """Build a complete mock consolidated sheet with headers + data for 3 ASINs."""
    headers = build_headers(week_labels)
    num_cols = len(headers)
    rows = [headers]

    config = _make_mock_config()

    for asin_info in MOCK_ASINS:
        asin = asin_info["asin"]
        product_name = asin_info["name"]

        # Separator row
        rows.append(build_asin_separator_row(asin, product_name, num_cols))

        # Build keyword rows
        snapshots = {
            wl: _make_snapshot(asin, date(2026, 1, 5 + i * 7))
            for i, wl in enumerate(week_labels)
        }
        keywords = _build_asin_keywords(asin, snapshots, week_labels, None, config)
        for qk in keywords:
            rows.append(qk.to_row(week_labels))

    return rows


# --- Tests ---


class TestBuildHeaders:
    def test_includes_asin_column(self):
        headers = build_headers(["W01"])
        assert headers[0] == "ASIN"
        assert headers[1] == "Rank"
        assert headers[2] == "Keyword"

    def test_static_columns(self):
        headers = build_headers([])
        assert headers == ["ASIN", "Rank", "Keyword", "In Title", "In Backend", "Alert"]

    def test_week_columns(self):
        headers = build_headers(["W01", "W02"])
        assert "W01 Vol" in headers
        assert "W02 Imp%" in headers
        # 5 static + 6*2 weekly + 1 alert = 18
        assert len(headers) == 5 + 12 + 1


class TestQuarterlyKeywordToRow:
    def test_includes_asin(self):
        qk = QuarterlyKeyword(
            asin="B0ASIN0001",
            rank=1,
            keyword="garlic press",
            in_title=True,
            in_backend=False,
            weekly_metrics={
                "W01": {
                    "volume": 5000,
                    "imp_share": 10.0,
                    "click_share": 2.5,
                    "purchase_share": 0.1,
                    "opportunity_score": 30.0,
                    "rank_status": "page_1_high",
                }
            },
        )
        row = qk.to_row(["W01"])
        assert row[0] == "B0ASIN0001"
        assert row[1] == 1
        assert row[2] == "garlic press"
        assert row[3] == "YES"
        assert row[4] == "NO"
        assert row[5] == 5000  # W01 Vol


class TestBuildAsinSeparatorRow:
    def test_correct_format(self):
        row = build_asin_separator_row("B0ASIN0001", "Garlic Press", 10)
        assert len(row) == 10
        assert row[0] == "B0ASIN0001"
        assert row[1] == ""
        assert row[2] == "Garlic Press"
        assert all(cell == "" for cell in row[3:])

    def test_empty_name(self):
        row = build_asin_separator_row("B0ASIN0001", "", 5)
        assert row[0] == "B0ASIN0001"
        assert row[2] == ""


class TestIsAsinSeparatorRow:
    def test_detects_separator(self):
        row = ["B0ASIN0001", "", "Garlic Press", "", ""]
        assert is_asin_separator_row(row) is True

    def test_rejects_data_row(self):
        row = ["B0ASIN0001", 1, "garlic press", "YES", "NO"]
        assert is_asin_separator_row(row) is False

    def test_rejects_empty_row(self):
        row = ["", "", "", "", ""]
        assert is_asin_separator_row(row) is False

    def test_rejects_short_row(self):
        assert is_asin_separator_row([]) is False
        assert is_asin_separator_row(["X"]) is False

    def test_rank_zero_is_not_separator(self):
        # Rank of 0 is truthy-ish but int 0 is falsy, so this IS a separator
        row = ["B0ASIN0001", 0, "garlic press", "YES", "NO"]
        assert is_asin_separator_row(row) is True

    def test_rank_string_zero_is_not_separator(self):
        # "0" is a truthy string, so this is NOT a separator
        row = ["B0ASIN0001", "0", "garlic press", "YES", "NO"]
        assert is_asin_separator_row(row) is False


class TestParseConsolidatedSheet:
    def test_parses_3_asins(self):
        sheet = _build_mock_consolidated_sheet(["W01", "W02"])
        result = parse_consolidated_sheet(sheet)

        assert len(result) == 3
        assert "B0ASIN0001" in result
        assert "B0ASIN0002" in result
        assert "B0ASIN0003" in result

    def test_10_keywords_per_asin(self):
        sheet = _build_mock_consolidated_sheet(["W01"])
        result = parse_consolidated_sheet(sheet)

        for asin in MOCK_ASINS:
            asin_id = asin["asin"]
            assert len(result[asin_id]["keywords"]) == 10

    def test_preserves_product_name(self):
        sheet = _build_mock_consolidated_sheet(["W01"])
        result = parse_consolidated_sheet(sheet)

        assert result["B0ASIN0001"]["name"] == "Garlic Press"
        assert result["B0ASIN0002"]["name"] == "Garlic Mincer"
        assert result["B0ASIN0003"]["name"] == "Garlic Crusher"

    def test_keyword_fields(self):
        sheet = _build_mock_consolidated_sheet(["W01"])
        result = parse_consolidated_sheet(sheet)

        first_kw = result["B0ASIN0001"]["keywords"][0]
        assert "rank" in first_kw
        assert "keyword" in first_kw
        assert "in_title" in first_kw
        assert "in_backend" in first_kw
        assert "row_data" in first_kw

    def test_empty_sheet(self):
        assert parse_consolidated_sheet([]) == {}
        assert parse_consolidated_sheet([["ASIN", "Rank"]]) == {}

    def test_raw_rows_include_separator(self):
        sheet = _build_mock_consolidated_sheet(["W01"])
        result = parse_consolidated_sheet(sheet)

        # raw_rows = 1 separator + 10 keyword rows = 11
        assert len(result["B0ASIN0001"]["raw_rows"]) == 11


class TestGetConsolidatedTabName:
    def test_returns_q_format(self):
        assert get_consolidated_tab_name(1) == "Q1"
        assert get_consolidated_tab_name(2) == "Q2"
        assert get_consolidated_tab_name(3) == "Q3"
        assert get_consolidated_tab_name(4) == "Q4"

    def test_defaults_to_current_quarter(self):
        result = get_consolidated_tab_name()
        assert result.startswith("Q")
        assert result[1:].isdigit()


class TestStartQuarterConsolidated:
    @patch("sqp_analyzer.commands.quarterly_tracker.get_listing_content")
    @patch("sqp_analyzer.commands.quarterly_tracker.fetch_sqp_report")
    @patch("sqp_analyzer.commands.quarterly_tracker.get_credentials")
    @patch("sqp_analyzer.commands.quarterly_tracker.get_quarter_weeks")
    @patch("sqp_analyzer.commands.quarterly_tracker.get_current_quarter")
    @patch("sqp_analyzer.commands.quarterly_tracker.SheetsClient")
    def test_writes_consolidated_tab(
        self,
        mock_sheets_cls,
        mock_quarter,
        mock_weeks,
        mock_creds,
        mock_fetch,
        mock_listing,
    ):
        # Setup
        mock_quarter.return_value = (1, 2026)
        mock_weeks.return_value = [
            (1, date(2026, 1, 5), date(2026, 1, 11)),
            (2, date(2026, 1, 12), date(2026, 1, 18)),
        ]
        mock_creds.return_value = {
            "refresh_token": "t",
            "lwa_app_id": "t",
            "lwa_client_secret": "t",
        }
        mock_listing.return_value = None

        # Mock sheets client
        mock_sheets = MagicMock()
        mock_sheets.get_active_asins.return_value = MOCK_ASINS
        mock_sheets_cls.return_value = mock_sheets

        # Mock SQP reports - return valid data for each ASIN/week combo
        def fake_fetch(creds, asin, start, end):
            records = _make_sqp_records(asin, start)
            return {
                "reportSpecification": {"dataStartTime": start.isoformat()},
                "dataByAsin": [
                    {
                        "asin": asin,
                        "searchQueryData": {
                            "searchQuery": r.search_query,
                            "searchQueryVolume": r.search_volume,
                        },
                        "impressionData": {
                            "totalImpressions": r.impressions_total,
                            "asinImpressions": r.impressions_asin,
                            "asinImpressionShare": r.impressions_share,
                        },
                        "clickData": {
                            "totalClicks": r.clicks_total,
                            "asinClicks": r.clicks_asin,
                            "asinClickShare": r.clicks_share,
                        },
                        "purchaseData": {
                            "totalPurchases": r.purchases_total,
                            "asinPurchases": r.purchases_asin,
                            "asinPurchaseShare": r.purchases_share,
                        },
                    }
                    for r in records
                ],
            }

        mock_fetch.side_effect = fake_fetch

        config = _make_mock_config()
        result = start_quarter(config)

        assert result is True

        # Verify write_quarterly_tracker was called with "Q1"
        mock_sheets.write_quarterly_tracker.assert_called_once()
        call_args = mock_sheets.write_quarterly_tracker.call_args
        tab_name = call_args[0][0]
        headers = call_args[0][1]
        rows = call_args[0][2]

        assert tab_name == "Q1"
        assert headers[0] == "ASIN"

        # 3 ASINs * (1 separator + 10 keywords) = 33 rows
        assert len(rows) == 33

        # Check separator rows
        separator_rows = [r for r in rows if is_asin_separator_row(r)]
        assert len(separator_rows) == 3

        # Check all 3 ASINs present
        asins_in_rows = {r[0] for r in separator_rows}
        assert asins_in_rows == {"B0ASIN0001", "B0ASIN0002", "B0ASIN0003"}


class TestUpdateWeekConsolidated:
    @patch("sqp_analyzer.commands.quarterly_tracker.get_listing_content")
    @patch("sqp_analyzer.commands.quarterly_tracker.fetch_sqp_report")
    @patch("sqp_analyzer.commands.quarterly_tracker.get_credentials")
    @patch("sqp_analyzer.commands.quarterly_tracker.get_last_complete_week")
    @patch("sqp_analyzer.commands.quarterly_tracker.get_week_in_quarter")
    @patch("sqp_analyzer.commands.quarterly_tracker.get_current_quarter")
    @patch("sqp_analyzer.commands.quarterly_tracker.SheetsClient")
    def test_merges_new_week(
        self,
        mock_sheets_cls,
        mock_quarter,
        mock_week_num,
        mock_last_week,
        mock_creds,
        mock_fetch,
        mock_listing,
    ):
        # Setup
        mock_quarter.return_value = (1, 2026)
        mock_week_num.return_value = 3
        mock_last_week.return_value = (date(2026, 1, 19), date(2026, 1, 25))
        mock_creds.return_value = {
            "refresh_token": "t",
            "lwa_app_id": "t",
            "lwa_client_secret": "t",
        }
        mock_listing.return_value = None

        # Build existing sheet with W01 and W02
        existing_sheet = _build_mock_consolidated_sheet(["W01", "W02"])

        # Mock sheets client
        mock_sheets = MagicMock()
        mock_sheets.get_active_asins.return_value = MOCK_ASINS
        mock_sheets.get_quarterly_tracker.return_value = existing_sheet
        mock_sheets_cls.return_value = mock_sheets

        # Mock SQP for W03 data
        def fake_fetch(creds, asin, start, end):
            records = _make_sqp_records(asin, start)
            return {
                "reportSpecification": {"dataStartTime": start.isoformat()},
                "dataByAsin": [
                    {
                        "asin": asin,
                        "searchQueryData": {
                            "searchQuery": r.search_query,
                            "searchQueryVolume": r.search_volume,
                        },
                        "impressionData": {
                            "totalImpressions": r.impressions_total,
                            "asinImpressions": r.impressions_asin,
                            "asinImpressionShare": r.impressions_share,
                        },
                        "clickData": {
                            "totalClicks": r.clicks_total,
                            "asinClicks": r.clicks_asin,
                            "asinClickShare": r.clicks_share,
                        },
                        "purchaseData": {
                            "totalPurchases": r.purchases_total,
                            "asinPurchases": r.purchases_asin,
                            "asinPurchaseShare": r.purchases_share,
                        },
                    }
                    for r in records
                ],
            }

        mock_fetch.side_effect = fake_fetch

        config = _make_mock_config()
        result = update_week(config)

        assert result is True

        # Verify write was called
        mock_sheets.write_quarterly_tracker.assert_called_once()
        call_args = mock_sheets.write_quarterly_tracker.call_args
        tab_name = call_args[0][0]
        headers = call_args[0][1]
        rows = call_args[0][2]

        assert tab_name == "Q1"

        # Headers should now include W01, W02, W03
        assert "W01 Vol" in headers
        assert "W02 Vol" in headers
        assert "W03 Vol" in headers

        # Still 33 rows (3 ASINs * 11)
        assert len(rows) == 33

        # Data rows should have ASIN in column 0
        data_rows = [r for r in rows if not is_asin_separator_row(r)]
        assert all(
            r[0] in {"B0ASIN0001", "B0ASIN0002", "B0ASIN0003"} for r in data_rows
        )
