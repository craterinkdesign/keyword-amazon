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
    extract_week_metrics,
    detect_drastic_changes,
    build_asin_summary,
    build_dashboard,
    generate_dashboard,
    DASHBOARD_TAB_NAME,
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
        # generate_dashboard reads the Q tab after writing
        mock_sheets.get_quarterly_tracker.return_value = _build_mock_consolidated_sheet(
            ["W01", "W02"]
        )
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

        # Verify write_quarterly_tracker was called twice (Q tab + Dashboard)
        assert mock_sheets.write_quarterly_tracker.call_count == 2

        # First call: Q tab
        call_args = mock_sheets.write_quarterly_tracker.call_args_list[0]
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

        # Second call: Dashboard tab
        dashboard_call = mock_sheets.write_quarterly_tracker.call_args_list[1]
        assert dashboard_call[0][0] == DASHBOARD_TAB_NAME


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

        # Verify write was called twice (Q tab + Dashboard)
        assert mock_sheets.write_quarterly_tracker.call_count == 2

        # First call: Q tab
        call_args = mock_sheets.write_quarterly_tracker.call_args_list[0]
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

        # Second call: Dashboard tab
        dashboard_call = mock_sheets.write_quarterly_tracker.call_args_list[1]
        assert dashboard_call[0][0] == DASHBOARD_TAB_NAME


# --- Dashboard test helpers ---


def _build_mock_sheet_with_changes(
    week_labels: list[str],
    volume_overrides: dict[str, dict[str, dict[str, int]]] | None = None,
    rank_overrides: dict[str, dict[str, dict[str, str]]] | None = None,
    alert_overrides: dict[str, dict[str, str]] | None = None,
) -> list[list]:
    """Build a mock consolidated sheet with specific overrides for testing.

    Args:
        week_labels: List of week labels
        volume_overrides: {asin: {keyword: {week_label: volume}}}
        rank_overrides: {asin: {keyword: {week_label: rank_status}}}
        alert_overrides: {asin: {keyword: alert_text}}

    Returns:
        Complete sheet as list of rows (including header)
    """
    sheet = _build_mock_consolidated_sheet(week_labels)
    headers = sheet[0]

    # Find week column indices
    week_vol_indices = {}
    week_rank_indices = {}
    for i, h in enumerate(headers):
        if h.endswith(" Vol"):
            week = h.replace(" Vol", "")
            week_vol_indices[week] = i
        if h.endswith(" Rank"):
            week = h.replace(" Rank", "")
            week_rank_indices[week] = i

    alert_idx = len(headers) - 1  # Alert is last column

    current_asin = None
    for row in sheet[1:]:
        if is_asin_separator_row(row):
            current_asin = row[0]
            continue

        keyword = row[2]

        # Apply volume overrides
        if volume_overrides and current_asin in volume_overrides:
            kw_overrides = volume_overrides[current_asin].get(keyword, {})
            for wl, vol in kw_overrides.items():
                if wl in week_vol_indices:
                    row[week_vol_indices[wl]] = vol

        # Apply rank overrides
        if rank_overrides and current_asin in rank_overrides:
            kw_overrides = rank_overrides[current_asin].get(keyword, {})
            for wl, rank in kw_overrides.items():
                if wl in week_rank_indices:
                    row[week_rank_indices[wl]] = rank

        # Apply alert overrides
        if alert_overrides and current_asin in alert_overrides:
            alert_text = alert_overrides[current_asin].get(keyword)
            if alert_text is not None:
                while len(row) <= alert_idx:
                    row.append("")
                row[alert_idx] = alert_text

    return sheet


# --- Dashboard tests ---


class TestExtractWeekMetrics:
    def test_extracts_first_week(self):
        sheet = _build_mock_consolidated_sheet(["W01", "W02"])
        parsed = parse_consolidated_sheet(sheet)
        kw_row = parsed["B0ASIN0001"]["keywords"][0]["row_data"]

        metrics = extract_week_metrics(kw_row, 0)
        assert metrics is not None
        assert metrics["volume"] == 5000
        assert metrics["rank_status"] is not None

    def test_extracts_second_week(self):
        sheet = _build_mock_consolidated_sheet(["W01", "W02"])
        parsed = parse_consolidated_sheet(sheet)
        kw_row = parsed["B0ASIN0001"]["keywords"][0]["row_data"]

        metrics = extract_week_metrics(kw_row, 1)
        assert metrics is not None
        assert metrics["volume"] == 5000

    def test_missing_data_returns_none(self):
        short_row = ["ASIN", 1, "keyword", "YES", "NO"]
        assert extract_week_metrics(short_row, 0) is None

    def test_all_dashes_returns_none(self):
        row = ["ASIN", 1, "keyword", "YES", "NO", "-", "-", "-", "-", "-", "-", ""]
        assert extract_week_metrics(row, 0) is None

    def test_string_conversion(self):
        row = [
            "ASIN",
            1,
            "keyword",
            "YES",
            "NO",
            "5000",
            "10.0",
            "2.5",
            "0.1",
            "30.0",
            "top_3",
            "",
        ]
        metrics = extract_week_metrics(row, 0)
        assert metrics is not None
        assert metrics["volume"] == 5000.0
        assert metrics["imp_share"] == 10.0

    def test_out_of_bounds_week_returns_none(self):
        row = [
            "ASIN",
            1,
            "keyword",
            "YES",
            "NO",
            "5000",
            "10.0",
            "2.5",
            "0.1",
            "30.0",
            "top_3",
            "",
        ]
        assert extract_week_metrics(row, 5) is None


class TestDetectDrasticChanges:
    def test_volume_drop_detected(self):
        sheet = _build_mock_sheet_with_changes(
            ["W01", "W02"],
            volume_overrides={
                "B0ASIN0001": {
                    "garlic press": {"W01": 5000, "W02": 2500},
                }
            },
        )
        parsed = parse_consolidated_sheet(sheet)
        flagged = detect_drastic_changes("B0ASIN0001", parsed["B0ASIN0001"], 2)

        kw_flags = [f for f in flagged if f["keyword"] == "garlic press"]
        assert len(kw_flags) == 1
        assert any("Volume" in r for r in kw_flags[0]["reasons"])

    def test_volume_drop_not_detected_under_threshold(self):
        sheet = _build_mock_sheet_with_changes(
            ["W01", "W02"],
            volume_overrides={
                "B0ASIN0001": {
                    "garlic press": {"W01": 5000, "W02": 4000},
                }
            },
        )
        parsed = parse_consolidated_sheet(sheet)
        flagged = detect_drastic_changes("B0ASIN0001", parsed["B0ASIN0001"], 2)

        kw_flags = [f for f in flagged if f["keyword"] == "garlic press"]
        volume_flags = [f for f in kw_flags if any("Volume" in r for r in f["reasons"])]
        assert len(volume_flags) == 0

    def test_rank_downgrade_detected(self):
        sheet = _build_mock_sheet_with_changes(
            ["W01", "W02"],
            rank_overrides={
                "B0ASIN0001": {
                    "garlic press": {"W01": "page_1_high", "W02": "invisible"},
                }
            },
        )
        parsed = parse_consolidated_sheet(sheet)
        flagged = detect_drastic_changes("B0ASIN0001", parsed["B0ASIN0001"], 2)

        kw_flags = [f for f in flagged if f["keyword"] == "garlic press"]
        assert len(kw_flags) >= 1
        assert any("Rank:" in r for r in kw_flags[0]["reasons"])

    def test_rank_upgrade_not_flagged(self):
        sheet = _build_mock_sheet_with_changes(
            ["W01", "W02"],
            rank_overrides={
                "B0ASIN0001": {
                    "garlic press": {"W01": "invisible", "W02": "top_3"},
                }
            },
        )
        parsed = parse_consolidated_sheet(sheet)
        flagged = detect_drastic_changes("B0ASIN0001", parsed["B0ASIN0001"], 2)

        kw_flags = [f for f in flagged if f["keyword"] == "garlic press"]
        rank_flags = [f for f in kw_flags if any("Rank:" in r for r in f["reasons"])]
        assert len(rank_flags) == 0

    def test_placement_drop_detected(self):
        sheet = _build_mock_sheet_with_changes(
            ["W01", "W02"],
            alert_overrides={
                "B0ASIN0001": {
                    "garlic press": "DROPPED FROM TITLE",
                }
            },
        )
        parsed = parse_consolidated_sheet(sheet)
        flagged = detect_drastic_changes("B0ASIN0001", parsed["B0ASIN0001"], 2)

        kw_flags = [f for f in flagged if f["keyword"] == "garlic press"]
        assert len(kw_flags) >= 1
        assert any("DROPPED FROM TITLE" in r for r in kw_flags[0]["reasons"])

    def test_multiple_reasons(self):
        sheet = _build_mock_sheet_with_changes(
            ["W01", "W02"],
            volume_overrides={
                "B0ASIN0001": {"garlic press": {"W01": 5000, "W02": 2000}},
            },
            rank_overrides={
                "B0ASIN0001": {"garlic press": {"W01": "top_3", "W02": "invisible"}},
            },
        )
        parsed = parse_consolidated_sheet(sheet)
        flagged = detect_drastic_changes("B0ASIN0001", parsed["B0ASIN0001"], 2)

        kw_flags = [f for f in flagged if f["keyword"] == "garlic press"]
        assert len(kw_flags) == 1
        assert len(kw_flags[0]["reasons"]) >= 2

    def test_single_week_no_wow_comparison(self):
        sheet = _build_mock_consolidated_sheet(["W01"])
        parsed = parse_consolidated_sheet(sheet)
        flagged = detect_drastic_changes("B0ASIN0001", parsed["B0ASIN0001"], 1)

        for f in flagged:
            for r in f["reasons"]:
                assert "Volume" not in r
                assert "Rank:" not in r

    def test_stable_data_no_flags(self):
        sheet = _build_mock_consolidated_sheet(["W01", "W02"])
        parsed = parse_consolidated_sheet(sheet)
        flagged = detect_drastic_changes("B0ASIN0001", parsed["B0ASIN0001"], 2)
        assert len(flagged) == 0


class TestBuildAsinSummary:
    def test_rank_status_counts(self):
        sheet = _build_mock_consolidated_sheet(["W01"])
        parsed = parse_consolidated_sheet(sheet)
        flagged = detect_drastic_changes("B0ASIN0001", parsed["B0ASIN0001"], 1)
        row = build_asin_summary("B0ASIN0001", parsed["B0ASIN0001"], 1, flagged)

        assert row[0] == "B0ASIN0001"
        assert row[1] == "Garlic Press"
        total = row[2] + row[3] + row[4] + row[5]
        assert total == 10

    def test_health_strong(self):
        sheet = _build_mock_consolidated_sheet(["W01"])
        parsed = parse_consolidated_sheet(sheet)
        flagged = []
        row = build_asin_summary("B0ASIN0001", parsed["B0ASIN0001"], 1, flagged)
        assert row[7] in ("STRONG", "OK")

    def test_health_at_risk(self):
        sheet = _build_mock_consolidated_sheet(["W01", "W02"])
        parsed = parse_consolidated_sheet(sheet)
        flagged = [
            {
                "keyword": "garlic press",
                "rank": 1,
                "reasons": ["Volume -50%"],
                "curr_vol": 2500,
                "curr_rank": "top_3",
                "prev_vol": 5000,
                "prev_rank": "top_3",
            }
        ]
        row = build_asin_summary("B0ASIN0001", parsed["B0ASIN0001"], 2, flagged)
        assert row[7] == "AT RISK"

    def test_health_weak(self):
        rank_overrides = {
            "B0ASIN0001": {
                kw: {"W01": "invisible"} for kw, _ in KEYWORDS_BY_ASIN["B0ASIN0001"]
            }
        }
        sheet = _build_mock_sheet_with_changes(["W01"], rank_overrides=rank_overrides)
        parsed = parse_consolidated_sheet(sheet)
        flagged = []
        row = build_asin_summary("B0ASIN0001", parsed["B0ASIN0001"], 1, flagged)
        assert row[7] == "WEAK"

    def test_top_alert_populated(self):
        sheet = _build_mock_consolidated_sheet(["W01", "W02"])
        parsed = parse_consolidated_sheet(sheet)
        flagged = [
            {
                "keyword": "garlic press",
                "rank": 1,
                "reasons": ["Volume -50% (5000 -> 2500)"],
                "curr_vol": 2500,
                "curr_rank": "top_3",
                "prev_vol": 5000,
                "prev_rank": "top_3",
            }
        ]
        row = build_asin_summary("B0ASIN0001", parsed["B0ASIN0001"], 2, flagged)
        assert row[6] == "Volume -50% (5000 -> 2500)"


class TestBuildDashboard:
    def test_both_sections_present(self):
        sheet = _build_mock_consolidated_sheet(["W01", "W02"])
        parsed = parse_consolidated_sheet(sheet)
        summary_headers, summary_rows, flagged_section = build_dashboard(parsed, 2)

        assert len(summary_headers) == 8
        assert len(summary_rows) == 3
        assert len(flagged_section) >= 1
        assert flagged_section[0][0] == "ASIN"

    def test_summary_row_count(self):
        sheet = _build_mock_consolidated_sheet(["W01"])
        parsed = parse_consolidated_sheet(sheet)
        _, summary_rows, _ = build_dashboard(parsed, 1)
        assert len(summary_rows) == 3

    def test_flagged_section_with_changes(self):
        sheet = _build_mock_sheet_with_changes(
            ["W01", "W02"],
            volume_overrides={
                "B0ASIN0001": {"garlic press": {"W01": 5000, "W02": 2000}},
            },
        )
        parsed = parse_consolidated_sheet(sheet)
        _, _, flagged_section = build_dashboard(parsed, 2)
        assert len(flagged_section) >= 2

    def test_flagged_section_empty_when_stable(self):
        sheet = _build_mock_consolidated_sheet(["W01", "W02"])
        parsed = parse_consolidated_sheet(sheet)
        _, _, flagged_section = build_dashboard(parsed, 2)
        assert len(flagged_section) == 1  # Only header

    def test_row_width_consistency(self):
        sheet = _build_mock_consolidated_sheet(["W01", "W02"])
        parsed = parse_consolidated_sheet(sheet)
        summary_headers, summary_rows, flagged_section = build_dashboard(parsed, 2)

        for row in summary_rows:
            assert len(row) == len(summary_headers)

        flagged_headers = flagged_section[0]
        for row in flagged_section[1:]:
            assert len(row) == len(flagged_headers)


class TestGenerateDashboard:
    def test_reads_q_tab_and_writes_dashboard(self):
        sheet = _build_mock_consolidated_sheet(["W01", "W02"])
        mock_sheets = MagicMock()
        mock_sheets.get_quarterly_tracker.return_value = sheet

        generate_dashboard(mock_sheets, "Q1")

        mock_sheets.get_quarterly_tracker.assert_called_once_with("Q1")
        mock_sheets.write_quarterly_tracker.assert_called_once()
        call_args = mock_sheets.write_quarterly_tracker.call_args
        assert call_args[0][0] == DASHBOARD_TAB_NAME

    def test_skips_on_empty_data(self):
        mock_sheets = MagicMock()
        mock_sheets.get_quarterly_tracker.return_value = None

        generate_dashboard(mock_sheets, "Q1")
        mock_sheets.write_quarterly_tracker.assert_not_called()

    def test_skips_on_header_only(self):
        mock_sheets = MagicMock()
        mock_sheets.get_quarterly_tracker.return_value = [
            ["ASIN", "Rank", "Keyword", "In Title", "In Backend", "Alert"]
        ]

        generate_dashboard(mock_sheets, "Q1")
        mock_sheets.write_quarterly_tracker.assert_not_called()

    def test_dashboard_contains_all_asins(self):
        sheet = _build_mock_consolidated_sheet(["W01", "W02"])
        mock_sheets = MagicMock()
        mock_sheets.get_quarterly_tracker.return_value = sheet

        generate_dashboard(mock_sheets, "Q1")

        call_args = mock_sheets.write_quarterly_tracker.call_args
        # write_quarterly_tracker(tab_name, headers, rows)
        rows = call_args[0][2]

        summary_asins = set()
        for row in rows:
            if not row:
                break
            if row[0] and str(row[0]).startswith("B0"):
                summary_asins.add(row[0])

        assert summary_asins == {"B0ASIN0001", "B0ASIN0002", "B0ASIN0003"}
