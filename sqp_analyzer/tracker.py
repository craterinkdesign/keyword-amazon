"""Weekly keyword tracking - monitors top keywords for changes."""

import csv
import io
import re
from datetime import date
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

from .parsers import _normalize_columns, _parse_float, _parse_int


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Alert thresholds
VOLUME_DROP_THRESHOLD = 30      # Alert if volume drops >30%
PURCHASE_DROP_THRESHOLD = 20    # Alert if purchase share drops >20%


def track_weekly(csv_path: str, spreadsheet_id: str, credentials_path: str = "google-credentials.json"):
    """Update keyword watchlist with new week's data.

    Args:
        csv_path: Path to new week's CSV export
        spreadsheet_id: Google Sheet ID
        credentials_path: Path to Google credentials JSON
    """
    # Parse new CSV data
    csv_path = Path(csv_path)
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        lines = f.readlines()

    # Extract week from metadata or filename
    week_label = _extract_week_label(lines[0], csv_path.name)

    # Skip metadata row
    if "Reporting Range" in lines[0]:
        lines = lines[1:]

    reader = csv.DictReader(io.StringIO("".join(lines)))

    # Build lookup of current week's data
    current_data = {}
    for row in reader:
        norm = _normalize_columns(row)
        query = norm.get("search_query", "")
        if query:
            current_data[query.lower()] = {
                "score": _parse_int(norm.get("search_score", 0)),
                "volume": _parse_int(norm.get("search_volume", 0)),
                "purchase_share": _parse_float(norm.get("purchases_share", 0)) or 0,
            }

    # Connect to Google Sheets
    creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(spreadsheet_id)

    # Get watchlist
    try:
        ws = sheet.worksheet("Keyword Watchlist")
    except gspread.WorksheetNotFound:
        print("Error: 'Keyword Watchlist' tab not found. Run initial setup first.")
        return

    # Read current watchlist
    all_values = ws.get_all_values()
    headers = all_values[0]

    # Check if this week already exists
    if any(week_label in h for h in headers):
        print(f"Week {week_label} already tracked. Skipping.")
        return

    # Find the Alert column index (last column before we add new ones)
    status_col = len(headers) - 1

    # Add new week columns before Alert
    new_headers = headers[:status_col] + [
        f"Score\n{week_label}",
        f"Vol\n{week_label}",
        f"Purch%\n{week_label}"
    ] + [headers[status_col]]

    # Update each row with new data
    new_rows = [new_headers]
    alerts = []

    for row in all_values[1:]:
        if not row or not row[1]:  # Skip empty rows
            continue

        keyword = row[1]
        keyword_lower = keyword.lower()

        # Get previous week's data - find the most recent volume and purchase columns
        prev_volume = 0
        prev_purchase = 0
        for i, h in enumerate(headers):
            if "Vol" in h and i < len(row):
                val = _parse_int(row[i])
                if val > 0:
                    prev_volume = val
            if "Purch" in h and i < len(row):
                val = _parse_float(row[i])
                if val and val > 0:
                    prev_purchase = val

        # Get current week's data
        if keyword_lower in current_data:
            curr = current_data[keyword_lower]
            curr_score = curr["score"]
            curr_volume = curr["volume"]
            curr_purchase = round(curr["purchase_share"], 1)

            # Check for alerts
            alerts_for_keyword = []

            # Volume drop check - keyword losing popularity
            if prev_volume > 0:
                volume_change = ((curr_volume - prev_volume) / prev_volume) * 100
                if volume_change < -VOLUME_DROP_THRESHOLD:
                    alerts_for_keyword.append(f"📉 Vol -{abs(volume_change):.0f}%")
                    alerts.append(f"{keyword}: Volume dropped {abs(volume_change):.0f}% (keyword losing popularity)")

            # Purchase share drop check - losing to competitors
            if prev_purchase > 0:
                purchase_change = ((curr_purchase - prev_purchase) / prev_purchase) * 100
                if purchase_change < -PURCHASE_DROP_THRESHOLD:
                    alerts_for_keyword.append(f"📉 Purch -{abs(purchase_change):.0f}%")
                    alerts.append(f"{keyword}: Purchase share dropped {abs(purchase_change):.0f}% (losing to competitors)")

            status = " | ".join(alerts_for_keyword) if alerts_for_keyword else ""
        else:
            # Keyword not found in new data
            curr_score = "-"
            curr_volume = "-"
            curr_purchase = "-"
            status = "❌ Not in top results"
            alerts.append(f"{keyword}: No longer in top search results!")

        # Build new row
        new_row = row[:status_col] + [curr_score, curr_volume, curr_purchase, status]
        new_rows.append(new_row)

    # Write updated data
    ws.clear()
    ws.update(values=new_rows, range_name="A1")

    # Re-apply checkbox validation to "In Title" column if it exists
    if "In Title" in new_headers:
        in_title_col = new_headers.index("In Title")
        num_rows = len(new_rows)
        sheet_id = ws.id
        checkbox_request = {
            "requests": [{
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": num_rows,
                        "startColumnIndex": in_title_col,
                        "endColumnIndex": in_title_col + 1
                    },
                    "cell": {
                        "dataValidation": {
                            "condition": {"type": "BOOLEAN"},
                            "strict": True,
                            "showCustomUi": True
                        }
                    },
                    "fields": "dataValidation"
                }
            }]
        }
        sheet.batch_update(checkbox_request)

    print(f"Updated watchlist with {week_label} data")

    if alerts:
        print(f"\n⚠️  ALERTS ({len(alerts)}):")
        for alert in alerts:
            print(f"  • {alert}")
    else:
        print("\n✓ All keywords stable")


def reset_watchlist(csv_path: str, spreadsheet_id: str, credentials_path: str = "google-credentials.json"):
    """Archive current watchlist and create new one with fresh top 10.

    Args:
        csv_path: Path to latest CSV export
        spreadsheet_id: Google Sheet ID
        credentials_path: Path to Google credentials JSON
    """
    # Parse CSV data
    csv_path = Path(csv_path)
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        lines = f.readlines()

    # Skip metadata row
    if "Reporting Range" in lines[0]:
        lines = lines[1:]

    reader = csv.DictReader(io.StringIO("".join(lines)))

    # Get all keywords with purchase share > 0
    keywords = []
    for row in reader:
        norm = _normalize_columns(row)
        query = norm.get("search_query", "")
        score = _parse_int(norm.get("search_score", 0))
        volume = _parse_int(norm.get("search_volume", 0))
        purchase_share = _parse_float(norm.get("purchases_share", 0)) or 0

        if query and purchase_share > 0:
            keywords.append({
                "keyword": query,
                "score": score,
                "volume": volume,
                "purchase_share": purchase_share,
            })

    # Sort by Amazon's Score (lower = better)
    keywords.sort(key=lambda x: x["score"])
    top_10 = keywords[:10]

    if not top_10:
        print("Error: No keywords with purchase share found in CSV")
        return

    # Connect to Google Sheets
    creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(spreadsheet_id)

    # Archive old watchlist if it exists
    try:
        old_ws = sheet.worksheet("Keyword Watchlist")
        archive_name = f"Watchlist Archive {date.today().isoformat()}"
        old_ws.update_title(archive_name)
        print(f"Archived old watchlist as '{archive_name}'")
    except gspread.WorksheetNotFound:
        pass

    # Create new watchlist
    ws = sheet.add_worksheet("Keyword Watchlist", rows=20, cols=15)

    # Get week label
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        first_line = f.readline()
    week_label = _extract_week_label(first_line, csv_path.name)

    # Build headers and rows
    headers = ["Rank", "Keyword", "In Title", f"Score\n{week_label}", f"Vol\n{week_label}", f"Purch%\n{week_label}", "Alert"]
    rows = [headers]

    for i, kw in enumerate(top_10, 1):
        rows.append([
            i,
            kw["keyword"],
            "",  # Checkbox - user marks if keyword is in listing title
            kw["score"],
            kw["volume"],
            round(kw["purchase_share"], 1),
            ""
        ])

    ws.update(values=rows, range_name="A1")

    # Add checkbox data validation to "In Title" column (C2:C11)
    sheet_id = ws.id
    checkbox_request = {
        "requests": [{
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": 11,
                    "startColumnIndex": 2,
                    "endColumnIndex": 3
                },
                "cell": {
                    "dataValidation": {
                        "condition": {"type": "BOOLEAN"},
                        "showCustomUi": True
                    }
                },
                "fields": "dataValidation"
            }
        }]
    }
    sheet.batch_update(checkbox_request)

    print(f"\nCreated new watchlist with top 10 keywords:")
    print(f"{'#':<3} {'Keyword':<45} {'Score':>6} {'Vol':>5}")
    print("-" * 62)
    for i, kw in enumerate(top_10, 1):
        print(f"{i:<3} {kw['keyword']:<45} {kw['score']:>6} {kw['volume']:>5}")

    print("\n✓ Fresh 12-week tracking cycle started")


def _extract_week_label(metadata_line: str, filename: str) -> str:
    """Extract week label from metadata or filename."""
    # Try to extract from metadata: "Week 5 | 2026-01-25 - 2026-01-31"
    match = re.search(r'Week\s+(\d+)\s*\|\s*(\d{4})-(\d{2})-(\d{2})', metadata_line)
    if match:
        year = match.group(2)
        week = match.group(1)
        return f"{year}-W{int(week):02d}"

    # Try from filename: Week_2026_01_31
    match = re.search(r'(\d{4})[-_](\d{2})[-_](\d{2})', filename)
    if match:
        d = date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        year, week, _ = d.isocalendar()
        return f"{year}-W{week:02d}"

    # Default to today
    today = date.today()
    year, week, _ = today.isocalendar()
    return f"{year}-W{week:02d}"


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python -m sqp_analyzer.tracker <csv_file>          # Update weekly")
        print("  python -m sqp_analyzer.tracker --reset <csv_file>  # Start fresh")
        sys.exit(1)

    from .config import load_config
    config = load_config()

    if sys.argv[1] == "--reset":
        if len(sys.argv) < 3:
            print("Error: CSV file required with --reset")
            sys.exit(1)
        reset_watchlist(
            sys.argv[2],
            config.sheets.spreadsheet_id,
            config.sheets.credentials_path,
        )
    else:
        track_weekly(
            sys.argv[1],
            config.sheets.spreadsheet_id,
            config.sheets.credentials_path,
        )
