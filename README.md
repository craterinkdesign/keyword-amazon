# Amazon SQP Quarterly Keyword Tracker

Track your top 10 Amazon keywords per quarter with full metrics and alerts.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Start tracking for an ASIN (fetches all weeks in current quarter)
python -m sqp_analyzer.commands.quarterly_tracker --start --asin B0XXXXXXXX

# Weekly update
python -m sqp_analyzer.commands.quarterly_tracker --update --asin B0XXXXXXXX

# Update all ASINs from master list
python -m sqp_analyzer.commands.quarterly_tracker --update-all
```

## How It Works

The quarterly tracker:
1. **Locks in your top 10 keywords** at the start of each quarter (by purchase volume)
2. **Fetches all weeks** from Q start through current week
3. **Tracks 6 metrics per week**: Volume, Imp%, Click%, Purchase%, Opportunity Score, Rank Status
4. **Detects placement changes** if SKU is provided (keyword dropped from title/backend)
5. **Alerts** when keywords drop from your listing

## Google Sheets Structure

| Tab | Purpose |
|-----|---------|
| `ASINs` | Master list of ASINs to track (with optional SKU column) |
| `Q{N}-{ASIN}` | Quarterly tracker (e.g., `Q1-B0CSH12L5P`) |

### Quarterly Tracker Columns

| Column | Description |
|--------|-------------|
| Rank | Locked position (1-10) based on initial week's volume |
| Keyword | Search query (locked for quarter) |
| In Title | Auto-detected: YES/NO (requires SKU) |
| In Backend | Auto-detected: YES/NO (requires SKU) |
| W01 Vol | Week 1 search volume |
| W01 Imp% | Week 1 impression share |
| W01 Clk% | Week 1 click share |
| W01 Pur% | Week 1 purchase share |
| W01 Opp | Week 1 opportunity score |
| W01 Rank | Week 1 rank status |
| ... | (repeat for W02-W13) |
| Alert | Placement drop alerts |

## Understanding the Metrics

### The Conversion Funnel

```
Search Volume → Impressions → Clicks → Purchases
     ↓              ↓           ↓          ↓
  Demand       Visibility    Appeal    Conversion
```

| Metric | Question It Answers | If It Drops... |
|--------|---------------------|----------------|
| **Volume** | Is this keyword still popular? | Market is shifting - find new keywords |
| **Imp%** | Are shoppers seeing you? | SEO/PPC issue - boost visibility |
| **Click%** | Are they clicking you? | Listing issue - fix image/title/price |
| **Purchase%** | Are they buying from you? | Competitor winning - check reviews/price |

### Rank Status

Based on impression share:

| Status | Imp Share | Meaning |
|--------|-----------|---------|
| `top_3` | ≥20% | Ranking in top 3 positions |
| `page_1_high` | 10-20% | Upper page 1 |
| `page_1_low` | 1-10% | Lower page 1 |
| `invisible` | <1% | Not ranking for this keyword |

### Opportunity Score

Higher score = more opportunity for improvement (0-100):

| Diagnostic | Multiplier | Meaning |
|------------|------------|---------|
| Ghost | 2.0x | High volume, not ranking - biggest opportunity |
| Window Shopper | 1.5x | Seen but not clicked - listing needs work |
| Price Problem | 1.3x | Clicked but not bought - check pricing |
| Healthy | 0.5x | Already performing well |

## Setup

### 1. SP-API Credentials

Create `.env` file:

```ini
SP_API_CLIENT_ID=amzn1.application-oa2-client.xxx
SP_API_CLIENT_SECRET=amzn1.oa2-cs.v1.xxx
SP_API_REFRESH_TOKEN=Atzr|xxx
AWS_ACCESS_KEY=your_aws_access_key
AWS_SECRET_KEY=your_aws_secret_key
SP_API_ROLE_ARN=arn:aws:iam::xxx:role/xxx

# For listing content lookup (title/backend detection)
SELLER_ID=your_seller_id
```

### 2. Google Sheets

```ini
SPREADSHEET_ID=your_google_sheet_id
MASTER_TAB_NAME=ASINs
GOOGLE_CREDENTIALS_PATH=google-credentials.json
```

1. Create a Google Sheet with an `ASINs` tab
2. Add columns: `ASIN`, `SKU` (optional), `Status` (Active/Inactive)
3. Create a service account at [Google Cloud Console](https://console.cloud.google.com/)
4. Download credentials as `google-credentials.json`
5. Share your Google Sheet with the service account email

### 3. Optional Thresholds

```ini
# Rank status thresholds (impression share %)
RANK_TOP_3_THRESHOLD=20.0
RANK_PAGE_1_HIGH_THRESHOLD=10.0
RANK_PAGE_1_LOW_THRESHOLD=1.0

# Diagnostic thresholds
GHOST_MIN_VOLUME=500
GHOST_MAX_IMP_SHARE=1.0
WINDOW_SHOPPER_MIN_IMP_SHARE=10.0
WINDOW_SHOPPER_MAX_CLICK_SHARE=1.0
PRICE_PROBLEM_MIN_IMP_SHARE=5.0
```

## Commands

### Quarterly Tracker

```bash
# Start new quarter (fetches W01 through current week)
python -m sqp_analyzer.commands.quarterly_tracker --start --asin B0XXXXXXXX

# Start with SKU for title/backend detection
python -m sqp_analyzer.commands.quarterly_tracker --start --asin B0XXXXXXXX --sku YOUR-SKU

# Weekly update (adds new week's data)
python -m sqp_analyzer.commands.quarterly_tracker --update --asin B0XXXXXXXX

# Update all active ASINs from master list
python -m sqp_analyzer.commands.quarterly_tracker --update-all

# Test Google Sheets connection
python -m sqp_analyzer.commands.quarterly_tracker --test-sheets
```

### Fetch SQP Data (Manual)

```bash
# Request new report
python -m sqp_analyzer.commands.fetch_sqp_data --asin B0XXXXXXXX

# Request and wait for completion
python -m sqp_analyzer.commands.fetch_sqp_data --asin B0XXXXXXXX --wait

# Check report status
python -m sqp_analyzer.commands.fetch_sqp_data --check REPORT_ID

# List recent reports
python -m sqp_analyzer.commands.fetch_sqp_data --list
```

### Fetch Listing Content

```bash
# Fetch listing title, bullets, and backend keywords
python -m sqp_analyzer.commands.fetch_listing --sku YOUR-SKU
```

### Traffic & Sales Reports

```bash
# Request traffic/sales report
python -m sqp_analyzer.commands.fetch_traffic_sales --asin B0XXXXXXXX --wait

# Write to Google Sheets
python -m sqp_analyzer.commands.analyze_traffic_sales --report-id REPORT_ID
```

## Quarterly Workflow

```
Quarter Start (Week 1):
├─ Run: --start --asin B0XXX
├─ Fetches all complete weeks in quarter
├─ Locks top 10 keywords by purchase volume
└─ Creates Q{N}-{ASIN} tab with full history

Weekly (Week 2-13):
├─ Run: --update --asin B0XXX (or --update-all)
├─ Adds new week's metrics
├─ Re-checks title/backend placement
└─ Flags any alerts

Quarter End:
├─ Review 13-week trends
├─ Archive or keep tab for reference
└─ Next quarter starts fresh with new top 10
```

## Alert Types

| Alert | Trigger | Action |
|-------|---------|--------|
| `DROPPED FROM TITLE` | Keyword was in title, now isn't | Re-add keyword to title |
| `DROPPED FROM BACKEND` | Keyword was in backend, now isn't | Re-add to backend keywords |

## Files

```
├── .env                      # Configuration
├── google-credentials.json   # Google service account
├── requirements.txt          # Python dependencies
│
└── sqp_analyzer/
    ├── config.py             # Configuration loader
    ├── models.py             # Data models
    │
    ├── sheets/
    │   └── client.py         # Google Sheets client
    │
    ├── amazon/               # SP-API helpers (auth, client)
    │
    └── commands/
        ├── quarterly_tracker.py   # Main quarterly tracking
        ├── fetch_listing.py       # Fetch listing content
        ├── fetch_sqp_data.py      # Fetch SQP reports
        ├── fetch_traffic_sales.py # Fetch traffic/sales
        └── analyze_traffic_sales.py # Write traffic/sales to Sheets
```

## Notes

- **Quarter detection**: Q1 (Jan-Mar), Q2 (Apr-Jun), Q3 (Jul-Sep), Q4 (Oct-Dec)
- **Week numbering**: W01-W13 within each quarter
- **Report timing**: SQP reports take 30-60 minutes to process
- **Listing lookup**: Requires `SELLER_ID` and SKU in ASINs tab
- **Keywords locked**: Top 10 keywords stay fixed for the quarter to show true trends
