# Amazon SQP Keyword Tracker

Track your top Amazon keywords over 12 weeks and get alerts when action is needed.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# First time: Import CSV and set up your top 10 keywords
python -m sqp_analyzer.main --import-csv "your_sqp_export.csv" --asin B0XXXXXXXX

# Weekly: Update tracker with new data
python -m sqp_analyzer.tracker "new_week_sqp_export.csv"
```

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
| **Impressions Share** | Are shoppers seeing you? | SEO/PPC issue - boost your ads |
| **Click Share** | Are they clicking you? | Listing issue - fix image/title/price |
| **Purchase Share** | Are they buying from you? | Competitor winning - check reviews/price |

### What We Track

The tracker monitors **Volume** and **Purchase Share** - the two most actionable metrics:

| Alert | Trigger | What It Means | Action |
|-------|---------|---------------|--------|
| 📉 Vol -XX% | Volume dropped >30% | Keyword losing popularity | Find trending replacement keywords |
| 📉 Purch -XX% | Purchase share dropped >20% | Losing sales to competitors | Investigate: check price, reviews, listing |
| ❌ Not in results | Keyword disappeared | You're no longer ranking | Major issue - review SEO/PPC strategy |

### Diagnostic Metrics

When purchase share drops, check these to find the root cause:

| If This Dropped | The Problem Is | Fix |
|-----------------|----------------|-----|
| Impressions Share | Visibility | Increase PPC bids, improve SEO |
| Click Share | Appeal | Better main image, title, or lower price |
| Purchase Share only | Conversion | Reviews, A+ content, price competitiveness |

## Setup

### 1. Google Sheets

1. Create a Google Sheet
2. Create a service account at [Google Cloud Console](https://console.cloud.google.com/)
3. Download credentials as `google-credentials.json`
4. Share your Google Sheet with the service account email

### 2. Configuration

Create `.env` file:

```ini
SPREADSHEET_ID=your_google_sheet_id
MASTER_TAB_NAME=ASINs

# Thresholds (optional)
BREAD_BUTTER_MIN_PURCHASE_SHARE=10.0
OPPORTUNITY_MAX_IMP_SHARE=5.0
OPPORTUNITY_MIN_PURCHASE_SHARE=5.0
LEAK_MIN_IMP_SHARE=5.0
LEAK_MAX_CLICK_SHARE=2.0
LEAK_MAX_PURCHASE_SHARE=2.0
PRICE_WARNING_THRESHOLD=10.0
PRICE_CRITICAL_THRESHOLD=20.0
```

### 3. Export SQP Data from Amazon

1. Go to **Seller Central** → **Brands** → **Brand Analytics**
2. Select **Search Query Performance**
3. Choose your ASIN and date range (Weekly)
4. Download CSV

## 12-Week Tracking Cycle

The tracker is designed for a 12-week observation period:

```
Week 1:  Set up top 10 keywords (locked in)
         ↓
Week 2-12: Run weekly tracker
         ↓  - New columns added each week
         ↓  - Alerts flag any drops
         ↓  - See trends develop over time
         ↓
Week 12: Review full trend data
         ↓
Reset:   Start fresh with new top 10
```

**Keywords stay locked** for the full 12 weeks so you can see true trends, not just week-to-week noise.

## Weekly Workflow

1. **Download** new week's SQP CSV from Amazon
2. **Run tracker**: `python -m sqp_analyzer.tracker "new_export.csv"`
3. **Check alerts** in terminal and Google Sheet
4. **Investigate** any drops using the diagnostic guide above

## After 12 Weeks: Reset

When you're ready to start fresh with new keywords:

```bash
python -m sqp_analyzer.tracker --reset "latest_export.csv"
```

This will:
- Archive your current watchlist (renamed with date)
- Create new watchlist with current top 10 keywords
- Start a fresh 12-week tracking cycle

## Google Sheet Tabs

| Tab | Purpose |
|-----|---------|
| **ASINs** | Your products to track |
| **Keyword Watchlist** | Top 10 keywords with weekly trend data |

## Keyword Categories (Full Analysis)

When running full analysis (`--import-csv` without tracker), keywords are categorized:

| Category | Criteria | Action |
|----------|----------|--------|
| **Bread & Butter** | Purchase Share ≥ 10% | Protect these - your money makers |
| **Opportunity** | Low Impressions + High Purchase Share | Increase PPC - high conversion potential |
| **Leak** | High Impressions + Low Clicks/Purchases | Fix listing - wasted visibility |

## Files

```
├── .env                      # Your configuration
├── google-credentials.json   # Google service account
├── requirements.txt          # Python dependencies
│
└── sqp_analyzer/
    ├── main.py               # Full analysis
    ├── tracker.py            # Weekly keyword tracking
    ├── importers.py          # CSV/Excel import
    ├── parsers.py            # Data parsing
    └── analyzers/            # Analysis algorithms
```
