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

---

## Deep Dive: How To Read The Numbers

### Search Volume Explained

**What it is:** How many people searched that exact term on Amazon this week.

**Example fluctuations:**
```
Week 1: "mg scoop" → 500 searches
Week 2: "mg scoop" → 480 searches  ← Normal fluctuation (ignore)
Week 3: "mg scoop" → 250 searches  ← 📉 50% drop (ALERT!)
```

**Why volume drops:**
- **Seasonal** - Summer vs winter products
- **Trend died** - Think fidget spinners in 2018
- **Language shift** - People started using different words

**What to do:** If volume dies, find what NEW keyword people are using instead. Check Amazon's search suggestions or your SQP report for rising terms.

### Purchase Share % Explained

**What it is:** Of everyone who BOUGHT something after searching this keyword, what percentage bought YOUR product.

**Example:**
```
100 people search "mg scoop"
 └─→ 40 people buy something (60 left without buying)
      └─→ 16 buy YOUR product
      └─→ 24 buy COMPETITOR products

Your Purchase Share = 16 ÷ 40 = 40%
```

**Why purchase share drops:**
- Competitor **lowered their price**
- Competitor **got more/better reviews**
- Your listing got worse (bad image, out of stock, lost Buy Box)
- **New competitor** entered the market

**What to do:** Investigate who's stealing your sales. Check the top 3 competitors for that keyword - compare their price, reviews, and main image to yours.

### Real Example

If your report shows:

| Keyword | Score | Volume | Purchase % |
|---------|-------|--------|------------|
| mg scoop | 3 | 52 | 66.7% |

This means:
- Amazon ranks it your **#3 most important keyword**
- **52 people** searched it this week
- Of everyone who bought after searching this, **66.7% bought from YOU**

**That's strong!** If next week it drops to 33%, someone is stealing half your sales on this keyword. Time to investigate.

### The Two Alerts Summarized

| Alert | What Happened | What To Do |
|-------|---------------|------------|
| 📉 Vol -30% | Fewer people searching this term | Find what keywords they're using NOW |
| 📉 Purch -20% | Same searches, but competitors winning more | Check competitor price/reviews/listing |

---

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

### 3. Get SQP Data from Amazon

**Option A: API (Recommended)**

Fetch SQP data directly via the SP-API:

```bash
# Request a new SQP report
python -m sqp_analyzer.commands.fetch_sqp_data --asin B0XXXXXXXX

# Check report status (reports take 30-60 min to process)
python -m sqp_analyzer.commands.fetch_sqp_data --check REPORT_ID

# List recent reports
python -m sqp_analyzer.commands.fetch_sqp_data --list
```

Requires SP-API credentials in `.env`:
```ini
SP_API_CLIENT_ID=amzn1.application-oa2-client.xxx
SP_API_CLIENT_SECRET=amzn1.oa2-cs.v1.xxx
SP_API_REFRESH_TOKEN=Atzr|xxx
```

**Option B: CSV Export**

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

### Keyword Watchlist Columns

| Column | Purpose |
|--------|---------|
| **Rank** | Amazon's Search Query Score (1 = most important) |
| **Keyword** | The search term |
| **In Title** | ☑ Checkbox - mark if you're using this keyword in your listing title |
| **Score/Vol/Purch%** | Weekly metrics (new columns added each week) |
| **Alert** | Empty if stable, shows warning when metrics drop |

Use the **In Title** checkbox to track which of your top keywords you've actually included in your listing title. This helps you see if you're missing any important keywords.

### Example Alert View

```
Keyword                        W05 Vol  W06 Vol  Purch%   Alert
----------------------------------------------------------------
10mg scoop                          57       28   37.5%   📉 Vol -51%
milligram measuring spoons          88       88   37.5%
mg scoop                            52       52   33.3%   📉 Purch -50%
```

- `📉 Vol -XX%` → Keyword losing popularity (find replacement)
- `📉 Purch -XX%` → Losing to competitors (investigate price/reviews)

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
    ├── analyzers/            # Analysis algorithms
    └── commands/
        ├── fetch_sqp_data.py         # Fetch SQP data via SP-API
        ├── fetch_traffic_sales.py    # Fetch traffic & sales by ASIN
        ├── analyze_sqp.py            # Analyze SQP data and write to Sheets
        └── analyze_traffic_sales.py  # Write traffic/sales to Sheets
```

## SP-API SQP Fetching

The `fetch_sqp_data` command fetches Search Query Performance data directly from Amazon's Brand Analytics API.

### Usage

```bash
# Test API connection
python -m sqp_analyzer.commands.fetch_sqp_data --test-connection

# Request new report (uses last complete week)
python -m sqp_analyzer.commands.fetch_sqp_data --asin B0XXXXXXXX

# Request with specific dates (start must be Sunday)
python -m sqp_analyzer.commands.fetch_sqp_data --asin B0XXXXXXXX \
    --start-date 2026-01-25 --end-date 2026-01-31

# List recent reports
python -m sqp_analyzer.commands.fetch_sqp_data --list

# Check status and download completed report
python -m sqp_analyzer.commands.fetch_sqp_data --check REPORT_ID

# Request and wait for completion (30-60 min)
python -m sqp_analyzer.commands.fetch_sqp_data --asin B0XXXXXXXX --wait
```

### Requirements

- Amazon Brand Registry enrollment
- SP-API credentials (LWA app ID, secret, refresh token)

### Notes

- Brand Analytics reports take **30-60 minutes** to process
- Weekly reports must start on a **Sunday**
- Reports show search queries, impressions, clicks, and purchase data for your ASIN

## SQP Analysis Command

The `analyze_sqp` command fetches a completed SQP report, runs diagnostic and placement analysis, and writes results to Google Sheets.

### Usage

```bash
# Analyze a completed report and write to Google Sheets
python -m sqp_analyzer.commands.analyze_sqp --report-id REPORT_ID

# Test Google Sheets connection
python -m sqp_analyzer.commands.analyze_sqp --test-sheets
```

### Workflow

1. Request a report: `python -m sqp_analyzer.commands.fetch_sqp_data --asin B0XXXXXXXX`
2. Wait 30-60 minutes for processing
3. Check status: `python -m sqp_analyzer.commands.fetch_sqp_data --check REPORT_ID`
4. Once DONE, analyze: `python -m sqp_analyzer.commands.analyze_sqp --report-id REPORT_ID`

### Output Tabs

The command creates/updates these Google Sheets tabs:

| Tab | Description |
|-----|-------------|
| **SQP-Diagnostics** | All keywords with diagnostic type, rank status, opportunity score |
| **SQP-Placements** | Keyword placement recommendations (Title/Bullets/Backend/Description) |
| **SQP-TopOpportunities** | Top 20 keywords ranked by opportunity score |

### Diagnostic Types

| Type | Meaning | Fix |
|------|---------|-----|
| **Ghost** | High volume but invisible (not ranking) | Add to listing or run PPC |
| **Window Shopper** | Seen but not clicked | Improve main image/title |
| **Price Problem** | Clicked but not bought | Check pricing vs competitors |
| **Healthy** | No issues detected | Maintain strategy |

### Placement Recommendations

| Placement | Criteria | Priority |
|-----------|----------|----------|
| **Title** | Top 5% volume OR (top 20% + good click share) | Highest |
| **Bullets** | 50-80% volume percentile | High |
| **Backend** | 20-50% volume percentile | Medium |
| **Description** | Bottom 20% volume | Lower |

## Traffic and Sales Report

The `fetch_traffic_sales` command fetches Business Report data (traffic and sales) by child ASIN.

### Usage

```bash
# Request report for last 7 days (daily by child ASIN)
python -m sqp_analyzer.commands.fetch_traffic_sales --asin B0CSH12L5P

# Request and wait for completion
python -m sqp_analyzer.commands.fetch_traffic_sales --asin B0CSH12L5P --wait

# Weekly granularity instead of daily
python -m sqp_analyzer.commands.fetch_traffic_sales --asin B0CSH12L5P --date-granularity WEEK

# Custom date range
python -m sqp_analyzer.commands.fetch_traffic_sales --start-date 2026-01-01 --end-date 2026-01-31

# Check status of pending report
python -m sqp_analyzer.commands.fetch_traffic_sales --check REPORT_ID

# List recent reports
python -m sqp_analyzer.commands.fetch_traffic_sales --list
```

### Report Options

| Option | Values | Default | Description |
|--------|--------|---------|-------------|
| `--date-granularity` | DAY, WEEK, MONTH | DAY | Time period aggregation |
| `--asin-granularity` | PARENT, CHILD, SKU | CHILD | Product aggregation level |

### Metrics Returned

**Sales Metrics:**
- Units Ordered
- Ordered Product Sales ($)
- Units Shipped
- Orders Shipped
- Refund Rate

**Traffic Metrics:**
- Page Views (browser + mobile)
- Sessions
- Buy Box Percentage
- Unit Session Percentage

### Notes

- Reports typically take **5-15 minutes** to process
- Data is available 72 hours after the period closes
- Use `CHILD` granularity to see individual variation performance

### Write to Google Sheets

Use `analyze_traffic_sales` to write report data to Google Sheets:

```bash
# Write completed report to Google Sheets
python -m sqp_analyzer.commands.analyze_traffic_sales --report-id REPORT_ID
```

### Workflow

1. Request report: `python -m sqp_analyzer.commands.fetch_traffic_sales --asin B0XXXXXXXX`
2. Wait 5-15 minutes for processing
3. Check status: `python -m sqp_analyzer.commands.fetch_traffic_sales --check REPORT_ID`
4. Once DONE, write to Sheets: `python -m sqp_analyzer.commands.analyze_traffic_sales --report-id REPORT_ID`

### Output Tabs

| Tab | Description |
|-----|-------------|
| **Traffic-ByDate** | Daily metrics: units, sales, sessions, page views, buy box % |
| **Traffic-ByASIN** | Per-ASIN metrics: units, sales, sessions, buy box % |
