"""Main entry point for SQP Analyzer."""

import argparse
import sys
from datetime import date
from pathlib import Path

from .config import load_config, AppConfig
from .amazon import BrandAnalyticsClient
from .sheets import SheetsClient
from .parsers import parse_api_report
from .models import ASINSummary, WeeklySnapshot
from .analyzers import (
    DiagnosticAnalyzer,
    KeywordCategorizer,
    PlacementRecommender,
    PriceBenchmark,
    TrendTracker,
)
from .importers import import_csv, import_excel, import_folder


def import_sqp_data(
    file_path: str,
    asin: str,
    week_date: date | None = None,
) -> list[WeeklySnapshot]:
    """Import SQP data from CSV/Excel file or folder.

    Args:
        file_path: Path to CSV/Excel file or folder
        asin: Parent ASIN for this data
        week_date: Optional week date (auto-detected if not provided)

    Returns:
        List of WeeklySnapshots
    """
    path = Path(file_path)

    if path.is_dir():
        print(f"Importing all files from {path}...")
        return import_folder(path, asin)
    elif path.suffix.lower() == ".csv":
        print(f"Importing CSV: {path}...")
        snapshot = import_csv(path, asin, week_date)
        print(f"  Imported {len(snapshot.records)} keywords for week {snapshot.week_date}")
        return [snapshot]
    elif path.suffix.lower() in (".xlsx", ".xls"):
        print(f"Importing Excel: {path}...")
        snapshot = import_excel(path, asin, week_date)
        print(f"  Imported {len(snapshot.records)} keywords for week {snapshot.week_date}")
        return [snapshot]
    else:
        raise ValueError(f"Unsupported file type: {path.suffix}")


def test_api_connection(config: AppConfig) -> bool:
    """Test SP-API connection."""
    print("Testing SP-API connection...")
    client = BrandAnalyticsClient(config.sp_api)
    result = client.test_connection()

    if result["success"]:
        print(f"✓ {result['message']}")
        return True
    else:
        print(f"✗ {result['message']}")
        return False


def test_sheets_connection(config: AppConfig) -> bool:
    """Test Google Sheets connection."""
    print("Testing Google Sheets connection...")
    client = SheetsClient(config.sheets)

    if client.test_connection():
        print("✓ Successfully connected to Google Sheets")
        return True
    else:
        print("✗ Failed to connect to Google Sheets")
        return False


def fetch_sqp_data(
    config: AppConfig,
    asin: str,
    weeks: int = 12,
) -> list[WeeklySnapshot]:
    """Fetch SQP data for an ASIN."""
    client = BrandAnalyticsClient(config.sp_api)
    print(f"Fetching {weeks} weeks of SQP data for {asin}...")

    responses = client.get_weekly_reports(asin, weeks)
    snapshots = []

    for response in responses:
        if response.success and response.data:
            snapshot = parse_api_report(response.data)
            snapshots.append(snapshot)
            print(f"  Week {snapshot.week_date}: {len(snapshot.records)} keywords")
        else:
            print(f"  Warning: Failed to fetch week - {response.error_message}")

    return snapshots


def analyze_snapshots(
    config: AppConfig,
    snapshots: list[WeeklySnapshot],
) -> dict:
    """Run all analysis on snapshots."""
    if not snapshots:
        return {}

    # Use most recent snapshot for categorization
    latest = max(snapshots, key=lambda s: s.week_date)

    # Initialize analyzers
    categorizer = KeywordCategorizer(config.thresholds)
    trend_tracker = TrendTracker()
    price_benchmark = PriceBenchmark(config.thresholds)
    diagnostic_analyzer = DiagnosticAnalyzer(config.thresholds)
    placement_recommender = PlacementRecommender(config.thresholds)

    # Run analysis
    categorized = categorizer.categorize(latest)
    trends = trend_tracker.analyze_trends(snapshots)
    price_flags = price_benchmark.analyze(latest)

    # Run new diagnostic and placement analysis
    diagnostics = diagnostic_analyzer.analyze(latest, price_flags)
    placements = placement_recommender.analyze(latest)

    return {
        "latest_snapshot": latest,
        "categorized": categorized,
        "bread_butter": categorizer.get_bread_butter(categorized),
        "opportunities": categorizer.get_opportunities(categorized),
        "leaks": categorizer.get_leaks(categorized),
        "trends": trends,
        "price_flags": price_flags,
        "diagnostics": diagnostics,
        "placements": placements,
        "summary": {
            "categories": categorizer.summarize(categorized),
            "prices": price_benchmark.summarize(price_flags),
            "diagnostics": diagnostic_analyzer.summarize(diagnostics),
            "placements": placement_recommender.summarize(placements),
        },
    }


def write_results_to_sheets(
    config: AppConfig,
    asin: str,
    analysis: dict,
    snapshots: list[WeeklySnapshot],
) -> None:
    """Write analysis results to Google Sheets."""
    client = SheetsClient(config.sheets)

    print("Writing results to Google Sheets...")

    # Write weekly data for most recent week
    if snapshots:
        latest = analysis["latest_snapshot"]
        print(f"  Writing weekly data for {latest.week_date}...")
        client.write_weekly_data(
            latest.week_date,
            [r.to_dict() for r in latest.records],
        )

    # Write categorized keywords
    if analysis.get("bread_butter"):
        print(f"  Writing {len(analysis['bread_butter'])} Bread & Butter keywords...")
        headers = [
            "Search Query", "ASIN", "Category", "Imp Share",
            "Click Share", "Purchase Share", "Volume", "Recommended Action"
        ]
        client.write_categorized_keywords(
            "SQP-BreadButter",
            [k.to_dict() for k in analysis["bread_butter"]],
            headers,
        )

    if analysis.get("opportunities"):
        print(f"  Writing {len(analysis['opportunities'])} Opportunity keywords...")
        headers = [
            "Search Query", "ASIN", "Category", "Imp Share",
            "Click Share", "Purchase Share", "Volume", "Recommended Action"
        ]
        client.write_categorized_keywords(
            "SQP-Opportunities",
            [k.to_dict() for k in analysis["opportunities"]],
            headers,
        )

    if analysis.get("leaks"):
        print(f"  Writing {len(analysis['leaks'])} Leak keywords...")
        headers = [
            "Search Query", "ASIN", "Category", "Imp Share",
            "Click Share", "Purchase Share", "Volume", "Recommended Action"
        ]
        client.write_categorized_keywords(
            "SQP-Leaks",
            [k.to_dict() for k in analysis["leaks"]],
            headers,
        )

    # Write trends
    if analysis.get("trends"):
        print(f"  Writing {len(analysis['trends'])} trend records...")
        client.write_trends([t.to_dict() for t in analysis["trends"]])

    # Write price flags
    if analysis.get("price_flags"):
        print(f"  Writing {len(analysis['price_flags'])} price flags...")
        client.write_price_flags([f.to_dict() for f in analysis["price_flags"]])

    # Write diagnostics
    if analysis.get("diagnostics"):
        print(f"  Writing {len(analysis['diagnostics'])} diagnostics...")
        client.write_diagnostics([d.to_dict() for d in analysis["diagnostics"]])

        # Write top 50 opportunities
        sorted_diagnostics = sorted(
            analysis["diagnostics"],
            key=lambda d: d.opportunity_score,
            reverse=True,
        )
        top_opportunities = sorted_diagnostics[:50]
        print(f"  Writing top {len(top_opportunities)} opportunities...")
        client.write_opportunity_ranking([d.to_dict() for d in top_opportunities])

    # Write placements
    if analysis.get("placements"):
        print(f"  Writing {len(analysis['placements'])} placements...")
        client.write_placements([p.to_dict() for p in analysis["placements"]])

    # Build and write summary
    summary = analysis.get("summary", {})
    categories = summary.get("categories", {})
    prices = summary.get("prices", {})

    summary_record = ASINSummary(
        asin=asin,
        total_keywords=categories.get("total", 0),
        bread_butter_count=categories.get("bread_butter", 0),
        opportunities_count=categories.get("opportunities", 0),
        leaks_count=categories.get("leaks", 0),
        price_flagged_count=prices.get("total_flagged", 0),
        health_score=calculate_health_score(categories, prices),
        last_updated=date.today(),
    )

    print("  Writing summary...")
    client.write_summary([summary_record.to_dict()])


def calculate_health_score(
    categories: dict[str, int],
    prices: dict[str, int],
) -> float:
    """Calculate overall health score (0-100).

    Higher is better:
    - More Bread & Butter keywords = good
    - Fewer Leaks = good
    - Fewer price flags = good
    """
    total = categories.get("total", 0)
    if total == 0:
        return 0.0

    bread_butter = categories.get("bread_butter", 0)
    leaks = categories.get("leaks", 0)
    price_flagged = prices.get("total_flagged", 0)

    # Base score from bread & butter percentage
    base_score = (bread_butter / total) * 100

    # Penalty for leaks
    leak_penalty = (leaks / total) * 30

    # Penalty for price issues
    price_penalty = min((price_flagged / total) * 20, 20)

    return max(0, min(100, base_score - leak_penalty - price_penalty))


def process_asin(
    config: AppConfig,
    asin: str,
    weeks: int = 12,
) -> None:
    """Process a single ASIN: fetch, analyze, write."""
    print(f"\n{'='*60}")
    print(f"Processing ASIN: {asin}")
    print('='*60)

    # Fetch data
    snapshots = fetch_sqp_data(config, asin, weeks)

    if not snapshots:
        print(f"No data available for {asin}")
        return

    # Analyze
    analysis = analyze_snapshots(config, snapshots)

    # Print summary
    summary = analysis.get("summary", {})
    categories = summary.get("categories", {})
    diagnostics_summary = summary.get("diagnostics", {})
    placements_summary = summary.get("placements", {})
    print(f"\nAnalysis Summary:")
    print(f"  Total keywords: {categories.get('total', 0)}")
    print(f"  Bread & Butter: {categories.get('bread_butter', 0)}")
    print(f"  Opportunities: {categories.get('opportunities', 0)}")
    print(f"  Leaks: {categories.get('leaks', 0)}")
    print(f"  Price flagged: {summary.get('prices', {}).get('total_flagged', 0)}")
    print(f"\nDiagnostics:")
    print(f"  Ghost (not ranking): {diagnostics_summary.get('ghost', 0)}")
    print(f"  Window Shopper (low CTR): {diagnostics_summary.get('window_shopper', 0)}")
    print(f"  Price Problem: {diagnostics_summary.get('price_problem', 0)}")
    print(f"  Healthy: {diagnostics_summary.get('healthy', 0)}")
    print(f"\nPlacements:")
    print(f"  Title: {placements_summary.get('title', 0)}")
    print(f"  Bullets: {placements_summary.get('bullets', 0)}")
    print(f"  Backend: {placements_summary.get('backend', 0)}")
    print(f"  Description: {placements_summary.get('description', 0)}")

    # Write to sheets
    write_results_to_sheets(config, asin, analysis, snapshots)

    print(f"\nCompleted processing {asin}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Amazon SQP Analyzer - Search Query Performance analysis tool"
    )
    parser.add_argument(
        "--test-api",
        action="store_true",
        help="Test SP-API connection only",
    )
    parser.add_argument(
        "--test-sheets",
        action="store_true",
        help="Test Google Sheets connection only",
    )
    parser.add_argument(
        "--asin",
        type=str,
        help="Process a single ASIN instead of reading from sheet",
    )
    parser.add_argument(
        "--weeks",
        type=int,
        default=12,
        help="Number of weeks to fetch (default: 12)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and analyze but don't write to sheets",
    )
    parser.add_argument(
        "--import-csv",
        type=str,
        metavar="PATH",
        help="Import SQP data from CSV file instead of API",
    )
    parser.add_argument(
        "--import-excel",
        type=str,
        metavar="PATH",
        help="Import SQP data from Excel file instead of API",
    )
    parser.add_argument(
        "--import-folder",
        type=str,
        metavar="PATH",
        help="Import all CSV/Excel files from folder",
    )
    parser.add_argument(
        "--week-date",
        type=str,
        metavar="YYYY-MM-DD",
        help="Week date for imported data (auto-detected from filename if not provided)",
    )

    args = parser.parse_args()

    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}")
        print("Make sure .env file exists with required credentials")
        sys.exit(1)

    # Test connections if requested
    if args.test_api:
        success = test_api_connection(config)
        sys.exit(0 if success else 1)

    if args.test_sheets:
        success = test_sheets_connection(config)
        sys.exit(0 if success else 1)

    # Parse week date if provided
    week_date = None
    if args.week_date:
        try:
            week_date = date.fromisoformat(args.week_date)
        except ValueError:
            print(f"Invalid date format: {args.week_date}. Use YYYY-MM-DD")
            sys.exit(1)

    # Check for import mode
    import_path = args.import_csv or args.import_excel or args.import_folder
    if import_path:
        # Import mode - requires ASIN
        if not args.asin:
            print("Error: --asin is required when importing CSV/Excel data")
            sys.exit(1)

        asin = args.asin.upper()

        print(f"\n{'='*60}")
        print(f"Import Mode - ASIN: {asin}")
        print('='*60)

        try:
            snapshots = import_sqp_data(import_path, asin, week_date)
        except Exception as e:
            print(f"Error importing data: {e}")
            sys.exit(1)

        if not snapshots:
            print("No data imported")
            sys.exit(1)

        # Analyze
        analysis = analyze_snapshots(config, snapshots)

        # Print summary
        summary = analysis.get("summary", {})
        categories = summary.get("categories", {})
        diagnostics_summary = summary.get("diagnostics", {})
        placements_summary = summary.get("placements", {})
        print(f"\nAnalysis Summary:")
        print(f"  Total keywords: {categories.get('total', 0)}")
        print(f"  Bread & Butter: {categories.get('bread_butter', 0)}")
        print(f"  Opportunities: {categories.get('opportunities', 0)}")
        print(f"  Leaks: {categories.get('leaks', 0)}")
        print(f"  Price flagged: {summary.get('prices', {}).get('total_flagged', 0)}")
        print(f"\nDiagnostics:")
        print(f"  Ghost (not ranking): {diagnostics_summary.get('ghost', 0)}")
        print(f"  Window Shopper (low CTR): {diagnostics_summary.get('window_shopper', 0)}")
        print(f"  Price Problem: {diagnostics_summary.get('price_problem', 0)}")
        print(f"  Healthy: {diagnostics_summary.get('healthy', 0)}")
        print(f"\nPlacements:")
        print(f"  Title: {placements_summary.get('title', 0)}")
        print(f"  Bullets: {placements_summary.get('bullets', 0)}")
        print(f"  Backend: {placements_summary.get('backend', 0)}")
        print(f"  Description: {placements_summary.get('description', 0)}")

        if not args.dry_run:
            write_results_to_sheets(config, asin, analysis, snapshots)

        print("\nImport complete!")
        sys.exit(0)

    # API mode - determine ASINs to process
    if args.asin:
        asins = [args.asin.upper()]
    else:
        # Read from Google Sheet
        print("Reading ASINs from Google Sheet...")
        sheets_client = SheetsClient(config.sheets)
        asins = sheets_client.get_active_asins()
        print(f"Found {len(asins)} active ASINs")

    if not asins:
        print("No ASINs to process")
        sys.exit(0)

    # Process each ASIN
    for asin in asins:
        try:
            if args.dry_run:
                snapshots = fetch_sqp_data(config, asin, args.weeks)
                if snapshots:
                    analysis = analyze_snapshots(config, snapshots)
                    print(f"Dry run complete for {asin}")
                    print(f"  Would write {len(analysis.get('categorized', []))} keywords")
            else:
                process_asin(config, asin, args.weeks)
        except Exception as e:
            print(f"Error processing {asin}: {e}")
            continue

    print("\nAll ASINs processed!")


if __name__ == "__main__":
    main()
