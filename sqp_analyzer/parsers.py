"""Parsers for API responses and raw data exports."""

from datetime import date
from typing import Any

from .amazon.models import SQPReport, SearchQueryMetrics
from .models import SQPRecord, WeeklySnapshot


def parse_api_report(report: SQPReport) -> WeeklySnapshot:
    """Parse SP-API SQPReport into WeeklySnapshot.

    Args:
        report: SQPReport from Brand Analytics API

    Returns:
        WeeklySnapshot with all search query records
    """
    snapshot = WeeklySnapshot(
        asin=report.asin,
        week_date=report.start_date,
    )

    for query in report.queries:
        record = SQPRecord(
            search_query=query.search_query,
            asin=report.asin,
            week_date=report.start_date,
            search_volume=query.search_volume,
            search_score=query.search_score,
            impressions_total=query.impressions_total,
            impressions_asin=query.impressions_asin,
            impressions_share=query.impressions_share,
            clicks_total=query.clicks_total,
            clicks_asin=query.clicks_asin,
            clicks_share=query.clicks_share,
            purchases_total=query.purchases_total,
            purchases_asin=query.purchases_asin,
            purchases_share=query.purchases_share,
            asin_price=query.asin_price,
            market_price=query.market_price,
        )
        snapshot.records.append(record)

    return snapshot


def parse_raw_export(
    data: list[dict[str, Any]],
    asin: str,
    week_date: date,
) -> WeeklySnapshot:
    """Parse raw SQP export data (from manual downloads).

    Handles various column naming conventions.

    Args:
        data: List of row dictionaries from CSV/Excel
        asin: Parent ASIN for this data
        week_date: Week date for this data

    Returns:
        WeeklySnapshot with parsed records
    """
    snapshot = WeeklySnapshot(asin=asin, week_date=week_date)

    for row in data:
        # Normalize column names
        normalized = _normalize_columns(row)

        record = SQPRecord(
            search_query=normalized.get("search_query", ""),
            asin=asin,
            week_date=week_date,
            search_volume=_parse_int(normalized.get("search_volume", 0)),
            search_score=_parse_float(normalized.get("search_score", 0)),
            impressions_total=_parse_int(normalized.get("impressions_total", 0)),
            impressions_asin=_parse_int(normalized.get("impressions_asin", 0)),
            impressions_share=_parse_float(normalized.get("impressions_share", 0)),
            clicks_total=_parse_int(normalized.get("clicks_total", 0)),
            clicks_asin=_parse_int(normalized.get("clicks_asin", 0)),
            clicks_share=_parse_float(normalized.get("clicks_share", 0)),
            purchases_total=_parse_int(normalized.get("purchases_total", 0)),
            purchases_asin=_parse_int(normalized.get("purchases_asin", 0)),
            purchases_share=_parse_float(normalized.get("purchases_share", 0)),
            asin_price=_parse_float(normalized.get("asin_price")),
            market_price=_parse_float(normalized.get("market_price")),
        )

        if record.search_query:
            snapshot.records.append(record)

    return snapshot


def _normalize_columns(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize column names to standard format."""
    # Exact mappings first (checked before partial matches)
    exact_mappings = {
        # Search query - exact matches
        "search query": "search_query",
        "searchquery": "search_query",
        "query": "search_query",
        "keyword": "search_query",
        # Volume - exact matches
        "search query volume": "search_volume",
        "search volume": "search_volume",
        "searchvolume": "search_volume",
        "volume": "search_volume",
        "sfr": "search_volume",
        # Score - exact matches
        "search query score": "search_score",
        "search score": "search_score",
        "searchscore": "search_score",
        "score": "search_score",
        # Amazon export format - exact column names
        "impressions: total count": "impressions_total",
        "impressions: asin count": "impressions_asin",
        "impressions: asin share %": "impressions_share",
        "clicks: total count": "clicks_total",
        "clicks: asin count": "clicks_asin",
        "clicks: asin share %": "clicks_share",
        "clicks: price (median)": "market_price",
        "clicks: asin price (median)": "asin_price",
        "purchases: total count": "purchases_total",
        "purchases: asin count": "purchases_asin",
        "purchases: asin share %": "purchases_share",
        "purchases: price (median)": "market_price",
        "purchases: asin price (median)": "asin_price",
        # Legacy format
        "impressions - total count": "impressions_total",
        "impressions - brand count": "impressions_asin",
        "impressions - brand share": "impressions_share",
        "clicks - total count": "clicks_total",
        "clicks - brand count": "clicks_asin",
        "clicks - brand share": "clicks_share",
        "purchases - total count": "purchases_total",
        "purchases - brand count": "purchases_asin",
        "purchases - brand share": "purchases_share",
        # Short forms
        "imp total": "impressions_total",
        "imp asin": "impressions_asin",
        "imp share": "impressions_share",
        "click total": "clicks_total",
        "click asin": "clicks_asin",
        "click share": "clicks_share",
        "purchase total": "purchases_total",
        "purchase asin": "purchases_asin",
        "purchase share": "purchases_share",
        # Pricing
        "your price": "asin_price",
        "asin price": "asin_price",
        "market price": "market_price",
        "median price": "market_price",
    }

    # Partial match patterns (for fallback)
    partial_patterns = {
        "impressions": {
            "total": "impressions_total",
            "asin": "impressions_asin",
            "share": "impressions_share",
        },
        "clicks": {
            "total": "clicks_total",
            "asin count": "clicks_asin",
            "asin share": "clicks_share",
            "asin price": "asin_price",
            "price (median)": "market_price",
        },
        "purchases": {
            "total": "purchases_total",
            "asin count": "purchases_asin",
            "asin share": "purchases_share",
        },
    }

    normalized = {}
    for key, value in row.items():
        key_lower = key.lower().strip()

        # Check exact mapping first
        if key_lower in exact_mappings:
            target = exact_mappings[key_lower]
            # Don't overwrite if already set (prefer earlier columns)
            if target not in normalized:
                normalized[target] = value
            continue

        # Check partial patterns
        matched = False
        for category, patterns in partial_patterns.items():
            if category in key_lower:
                for pattern, target in patterns.items():
                    if pattern in key_lower:
                        if target not in normalized:
                            normalized[target] = value
                        matched = True
                        break
            if matched:
                break

        if not matched:
            # Keep original with normalized key
            normalized[key_lower.replace(" ", "_").replace(":", "")] = value

    return normalized


def _parse_int(value: Any) -> int:
    """Parse value to integer."""
    if value is None or value == "":
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    try:
        # Handle strings with commas or percentage signs
        cleaned = str(value).replace(",", "").replace("%", "").strip()
        return int(float(cleaned))
    except (ValueError, TypeError):
        return 0


def _parse_float(value: Any) -> float | None:
    """Parse value to float."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        # Handle strings with commas, percentages, or currency symbols
        cleaned = str(value).replace(",", "").replace("%", "").replace("$", "").strip()
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def aggregate_child_asins(snapshots: list[WeeklySnapshot]) -> WeeklySnapshot:
    """Aggregate multiple child ASIN snapshots into one.

    When parent ASIN data includes child variations, this combines
    their metrics into a single view.

    Args:
        snapshots: List of snapshots (typically for child ASINs)

    Returns:
        Single aggregated WeeklySnapshot
    """
    if not snapshots:
        raise ValueError("No snapshots to aggregate")

    # Use first snapshot's metadata
    base = snapshots[0]
    aggregated = WeeklySnapshot(
        asin=base.asin,
        week_date=base.week_date,
    )

    # Group records by search query
    query_records: dict[str, list[SQPRecord]] = {}
    for snapshot in snapshots:
        for record in snapshot.records:
            if record.search_query not in query_records:
                query_records[record.search_query] = []
            query_records[record.search_query].append(record)

    # Aggregate each query's records
    for query, records in query_records.items():
        # Sum ASIN-specific metrics, keep totals from first record
        first = records[0]
        combined = SQPRecord(
            search_query=query,
            asin=base.asin,
            week_date=base.week_date,
            search_volume=first.search_volume,
            search_score=first.search_score,
            impressions_total=first.impressions_total,
            clicks_total=first.clicks_total,
            purchases_total=first.purchases_total,
            # Sum ASIN counts across all records
            impressions_asin=sum(r.impressions_asin for r in records),
            clicks_asin=sum(r.clicks_asin for r in records),
            purchases_asin=sum(r.purchases_asin for r in records),
            # Average prices
            asin_price=_avg_prices([r.asin_price for r in records]),
            market_price=first.market_price,
        )

        # Recalculate shares
        if combined.impressions_total > 0:
            combined.impressions_share = (
                combined.impressions_asin / combined.impressions_total * 100
            )
        if combined.clicks_total > 0:
            combined.clicks_share = (
                combined.clicks_asin / combined.clicks_total * 100
            )
        if combined.purchases_total > 0:
            combined.purchases_share = (
                combined.purchases_asin / combined.purchases_total * 100
            )

        aggregated.records.append(combined)

    return aggregated


def _avg_prices(prices: list[float | None]) -> float | None:
    """Calculate average of non-None prices."""
    valid_prices = [p for p in prices if p is not None]
    if not valid_prices:
        return None
    return sum(valid_prices) / len(valid_prices)
