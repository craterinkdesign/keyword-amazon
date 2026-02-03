"""Output formatting utilities for Google Sheets."""

from typing import Any
from datetime import date, datetime


def format_percentage(value: float | None, decimals: int = 2) -> str:
    """Format a decimal as percentage string."""
    if value is None:
        return ""
    return f"{value:.{decimals}f}%"


def format_currency(value: float | None, decimals: int = 2) -> str:
    """Format value as currency."""
    if value is None:
        return ""
    return f"${value:.{decimals}f}"


def format_number(value: int | float | None, decimals: int = 0) -> str:
    """Format number with thousands separator."""
    if value is None:
        return ""
    if decimals == 0:
        return f"{int(value):,}"
    return f"{value:,.{decimals}f}"


def format_date(value: date | datetime | None) -> str:
    """Format date for display."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        value = value.date()
    return value.isoformat()


def format_sqp_record(record: dict[str, Any]) -> dict[str, Any]:
    """Format an SQP record for sheet output."""
    return {
        "Search Query": record.get("search_query", ""),
        "Volume": format_number(record.get("search_volume")),
        "Score": format_number(record.get("search_score"), decimals=2),
        "Imp Total": format_number(record.get("impressions_total")),
        "Imp ASIN": format_number(record.get("impressions_asin")),
        "Imp Share": format_percentage(record.get("impressions_share")),
        "Click Total": format_number(record.get("clicks_total")),
        "Click ASIN": format_number(record.get("clicks_asin")),
        "Click Share": format_percentage(record.get("clicks_share")),
        "Purchase Total": format_number(record.get("purchases_total")),
        "Purchase ASIN": format_number(record.get("purchases_asin")),
        "Purchase Share": format_percentage(record.get("purchases_share")),
        "ASIN Price": format_currency(record.get("asin_price")),
        "Market Price": format_currency(record.get("market_price")),
    }


def format_categorized_keyword(keyword: dict[str, Any]) -> dict[str, Any]:
    """Format a categorized keyword for sheet output."""
    return {
        "Search Query": keyword.get("search_query", ""),
        "ASIN": keyword.get("asin", ""),
        "Category": keyword.get("category", ""),
        "Imp Share": format_percentage(keyword.get("impressions_share")),
        "Click Share": format_percentage(keyword.get("clicks_share")),
        "Purchase Share": format_percentage(keyword.get("purchases_share")),
        "Volume": format_number(keyword.get("search_volume")),
        "Recommended Action": keyword.get("action", ""),
    }


def format_trend_record(record: dict[str, Any]) -> dict[str, Any]:
    """Format a trend record for sheet output."""
    formatted = {
        "Search Query": record.get("search_query", ""),
        "ASIN": record.get("asin", ""),
        "Trend Direction": record.get("trend_direction", ""),
        "Growth %": format_percentage(record.get("growth_percent")),
    }

    # Add week columns
    for key, value in record.items():
        if key.startswith("Week "):
            formatted[key] = format_percentage(value)

    return formatted


def format_price_flag(flag: dict[str, Any]) -> dict[str, Any]:
    """Format a price flag record for sheet output."""
    return {
        "Search Query": flag.get("search_query", ""),
        "ASIN": flag.get("asin", ""),
        "ASIN Price": format_currency(flag.get("asin_price")),
        "Market Price": format_currency(flag.get("market_price")),
        "Price Diff %": format_percentage(flag.get("price_diff_percent")),
        "Severity": flag.get("severity", ""),
        "Imp Share": format_percentage(flag.get("impressions_share")),
        "Purchase Share": format_percentage(flag.get("purchases_share")),
    }


def format_summary_record(record: dict[str, Any]) -> dict[str, Any]:
    """Format a summary record for sheet output."""
    return {
        "ASIN": record.get("asin", ""),
        "Product Name": record.get("product_name", ""),
        "Total Keywords": format_number(record.get("total_keywords")),
        "Bread & Butter": format_number(record.get("bread_butter_count")),
        "Opportunities": format_number(record.get("opportunities_count")),
        "Leaks": format_number(record.get("leaks_count")),
        "Price Flagged": format_number(record.get("price_flagged_count")),
        "Health Score": format_percentage(record.get("health_score")),
        "Last Updated": format_date(record.get("last_updated")),
    }
