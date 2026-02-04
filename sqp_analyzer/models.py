"""Core data models for SQP Analyzer - Quarterly Tracker."""

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Any


class RankStatus(Enum):
    """Estimated page position based on impression share."""
    TOP_3 = "top_3"           # >20% imp share
    PAGE_1_HIGH = "page_1_high"  # 10-20%
    PAGE_1_LOW = "page_1_low"    # 1-10%
    INVISIBLE = "invisible"       # <1%


class DiagnosticType(Enum):
    """Keyword diagnostic types for opportunity score calculation."""
    GHOST = "ghost"              # High volume, no impressions
    WINDOW_SHOPPER = "window_shopper"  # Seen but not clicked
    PRICE_PROBLEM = "price_problem"    # Clicked but not bought
    HEALTHY = "healthy"


@dataclass
class SQPRecord:
    """Single SQP data record for a search query."""
    search_query: str
    asin: str
    week_date: date

    # Volume metrics
    search_volume: int = 0
    search_score: float = 0.0

    # Impressions
    impressions_total: int = 0
    impressions_asin: int = 0
    impressions_share: float = 0.0

    # Clicks
    clicks_total: int = 0
    clicks_asin: int = 0
    clicks_share: float = 0.0

    # Purchases
    purchases_total: int = 0
    purchases_asin: int = 0
    purchases_share: float = 0.0

    # Pricing
    asin_price: float | None = None
    market_price: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for sheet output."""
        return {
            "Search Query": self.search_query,
            "ASIN": self.asin,
            "Week": self.week_date.isoformat(),
            "Volume": self.search_volume,
            "Score": self.search_score,
            "Imp Total": self.impressions_total,
            "Imp ASIN": self.impressions_asin,
            "Imp Share": self.impressions_share,
            "Click Total": self.clicks_total,
            "Click ASIN": self.clicks_asin,
            "Click Share": self.clicks_share,
            "Purchase Total": self.purchases_total,
            "Purchase ASIN": self.purchases_asin,
            "Purchase Share": self.purchases_share,
            "ASIN Price": self.asin_price,
            "Market Price": self.market_price,
        }


@dataclass
class WeeklySnapshot:
    """Weekly snapshot of all SQP data for an ASIN."""
    asin: str
    week_date: date
    records: list[SQPRecord] = field(default_factory=list)

    def get_records_by_query(self) -> dict[str, SQPRecord]:
        """Get records indexed by search query."""
        return {r.search_query: r for r in self.records}


@dataclass
class ListingContent:
    """Listing content for keyword placement detection."""
    asin: str
    sku: str
    title: str = ""
    bullets: list[str] = field(default_factory=list)
    backend_keywords: list[str] = field(default_factory=list)

    def contains_keyword(self, keyword: str) -> tuple[bool, bool]:
        """Check if keyword is in title and/or backend.

        Returns:
            Tuple of (in_title, in_backend)
        """
        keyword_lower = keyword.lower()
        in_title = keyword_lower in self.title.lower()

        # Check backend keywords
        backend_text = " ".join(self.backend_keywords).lower()
        in_backend = keyword_lower in backend_text

        return in_title, in_backend


@dataclass
class QuarterlyKeyword:
    """A keyword tracked for the quarter with weekly metrics."""
    rank: int
    keyword: str
    in_title: bool = False
    in_backend: bool = False
    weekly_metrics: dict[str, dict[str, Any]] = field(default_factory=dict)
    alerts: list[str] = field(default_factory=list)

    def to_row(self, weeks_to_include: list[str]) -> list[Any]:
        """Convert to a row for Google Sheets.

        Args:
            weeks_to_include: List of week labels to include (e.g., ['W01', 'W02'])

        Returns:
            List of values for the row
        """
        row = [
            self.rank,
            self.keyword,
            "YES" if self.in_title else "NO",
            "YES" if self.in_backend else "NO",
        ]

        # Add weekly metrics
        for week in weeks_to_include:
            metrics = self.weekly_metrics.get(week, {})
            row.extend([
                metrics.get("volume", ""),
                metrics.get("imp_share", ""),
                metrics.get("click_share", ""),
                metrics.get("purchase_share", ""),
                metrics.get("opportunity_score", ""),
                metrics.get("rank_status", ""),
            ])

        # Add alerts
        row.append(" | ".join(self.alerts) if self.alerts else "")

        return row
