"""Core data models for SQP Analyzer."""

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Any


class KeywordCategory(Enum):
    """Keyword categorization."""
    BREAD_BUTTER = "bread_butter"
    OPPORTUNITY = "opportunity"
    LEAK = "leak"
    UNCATEGORIZED = "uncategorized"


class TrendDirection(Enum):
    """Trend direction indicator."""
    GROWING = "growing"
    STABLE = "stable"
    DECLINING = "declining"


class PriceSeverity(Enum):
    """Price competitiveness severity levels."""
    OK = "ok"
    WARNING = "warning"
    CRITICAL = "critical"


class RankStatus(Enum):
    """Estimated page position based on impression share."""
    TOP_3 = "top_3"           # >20% imp share
    PAGE_1_HIGH = "page_1_high"  # 10-20%
    PAGE_1_LOW = "page_1_low"    # 1-10%
    INVISIBLE = "invisible"       # <1%


class DiagnosticType(Enum):
    """Keyword diagnostic types for root cause analysis."""
    GHOST = "ghost"              # High volume, no impressions
    WINDOW_SHOPPER = "window_shopper"  # Seen but not clicked
    PRICE_PROBLEM = "price_problem"    # Clicked but not bought
    HEALTHY = "healthy"


class PlacementTarget(Enum):
    """Recommended keyword placement location."""
    TITLE = "title"
    BULLETS = "bullets"
    BACKEND = "backend"
    DESCRIPTION = "description"


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
class CategorizedKeyword:
    """A keyword with its category and metrics."""
    search_query: str
    asin: str
    category: KeywordCategory
    action: str = ""

    # Latest metrics
    impressions_share: float = 0.0
    clicks_share: float = 0.0
    purchases_share: float = 0.0
    search_volume: int = 0

    # Pricing
    asin_price: float | None = None
    market_price: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for sheet output."""
        return {
            "Search Query": self.search_query,
            "ASIN": self.asin,
            "Category": self.category.value,
            "Imp Share": self.impressions_share,
            "Click Share": self.clicks_share,
            "Purchase Share": self.purchases_share,
            "Volume": self.search_volume,
            "Recommended Action": self.action,
        }


@dataclass
class TrendRecord:
    """12-week trend data for a keyword."""
    search_query: str
    asin: str
    weekly_purchase_shares: dict[str, float] = field(default_factory=dict)
    trend_direction: TrendDirection = TrendDirection.STABLE
    growth_percent: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for sheet output."""
        result = {
            "Search Query": self.search_query,
            "ASIN": self.asin,
            "Trend Direction": self.trend_direction.value,
            "Growth %": self.growth_percent,
        }
        # Add weekly shares
        for week, share in self.weekly_purchase_shares.items():
            result[week] = share
        return result


@dataclass
class PriceFlag:
    """Price competitiveness flag for a keyword."""
    search_query: str
    asin: str
    asin_price: float
    market_price: float
    price_diff_percent: float
    severity: PriceSeverity
    impressions_share: float = 0.0
    purchases_share: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for sheet output."""
        return {
            "Search Query": self.search_query,
            "ASIN": self.asin,
            "ASIN Price": self.asin_price,
            "Market Price": self.market_price,
            "Price Diff %": self.price_diff_percent,
            "Severity": self.severity.value,
            "Imp Share": self.impressions_share,
            "Purchase Share": self.purchases_share,
        }


@dataclass
class ASINSummary:
    """Summary statistics for an ASIN."""
    asin: str
    product_name: str = ""
    total_keywords: int = 0
    bread_butter_count: int = 0
    opportunities_count: int = 0
    leaks_count: int = 0
    price_flagged_count: int = 0
    health_score: float = 0.0
    last_updated: date | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for sheet output."""
        return {
            "ASIN": self.asin,
            "Product Name": self.product_name,
            "Total Keywords": self.total_keywords,
            "Bread & Butter": self.bread_butter_count,
            "Opportunities": self.opportunities_count,
            "Leaks": self.leaks_count,
            "Price Flagged": self.price_flagged_count,
            "Health Score": self.health_score,
            "Last Updated": self.last_updated.isoformat() if self.last_updated else "",
        }


@dataclass
class KeywordDiagnostic:
    """Diagnostic analysis for a keyword."""
    search_query: str
    asin: str
    diagnostic_type: DiagnosticType
    rank_status: RankStatus
    opportunity_score: float
    search_volume: int = 0
    impressions_share: float = 0.0
    clicks_share: float = 0.0
    purchases_share: float = 0.0
    recommended_fix: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for sheet output."""
        return {
            "Search Query": self.search_query,
            "ASIN": self.asin,
            "Diagnostic": self.diagnostic_type.value,
            "Rank Status": self.rank_status.value,
            "Opportunity Score": self.opportunity_score,
            "Volume": self.search_volume,
            "Imp Share": self.impressions_share,
            "Click Share": self.clicks_share,
            "Purchase Share": self.purchases_share,
            "Recommended Fix": self.recommended_fix,
        }


@dataclass
class KeywordPlacement:
    """Keyword placement recommendation."""
    search_query: str
    asin: str
    placement: PlacementTarget
    priority: int
    search_volume: int = 0
    clicks_share: float = 0.0
    reasoning: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for sheet output."""
        return {
            "Search Query": self.search_query,
            "ASIN": self.asin,
            "Placement": self.placement.value,
            "Priority": self.priority,
            "Volume": self.search_volume,
            "Click Share": self.clicks_share,
            "Reasoning": self.reasoning,
        }
