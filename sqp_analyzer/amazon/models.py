"""Data models for SP-API responses."""

from dataclasses import dataclass, field
from datetime import date
from typing import Any


@dataclass
class SearchQueryMetrics:
    """Metrics for a single search query."""

    search_query: str
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

    # Metadata
    asin: str = ""
    reporting_date: date | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "search_query": self.search_query,
            "search_volume": self.search_volume,
            "search_score": self.search_score,
            "impressions_total": self.impressions_total,
            "impressions_asin": self.impressions_asin,
            "impressions_share": self.impressions_share,
            "clicks_total": self.clicks_total,
            "clicks_asin": self.clicks_asin,
            "clicks_share": self.clicks_share,
            "purchases_total": self.purchases_total,
            "purchases_asin": self.purchases_asin,
            "purchases_share": self.purchases_share,
            "asin_price": self.asin_price,
            "market_price": self.market_price,
            "asin": self.asin,
            "reporting_date": self.reporting_date.isoformat()
            if self.reporting_date
            else None,
        }


@dataclass
class SQPReport:
    """Search Query Performance report for a date range."""

    asin: str
    start_date: date
    end_date: date
    marketplace_id: str
    queries: list[SearchQueryMetrics] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "asin": self.asin,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "marketplace_id": self.marketplace_id,
            "queries": [q.to_dict() for q in self.queries],
        }


@dataclass
class APIResponse:
    """Generic SP-API response wrapper."""

    success: bool
    data: Any | None = None
    error_code: str | None = None
    error_message: str | None = None

    @classmethod
    def from_success(cls, data: Any) -> "APIResponse":
        """Create successful response."""
        return cls(success=True, data=data)

    @classmethod
    def from_error(cls, code: str, message: str) -> "APIResponse":
        """Create error response."""
        return cls(success=False, error_code=code, error_message=message)
