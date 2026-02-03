"""Price benchmarking and competitiveness analysis."""

from ..config import Thresholds
from ..models import PriceFlag, PriceSeverity, SQPRecord, WeeklySnapshot


class PriceBenchmark:
    """Analyze price competitiveness against market."""

    def __init__(self, thresholds: Thresholds):
        self.thresholds = thresholds

    def analyze(self, snapshot: WeeklySnapshot) -> list[PriceFlag]:
        """Analyze price competitiveness for all keywords in snapshot.

        Flags keywords where ASIN price is significantly above market price,
        which may explain poor conversion despite good visibility.

        Args:
            snapshot: WeeklySnapshot with price data

        Returns:
            List of PriceFlag objects (only flagged keywords)
        """
        flags = []

        for record in snapshot.records:
            flag = self._analyze_record(record)
            if flag is not None:
                flags.append(flag)

        # Sort by severity (critical first) then by price diff
        flags.sort(
            key=lambda f: (
                0 if f.severity == PriceSeverity.CRITICAL else 1,
                -f.price_diff_percent,
            )
        )

        return flags

    def _analyze_record(self, record: SQPRecord) -> PriceFlag | None:
        """Analyze price competitiveness for a single record.

        Returns PriceFlag if flagged, None otherwise.
        """
        # Need both prices to compare
        if record.asin_price is None or record.market_price is None:
            return None

        if record.market_price <= 0:
            return None

        # Calculate price difference percentage
        price_diff = (
            (record.asin_price - record.market_price) / record.market_price * 100
        )

        # Determine severity
        if price_diff >= self.thresholds.price_critical_threshold:
            severity = PriceSeverity.CRITICAL
        elif price_diff >= self.thresholds.price_warning_threshold:
            severity = PriceSeverity.WARNING
        else:
            return None  # Not flagged

        return PriceFlag(
            search_query=record.search_query,
            asin=record.asin,
            asin_price=record.asin_price,
            market_price=record.market_price,
            price_diff_percent=price_diff,
            severity=severity,
            impressions_share=record.impressions_share,
            purchases_share=record.purchases_share,
        )

    def get_critical_flags(self, flags: list[PriceFlag]) -> list[PriceFlag]:
        """Get only critical severity flags."""
        return [f for f in flags if f.severity == PriceSeverity.CRITICAL]

    def get_warning_flags(self, flags: list[PriceFlag]) -> list[PriceFlag]:
        """Get only warning severity flags."""
        return [f for f in flags if f.severity == PriceSeverity.WARNING]

    def get_priced_out_keywords(
        self, flags: list[PriceFlag], min_imp_share: float = 5.0
    ) -> list[PriceFlag]:
        """Get keywords where price is likely causing lost sales.

        These are keywords where:
        - ASIN has decent visibility (impression share)
        - But price is significantly above market
        - Potentially explaining low purchase share

        Args:
            flags: List of PriceFlag objects
            min_imp_share: Minimum impression share to consider

        Returns:
            Filtered list of price flags
        """
        return [
            f for f in flags
            if f.impressions_share >= min_imp_share
        ]

    def summarize(self, flags: list[PriceFlag]) -> dict[str, int]:
        """Get summary counts."""
        return {
            "total_flagged": len(flags),
            "critical": len(self.get_critical_flags(flags)),
            "warning": len(self.get_warning_flags(flags)),
        }
