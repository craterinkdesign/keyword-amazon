"""12-week trend tracking for keyword performance."""

from datetime import date
from statistics import mean

from ..models import SQPRecord, TrendDirection, TrendRecord, WeeklySnapshot


class TrendTracker:
    """Track 12-week trends in keyword performance."""

    def __init__(self, growth_threshold: float = 10.0):
        """Initialize trend tracker.

        Args:
            growth_threshold: Percentage change to consider as growth/decline
        """
        self.growth_threshold = growth_threshold

    def analyze_trends(
        self, snapshots: list[WeeklySnapshot]
    ) -> list[TrendRecord]:
        """Analyze trends across multiple weekly snapshots.

        Tracks Purchase Share over 12 weeks to identify:
        - Growing keywords (opportunity to capitalize)
        - Declining keywords (need intervention)
        - Stable keywords (maintain current strategy)

        Args:
            snapshots: List of WeeklySnapshots, newest first

        Returns:
            List of TrendRecord objects
        """
        if not snapshots:
            return []

        # Sort by date (oldest first for trend calculation)
        sorted_snapshots = sorted(snapshots, key=lambda s: s.week_date)

        # Group records by search query across all weeks
        query_weeks: dict[str, dict[date, SQPRecord]] = {}

        for snapshot in sorted_snapshots:
            for record in snapshot.records:
                if record.search_query not in query_weeks:
                    query_weeks[record.search_query] = {}
                query_weeks[record.search_query][snapshot.week_date] = record

        # Build trend records
        trends = []
        for query, weeks_data in query_weeks.items():
            trend = self._calculate_trend(query, weeks_data, sorted_snapshots[0].asin)
            trends.append(trend)

        # Sort by growth percentage (descending)
        trends.sort(key=lambda t: t.growth_percent, reverse=True)

        return trends

    def _calculate_trend(
        self,
        query: str,
        weeks_data: dict[date, SQPRecord],
        asin: str,
    ) -> TrendRecord:
        """Calculate trend for a single search query."""
        # Sort weeks chronologically
        sorted_weeks = sorted(weeks_data.keys())

        # Build weekly purchase share dict
        weekly_shares = {}
        for i, week_date in enumerate(sorted_weeks, 1):
            record = weeks_data[week_date]
            week_label = f"Week {i}"
            weekly_shares[week_label] = record.purchases_share

        # Calculate trend direction and growth
        shares = [weeks_data[w].purchases_share for w in sorted_weeks]
        direction, growth = self._analyze_direction(shares)

        return TrendRecord(
            search_query=query,
            asin=asin,
            weekly_purchase_shares=weekly_shares,
            trend_direction=direction,
            growth_percent=growth,
        )

    def _analyze_direction(
        self, shares: list[float]
    ) -> tuple[TrendDirection, float]:
        """Analyze trend direction from share values.

        Uses comparison of recent vs earlier periods.
        """
        if len(shares) < 2:
            return TrendDirection.STABLE, 0.0

        # Compare first half vs second half averages
        mid = len(shares) // 2
        early_avg = mean(shares[:mid]) if shares[:mid] else 0
        recent_avg = mean(shares[mid:]) if shares[mid:] else 0

        if early_avg == 0:
            if recent_avg > 0:
                return TrendDirection.GROWING, 100.0
            return TrendDirection.STABLE, 0.0

        growth = ((recent_avg - early_avg) / early_avg) * 100

        if growth >= self.growth_threshold:
            return TrendDirection.GROWING, growth
        elif growth <= -self.growth_threshold:
            return TrendDirection.DECLINING, growth
        else:
            return TrendDirection.STABLE, growth

    def get_phase_analysis(
        self, snapshots: list[WeeklySnapshot]
    ) -> dict[str, dict[str, list[str]]]:
        """Analyze keywords by growth phase.

        Phases (from plan):
        - Weeks 1-4: Track Impression Share growth (SEO/Ads working)
        - Weeks 5-8: Track Click Share following (Title/Image/Price attractive)
        - Weeks 9-12: Track Purchase Share growth (true ranking success)

        Returns dict with phase names and growing/declining keywords.
        """
        if len(snapshots) < 4:
            return {}

        sorted_snapshots = sorted(snapshots, key=lambda s: s.week_date)

        # Define phases
        phases = {
            "impression_phase": (0, 4, "impressions_share"),
            "click_phase": (4, 8, "clicks_share"),
            "purchase_phase": (8, 12, "purchases_share"),
        }

        results = {}

        for phase_name, (start, end, metric) in phases.items():
            phase_snapshots = sorted_snapshots[start:end]
            if len(phase_snapshots) < 2:
                continue

            # Track each query's metric change
            growing = []
            declining = []

            # Group by query
            query_data: dict[str, list[float]] = {}
            for snapshot in phase_snapshots:
                for record in snapshot.records:
                    if record.search_query not in query_data:
                        query_data[record.search_query] = []
                    query_data[record.search_query].append(
                        getattr(record, metric)
                    )

            for query, values in query_data.items():
                if len(values) < 2:
                    continue
                early = mean(values[: len(values) // 2])
                recent = mean(values[len(values) // 2 :])

                if early > 0:
                    change = ((recent - early) / early) * 100
                    if change >= self.growth_threshold:
                        growing.append(query)
                    elif change <= -self.growth_threshold:
                        declining.append(query)

            results[phase_name] = {
                "growing": growing,
                "declining": declining,
            }

        return results

    def get_growing_keywords(
        self, trends: list[TrendRecord]
    ) -> list[TrendRecord]:
        """Filter to only growing keywords."""
        return [t for t in trends if t.trend_direction == TrendDirection.GROWING]

    def get_declining_keywords(
        self, trends: list[TrendRecord]
    ) -> list[TrendRecord]:
        """Filter to only declining keywords."""
        return [t for t in trends if t.trend_direction == TrendDirection.DECLINING]
