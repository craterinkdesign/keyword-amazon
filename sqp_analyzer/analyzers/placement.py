"""Placement recommender for SQP keywords."""

from ..config import Thresholds
from ..models import (
    KeywordPlacement,
    PlacementTarget,
    SQPRecord,
    WeeklySnapshot,
)


class PlacementRecommender:
    """Recommends keyword placement locations based on volume and performance."""

    def __init__(self, thresholds: Thresholds):
        self.thresholds = thresholds

    def _calculate_percentile(self, value: int, all_values: list[int]) -> float:
        """Calculate percentile of a value within a list.

        Returns the percentage of values that are less than or equal to the given value.
        """
        if not all_values:
            return 0.0
        if len(all_values) == 1:
            return 100.0 if value >= all_values[0] else 0.0
        count_below = sum(1 for v in all_values if v <= value)
        return (count_below / len(all_values)) * 100

    def recommend_placement(
        self,
        record: SQPRecord,
        volume_percentile: float,
    ) -> tuple[PlacementTarget, str]:
        """Recommend placement location for a keyword.

        Args:
            record: SQP record with keyword data
            volume_percentile: Keyword's volume percentile (0-100)

        Returns:
            Tuple of (PlacementTarget, reasoning string)
        """
        # TITLE: Top 5% volume OR (top 20% volume + good click share)
        if volume_percentile >= self.thresholds.title_top_volume_percentile:
            return (
                PlacementTarget.TITLE,
                f"Top {100 - volume_percentile:.0f}% volume - must be in title"
            )

        if (
            volume_percentile >= self.thresholds.title_min_volume_percentile
            and record.clicks_share >= self.thresholds.title_min_click_share
        ):
            return (
                PlacementTarget.TITLE,
                f"High volume ({volume_percentile:.0f}th percentile) with "
                f"{record.clicks_share:.1f}% click share"
            )

        # BULLETS: 50-80% volume percentile
        if volume_percentile >= self.thresholds.bullets_min_volume_percentile:
            return (
                PlacementTarget.BULLETS,
                f"Mid-high volume ({volume_percentile:.0f}th percentile) - "
                "include in bullet points"
            )

        # BACKEND: 20-50% volume percentile
        if volume_percentile >= self.thresholds.backend_min_volume_percentile:
            return (
                PlacementTarget.BACKEND,
                f"Moderate volume ({volume_percentile:.0f}th percentile) - "
                "add to backend keywords"
            )

        # DESCRIPTION: Bottom 20%
        return (
            PlacementTarget.DESCRIPTION,
            f"Lower volume ({volume_percentile:.0f}th percentile) - "
            "consider for description or A+ content"
        )

    def analyze(self, snapshot: WeeklySnapshot) -> list[KeywordPlacement]:
        """Analyze all keywords and recommend placements.

        Args:
            snapshot: Weekly snapshot to analyze

        Returns:
            List of KeywordPlacement objects with priorities
        """
        if not snapshot.records:
            return []

        # Calculate volume percentiles
        volumes = [r.search_volume for r in snapshot.records]

        placements = []
        for record in snapshot.records:
            volume_percentile = self._calculate_percentile(record.search_volume, volumes)
            placement, reasoning = self.recommend_placement(record, volume_percentile)

            placements.append(
                KeywordPlacement(
                    search_query=record.search_query,
                    asin=record.asin,
                    placement=placement,
                    priority=0,  # Will be set below
                    search_volume=record.search_volume,
                    clicks_share=record.clicks_share,
                    reasoning=reasoning,
                )
            )

        # Assign priorities within each placement category (1 = highest volume)
        for target in PlacementTarget:
            category_placements = [p for p in placements if p.placement == target]
            category_placements.sort(key=lambda p: p.search_volume, reverse=True)
            for i, p in enumerate(category_placements, 1):
                p.priority = i

        # Sort by placement priority (TITLE first) then by priority number
        placement_order = {
            PlacementTarget.TITLE: 0,
            PlacementTarget.BULLETS: 1,
            PlacementTarget.BACKEND: 2,
            PlacementTarget.DESCRIPTION: 3,
        }
        placements.sort(key=lambda p: (placement_order[p.placement], p.priority))

        return placements

    def summarize(self, placements: list[KeywordPlacement]) -> dict[str, int]:
        """Summarize placement counts."""
        counts = {
            "total": len(placements),
            "title": 0,
            "bullets": 0,
            "backend": 0,
            "description": 0,
        }
        for p in placements:
            if p.placement == PlacementTarget.TITLE:
                counts["title"] += 1
            elif p.placement == PlacementTarget.BULLETS:
                counts["bullets"] += 1
            elif p.placement == PlacementTarget.BACKEND:
                counts["backend"] += 1
            else:
                counts["description"] += 1
        return counts
