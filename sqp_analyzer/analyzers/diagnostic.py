"""Diagnostic analyzer for SQP keywords."""

from ..config import Thresholds
from ..models import (
    DiagnosticType,
    KeywordDiagnostic,
    PriceFlag,
    RankStatus,
    SQPRecord,
    WeeklySnapshot,
)


class DiagnosticAnalyzer:
    """Analyzes keywords for diagnostic issues and opportunities."""

    def __init__(self, thresholds: Thresholds):
        self.thresholds = thresholds

    def get_rank_status(self, impressions_share: float) -> RankStatus:
        """Estimate page position from impression share percentage."""
        if impressions_share >= self.thresholds.rank_top_3_threshold:
            return RankStatus.TOP_3
        elif impressions_share >= self.thresholds.rank_page_1_high_threshold:
            return RankStatus.PAGE_1_HIGH
        elif impressions_share >= self.thresholds.rank_page_1_low_threshold:
            return RankStatus.PAGE_1_LOW
        else:
            return RankStatus.INVISIBLE

    def calculate_opportunity_score(self, record: SQPRecord) -> float:
        """Calculate opportunity score: volume * (1 - imp_share/100).

        Higher score = more untapped potential.
        """
        return record.search_volume * (1 - record.impressions_share / 100)

    def diagnose(
        self,
        record: SQPRecord,
        has_price_flag: bool = False,
    ) -> DiagnosticType:
        """Diagnose keyword issues.

        Priority order:
        1. GHOST: High volume but invisible (not ranking)
        2. WINDOW_SHOPPER: Seen but not clicked (bad image/title)
        3. PRICE_PROBLEM: Clicked but not bought (price too high)
        4. HEALTHY: No issues detected
        """
        # GHOST: High volume, no impressions
        if (
            record.search_volume >= self.thresholds.ghost_min_volume
            and record.impressions_share < self.thresholds.ghost_max_imp_share
        ):
            return DiagnosticType.GHOST

        # WINDOW_SHOPPER: Good impressions but low clicks
        if (
            record.impressions_share >= self.thresholds.window_shopper_min_imp_share
            and record.clicks_share < self.thresholds.window_shopper_max_click_share
        ):
            return DiagnosticType.WINDOW_SHOPPER

        # PRICE_PROBLEM: Has price flag and decent impressions
        if (
            has_price_flag
            and record.impressions_share >= self.thresholds.price_problem_min_imp_share
        ):
            return DiagnosticType.PRICE_PROBLEM

        return DiagnosticType.HEALTHY

    def get_fix_recommendation(self, diagnostic: DiagnosticType) -> str:
        """Get actionable fix recommendation for a diagnostic type."""
        recommendations = {
            DiagnosticType.GHOST: (
                "Not ranking for this keyword. Add to listing (title/bullets/backend) "
                "or run PPC to build relevance."
            ),
            DiagnosticType.WINDOW_SHOPPER: (
                "Customers see but don't click. Improve main image, title, "
                "or review count. Check competitor positioning."
            ),
            DiagnosticType.PRICE_PROBLEM: (
                "Price is above market. Consider price adjustment, bundle offers, "
                "or highlight value proposition in listing."
            ),
            DiagnosticType.HEALTHY: "No issues detected. Maintain current strategy.",
        }
        return recommendations.get(diagnostic, "")

    def analyze(
        self,
        snapshot: WeeklySnapshot,
        price_flags: list[PriceFlag] | None = None,
    ) -> list[KeywordDiagnostic]:
        """Analyze all keywords in a snapshot.

        Args:
            snapshot: Weekly snapshot to analyze
            price_flags: Optional list of price flags for price problem detection

        Returns:
            List of KeywordDiagnostic objects sorted by opportunity score (descending)
        """
        # Build set of keywords with price flags for fast lookup
        price_flagged_queries = set()
        if price_flags:
            price_flagged_queries = {pf.search_query for pf in price_flags}

        diagnostics = []
        for record in snapshot.records:
            has_price_flag = record.search_query in price_flagged_queries
            diagnostic_type = self.diagnose(record, has_price_flag)
            rank_status = self.get_rank_status(record.impressions_share)
            opportunity_score = self.calculate_opportunity_score(record)

            diagnostics.append(
                KeywordDiagnostic(
                    search_query=record.search_query,
                    asin=record.asin,
                    diagnostic_type=diagnostic_type,
                    rank_status=rank_status,
                    opportunity_score=opportunity_score,
                    search_volume=record.search_volume,
                    impressions_share=record.impressions_share,
                    clicks_share=record.clicks_share,
                    purchases_share=record.purchases_share,
                    recommended_fix=self.get_fix_recommendation(diagnostic_type),
                )
            )

        # Sort by opportunity score descending
        diagnostics.sort(key=lambda d: d.opportunity_score, reverse=True)
        return diagnostics

    def summarize(self, diagnostics: list[KeywordDiagnostic]) -> dict[str, int]:
        """Summarize diagnostic counts."""
        counts = {
            "total": len(diagnostics),
            "ghost": 0,
            "window_shopper": 0,
            "price_problem": 0,
            "healthy": 0,
        }
        for d in diagnostics:
            if d.diagnostic_type == DiagnosticType.GHOST:
                counts["ghost"] += 1
            elif d.diagnostic_type == DiagnosticType.WINDOW_SHOPPER:
                counts["window_shopper"] += 1
            elif d.diagnostic_type == DiagnosticType.PRICE_PROBLEM:
                counts["price_problem"] += 1
            else:
                counts["healthy"] += 1
        return counts
