"""Keyword categorization using Bread & Butter / Opportunity / Leak framework."""

from ..config import Thresholds
from ..models import (
    CategorizedKeyword,
    KeywordCategory,
    SQPRecord,
    WeeklySnapshot,
)


class KeywordCategorizer:
    """Categorize keywords based on impression, click, and purchase share."""

    def __init__(self, thresholds: Thresholds):
        self.thresholds = thresholds

    def categorize(self, snapshot: WeeklySnapshot) -> list[CategorizedKeyword]:
        """Categorize all keywords in a weekly snapshot.

        Categories:
        - Bread & Butter: Purchase Share >= threshold (protect these)
        - Opportunity: Low Imp Share + High Purchase Share (increase PPC)
        - Leak: High Imp Share + Low Click/Purchase Share (fix price/image)

        Args:
            snapshot: WeeklySnapshot with SQP records

        Returns:
            List of CategorizedKeyword objects
        """
        categorized = []

        for record in snapshot.records:
            keyword = self._categorize_record(record)
            categorized.append(keyword)

        return categorized

    def _categorize_record(self, record: SQPRecord) -> CategorizedKeyword:
        """Categorize a single SQP record."""
        category = KeywordCategory.UNCATEGORIZED
        action = ""

        # Check Bread & Butter first (highest priority)
        if record.purchases_share >= self.thresholds.bread_butter_min_purchase_share:
            category = KeywordCategory.BREAD_BUTTER
            action = "Protect: maintain ranking and defend against competitors"

        # Check Opportunity
        elif (
            record.impressions_share < self.thresholds.opportunity_max_imp_share
            and record.purchases_share >= self.thresholds.opportunity_min_purchase_share
        ):
            category = KeywordCategory.OPPORTUNITY
            action = "Increase PPC bids to gain visibility; high conversion potential"

        # Check Leak
        elif (
            record.impressions_share >= self.thresholds.leak_min_imp_share
            and (
                record.clicks_share < self.thresholds.leak_max_click_share
                or record.purchases_share < self.thresholds.leak_max_purchase_share
            )
        ):
            category = KeywordCategory.LEAK
            if record.clicks_share < self.thresholds.leak_max_click_share:
                action = "Fix: improve main image, title, or pricing to boost clicks"
            else:
                action = "Fix: review listing content, A+ content, or price competitiveness"

        return CategorizedKeyword(
            search_query=record.search_query,
            asin=record.asin,
            category=category,
            action=action,
            impressions_share=record.impressions_share,
            clicks_share=record.clicks_share,
            purchases_share=record.purchases_share,
            search_volume=record.search_volume,
            asin_price=record.asin_price,
            market_price=record.market_price,
        )

    def get_bread_butter(
        self, categorized: list[CategorizedKeyword]
    ) -> list[CategorizedKeyword]:
        """Get all Bread & Butter keywords."""
        return [k for k in categorized if k.category == KeywordCategory.BREAD_BUTTER]

    def get_opportunities(
        self, categorized: list[CategorizedKeyword]
    ) -> list[CategorizedKeyword]:
        """Get all Opportunity keywords."""
        return [k for k in categorized if k.category == KeywordCategory.OPPORTUNITY]

    def get_leaks(
        self, categorized: list[CategorizedKeyword]
    ) -> list[CategorizedKeyword]:
        """Get all Leak keywords."""
        return [k for k in categorized if k.category == KeywordCategory.LEAK]

    def summarize(self, categorized: list[CategorizedKeyword]) -> dict[str, int]:
        """Get counts by category."""
        return {
            "total": len(categorized),
            "bread_butter": len(self.get_bread_butter(categorized)),
            "opportunities": len(self.get_opportunities(categorized)),
            "leaks": len(self.get_leaks(categorized)),
            "uncategorized": len(
                [k for k in categorized if k.category == KeywordCategory.UNCATEGORIZED]
            ),
        }
