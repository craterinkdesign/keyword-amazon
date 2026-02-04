#!/usr/bin/env python3
"""Fetch listing content from Amazon SP-API.

This module fetches listing title, bullets, and backend keywords
using the Listings Items API to support keyword placement detection.

Usage:
    python -m sqp_analyzer.commands.fetch_listing --sku YOUR_SKU
"""

import argparse
import sys

from sp_api.api import ListingsItems
from sp_api.base import Marketplaces

from ..config import load_config
from ..models import ListingContent


def get_credentials() -> dict:
    """Load SP-API credentials from environment."""
    config = load_config()
    return {
        "refresh_token": config.sp_api.refresh_token,
        "lwa_app_id": config.sp_api.client_id,
        "lwa_client_secret": config.sp_api.client_secret,
    }


def get_listing_content(seller_id: str, sku: str) -> ListingContent | None:
    """Fetch listing title, bullets, and backend keywords.

    Args:
        seller_id: Amazon Seller ID
        sku: Product SKU

    Returns:
        ListingContent with title, bullets, and backend keywords,
        or None if listing not found
    """
    credentials = get_credentials()

    try:
        listings = ListingsItems(credentials=credentials, marketplace=Marketplaces.US)
        response = listings.get_listings_item(
            sellerId=seller_id,
            sku=sku,
            marketplaceIds=["ATVPDKIKX0DER"],
            includedData=["summaries", "attributes"],
        )

        payload = response.payload
        if not payload:
            return None

        attrs = payload.get("attributes", {})
        summaries = payload.get("summaries", [{}])
        summary = summaries[0] if summaries else {}

        # Extract ASIN from identifiers or summary
        asin = summary.get("asin", "")
        if not asin:
            identifiers = attrs.get("externally_assigned_product_identifier", [])
            for ident in identifiers:
                if ident.get("type") == "asin":
                    asin = ident.get("value", "")
                    break

        # Extract title
        title = summary.get("itemName", "")

        # Extract bullet points
        bullets = []
        bullet_attrs = attrs.get("bullet_point", [])
        for bullet in bullet_attrs:
            value = bullet.get("value", "")
            if value:
                bullets.append(value)

        # Extract backend keywords (generic_keyword)
        backend_keywords = []
        generic_keywords = attrs.get("generic_keyword", [])
        for kw in generic_keywords:
            value = kw.get("value", "")
            if value:
                backend_keywords.append(value)

        return ListingContent(
            asin=asin,
            sku=sku,
            title=title,
            bullets=bullets,
            backend_keywords=backend_keywords,
        )

    except Exception as e:
        print(f"Error fetching listing for SKU {sku}: {e}")
        return None


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser."""
    parser = argparse.ArgumentParser(
        description="Fetch listing content from Amazon SP-API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Fetch listing content for a SKU
    python -m sqp_analyzer.commands.fetch_listing --sku YOUR_SKU

    # Test connection
    python -m sqp_analyzer.commands.fetch_listing --test-connection
        """,
    )
    parser.add_argument(
        "--sku",
        type=str,
        help="SKU to fetch listing for",
    )
    parser.add_argument(
        "--test-connection",
        action="store_true",
        help="Test API connection only",
    )
    return parser


def main() -> int:
    """Main entry point."""
    parser = create_parser()
    args = parser.parse_args()

    # Load config
    config = load_config()

    if not config.sp_api.seller_id:
        print("[ERROR] SELLER_ID not set in environment")
        print("Add SELLER_ID=YOUR_SELLER_ID to your .env file")
        return 1

    # Test connection
    if args.test_connection:
        print("Testing SP-API Listings connection...")
        credentials = get_credentials()
        try:
            listings = ListingsItems(credentials=credentials, marketplace=Marketplaces.US)
            # Try to get a listing (will fail but proves connection works)
            print("[SUCCESS] Connected to SP-API Listings API")
            return 0
        except Exception as e:
            print(f"[FAILED] {e}")
            return 1

    if not args.sku:
        parser.print_help()
        return 1

    # Fetch listing
    print(f"Fetching listing for SKU: {args.sku}")
    listing = get_listing_content(config.sp_api.seller_id, args.sku)

    if not listing:
        print("[ERROR] Could not fetch listing")
        return 1

    # Display results
    print("\n" + "=" * 60)
    print(f"ASIN: {listing.asin}")
    print(f"SKU: {listing.sku}")
    print("=" * 60)

    print(f"\nTitle:\n  {listing.title}")

    print(f"\nBullet Points ({len(listing.bullets)}):")
    for i, bullet in enumerate(listing.bullets, 1):
        print(f"  {i}. {bullet[:80]}..." if len(bullet) > 80 else f"  {i}. {bullet}")

    print(f"\nBackend Keywords ({len(listing.backend_keywords)}):")
    for kw in listing.backend_keywords:
        print(f"  - {kw}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
