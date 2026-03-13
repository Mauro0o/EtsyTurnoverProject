"""
models.py - Domain dataclasses for the Etsy Turnover Scraper.

Three core record types:
  SoldListing        – one card from /shop/{name}/sold
  ActiveListing      – one card from the live storefront /shop/{name}
  MatchedTurnoverRow – result of joining sold against active by listing_id

IMPORTANT: estimated_turnover is an approximation.  Etsy sold pages do NOT
expose the historical sale price; only "Sold" is shown.  The estimated price
is therefore the *current* active listing price at scrape time, which may
differ from the actual sale price.  Coverage is limited to sold listings whose
IDs are still present on the active storefront.

Sold deduplication modes (set via --sold-dedup-mode / AppConfig.sold_dedup_mode):
  preserve_all        (DEFAULT) – every scraped sold row is kept as a distinct
                                  sale event.  Repeated listing_ids count as
                                  multiple units sold.
  unique_listing_id             – collapse sold rows to one per listing_id
                                  (legacy / reference mode).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class SoldListing:
    """
    One listing card scraped from a shop's /sold page.

    sold_row_id is a deterministic surrogate key of the form
    "{listing_id}|{shop_name}|{domain}|p{page}|i{idx}" assigned in the scraper
    loop.  It allows the same listing to appear multiple times in the table as
    distinct sale events.

    sold_price_raw is almost always None because Etsy replaces the price with
    the word "Sold" on these pages.  It is kept for any future cases where
    Etsy surfaces a price.
    """

    listing_id: str
    scrape_timestamp: str
    domain: str
    shop_name: str
    sold_row_id: str = ""              # assigned in scraper after parsing
    shop_id: Optional[str] = None
    sold_page_url: str = ""
    sold_page_number: int = 1
    listing_url: Optional[str] = None
    product_title: Optional[str] = None
    image_url: Optional[str] = None
    sold_flag: bool = True
    card_text_status: Optional[str] = "Sold"
    sold_price_raw: Optional[str] = None   # Kept for completeness; usually empty
    currency: Optional[str] = None
    extraction_notes: Optional[str] = None
    raw_html_snapshot_path: Optional[str] = None


@dataclass
class ActiveListing:
    """
    One listing scraped from a shop's live storefront page.

    price is the *current* listed price at scrape time.  It is used as a proxy
    for the sold price when matching against SoldListings.
    """

    listing_id: str
    scrape_timestamp: str
    domain: str
    shop_name: str
    shop_id: Optional[str] = None
    storefront_page_url: str = ""
    storefront_page_number: int = 1
    listing_url: Optional[str] = None
    product_title: Optional[str] = None
    image_url: Optional[str] = None
    price: Optional[float] = None
    currency: Optional[str] = None
    availability: Optional[str] = None      # "active" | "unknown"
    availability_raw: Optional[str] = None  # Raw text from the card price area
    listing_position_on_page: int = 0
    extraction_notes: Optional[str] = None
    raw_html_snapshot_path: Optional[str] = None
    storefront_keyword: str = ""  # "" = no keyword filter; otherwise the keyword used


@dataclass
class MatchedTurnoverRow:
    """
    Result of cross-matching sold listings against active listings.

    One row per UNIQUE sold listing_id.  When a listing_id appears multiple
    times in sold_listings (i.e. sold more than once), sales_count reflects the
    total number of sales events and estimated_turnover = estimated_price * sales_count.

    matched_flag = 1  → exact listing_id match found; estimated_price populated.
    matched_flag = 0  → no match; sold listing ID not present on storefront.

    ESTIMATION NOTE: estimated_turnover uses the current active listing price
    as a proxy — NOT the actual historical sale price.
    """

    scrape_timestamp: str
    domain: str
    shop_name: str
    sold_listing_id: str
    active_listing_id: Optional[str] = None
    match_type: Optional[str] = None           # "exact_listing_id" or None
    sold_title: Optional[str] = None
    active_title: Optional[str] = None
    sold_listing_url: Optional[str] = None    # URL of the sold listing for visual verification
    active_listing_url: Optional[str] = None  # URL of the matched active listing
    estimated_price: Optional[float] = None
    currency: Optional[str] = None
    sales_count: int = 1                       # total sale events for this listing_id
    estimated_turnover: Optional[float] = None # estimated_price * sales_count
    matched_flag: int = 0                      # 1 = matched, 0 = unmatched
    notes: Optional[str] = None


@dataclass
class RunSummary:
    """Aggregated metrics produced after the matching phase completes."""

    total_sold_rows: int = 0             # all scraped sold rows (incl. duplicates)
    sold_unique_listing_ids: int = 0     # distinct listing IDs in sold rows
    total_active_rows: int = 0
    exact_matches: int = 0               # total matched sales events (sum of sales_count)
    matched_unique_listing_ids: int = 0  # distinct listing IDs that had a match
    unmatched_sold_rows: int = 0         # total_sold_rows - exact_matches
    price_match_coverage_pct: float = 0.0
    estimated_turnover_sum: float = 0.0
