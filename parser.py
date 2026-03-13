"""
parser.py - HTML parsing for Etsy sold and storefront listing pages.

Uses BeautifulSoup with the lxml parser.  All methods are fully defensive:
missing fields yield None rather than raising exceptions.

UPDATE the selector constants below if Etsy changes its HTML structure.

Key selectors (verified against live Etsy HTML, Q1 2025):
─────────────────────────────────────────────────────────────────────────────
  CARD ROOT:       Any element carrying both [data-listing-id] and
                   [data-listing-card-v2] attributes.
                   Fallback: [data-listing-id] + [data-behat-listing-card].

  LISTING ID:      data-listing-id attribute on the card root element.
  SHOP ID:         data-shop-id attribute on the card root element.

  LISTING LINK:    <a data-listing-link> — href is the listing URL,
                   title attribute is the product name.

  IMAGE:           <img data-listing-card-listing-image> — src attribute.

  PRICE (active):  <span class="currency-value">  (e.g. "34.43")
                   <span class="currency-symbol">  (e.g. "€")

  SOLD STATUS:     <p class="wt-text-title-01"> containing "Sold" text.
                   This element REPLACES the price on sold pages.

PAGINATION selectors (verified against live Etsy HTML, Q1 2025):
─────────────────────────────────────────────────────────────────────────────
  PAGINATION NAV:  <nav data-clg-id="WtPagination" aria-label="Pagination of listings">
  CONTAINER:       <div data-item-pagination="">  (wraps the nav)
  PAGE LINKS:      <a data-page="N">  inside the nav  (N is the page number)
  CURRENT PAGE:    <a class="...wt-is-selected" aria-current="true">
  PREV/NEXT:       <a class="...wt-btn--icon ..."> with screen-reader text
                   "Previous page" / "Next page"

  Strategy: take the maximum integer value of all [data-page] attributes
  inside the pagination nav.  This correctly finds the last page even when
  Etsy collapses the middle pages with "...".
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Optional

from bs4 import BeautifulSoup, Tag

from models import ActiveListing, SoldListing

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Selector constants
# UPDATE THESE if Etsy changes its DOM.
# ---------------------------------------------------------------------------

# Attributes that identify a genuine listing card root element.
CARD_LISTING_ID_ATTR = "data-listing-id"
CARD_CARD_V2_ATTR = "data-listing-card-v2"
CARD_BEHAT_ATTR = "data-behat-listing-card"
CARD_SHOP_ID_ATTR = "data-shop-id"

# The <a> tag that wraps the card image and acts as the primary link.
LISTING_LINK_ATTR = "data-listing-link"

# The <img> inside the card image container.
LISTING_IMAGE_ATTR = "data-listing-card-listing-image"

# The <h3> title element uses an id like "listing-title-{listing_id}".
TITLE_H3_ID_PREFIX = "listing-title-"

# Price / status selectors.
PRICE_VALUE_SEL = "span.currency-value"          # numeric portion, e.g. "34.43"
CURRENCY_SYMBOL_SEL = "span.currency-symbol"     # symbol, e.g. "€"
SOLD_STATUS_SEL = "p.wt-text-title-01"           # contains "Sold" on sold pages


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_sold_page(
    html: str,
    page_url: str,
    page_number: int,
    host: str,
    shop_name: str,
    shop_id: str,
    snapshot_path: Optional[str] = None,
) -> list[SoldListing]:
    """
    Parse a sold page and return SoldListing objects.

    A card is treated as sold when its price area shows "Sold" text.
    Every card found is returned; partial extraction is preferred over skipping.

    Args:
        html:          Raw HTML string of the sold page.
        page_url:      Canonical URL of the page (stored on each record).
        page_number:   1-based page number (stored on each record).
        host:          Etsy host, e.g. 'etsy.com' (used only for relative-URL fallback).
        shop_name:     Shop slug.
        shop_id:       Shop ID (can be overridden per card from data-shop-id).
        snapshot_path: Path to the saved HTML snapshot (optional, stored verbatim).

    Returns:
        List of SoldListing dataclass instances.
    """
    soup = BeautifulSoup(html, "lxml")
    cards = _find_listing_cards(soup)
    logger.debug("Sold page %d: found %d candidate cards", page_number, len(cards))

    now_ts = _utcnow()
    results: list[SoldListing] = []

    for card in cards:
        try:
            listing_id = _get_attr(card, CARD_LISTING_ID_ATTR)
            if not listing_id:
                continue

            shop_id_on_card = _get_attr(card, CARD_SHOP_ID_ATTR) or shop_id or None
            listing_url = _extract_listing_url(card, host)
            title = _extract_title(card, listing_id)
            image_url = _extract_image(card)
            card_text, price_raw, currency = _extract_price_info(card)
            notes = _build_notes(card)

            results.append(
                SoldListing(
                    listing_id=listing_id,
                    scrape_timestamp=now_ts,
                    domain=host,
                    shop_name=shop_name,
                    shop_id=shop_id_on_card,
                    sold_page_url=page_url,
                    sold_page_number=page_number,
                    listing_url=listing_url,
                    product_title=title,
                    image_url=image_url,
                    sold_flag=True,
                    card_text_status=card_text or "Sold",
                    sold_price_raw=price_raw,
                    currency=currency,
                    extraction_notes=notes or None,
                    raw_html_snapshot_path=snapshot_path,
                )
            )

        except Exception as exc:
            logger.warning("Error parsing sold card: %s", exc)

    logger.debug("Sold page %d: parsed %d listings", page_number, len(results))
    return results


def parse_storefront_page(
    html: str,
    page_url: str,
    page_number: int,
    host: str,
    shop_name: str,
    shop_id: str,
    snapshot_path: Optional[str] = None,
) -> list[ActiveListing]:
    """
    Parse a shop storefront page and return ActiveListing objects.

    Extracts price, currency, and availability for each card found.

    Args:
        html:          Raw HTML string of the storefront page.
        page_url:      Canonical URL stored on each record.
        page_number:   1-based page number.
        host:          Etsy host, e.g. 'etsy.com' (used only for relative-URL fallback).
        shop_name:     Shop slug.
        shop_id:       Shop ID (fallback if not on card).
        snapshot_path: Path to saved HTML snapshot (optional).

    Returns:
        List of ActiveListing dataclass instances.
    """
    soup = BeautifulSoup(html, "lxml")
    cards = _find_listing_cards(soup)
    logger.debug("Storefront page %d: found %d candidate cards", page_number, len(cards))

    now_ts = _utcnow()
    results: list[ActiveListing] = []

    for position, card in enumerate(cards, start=1):
        try:
            listing_id = _get_attr(card, CARD_LISTING_ID_ATTR)
            if not listing_id:
                continue

            shop_id_on_card = _get_attr(card, CARD_SHOP_ID_ATTR) or shop_id or None
            listing_url = _extract_listing_url(card, host)
            title = _extract_title(card, listing_id)
            image_url = _extract_image(card)
            card_text, price_raw, currency = _extract_price_info(card)
            price = _normalise_price(price_raw)
            availability = "active" if price is not None else "unknown"
            notes = _build_notes(card)

            results.append(
                ActiveListing(
                    listing_id=listing_id,
                    scrape_timestamp=now_ts,
                    domain=host,
                    shop_name=shop_name,
                    shop_id=shop_id_on_card,
                    storefront_page_url=page_url,
                    storefront_page_number=page_number,
                    listing_url=listing_url,
                    product_title=title,
                    image_url=image_url,
                    price=price,
                    currency=currency,
                    availability=availability,
                    availability_raw=card_text or None,
                    listing_position_on_page=position,
                    extraction_notes=notes or None,
                    raw_html_snapshot_path=snapshot_path,
                )
            )

        except Exception as exc:
            logger.warning("Error parsing storefront card: %s", exc)

    logger.debug("Storefront page %d: parsed %d listings", page_number, len(results))
    return results


# ---------------------------------------------------------------------------
# Pagination parsing
# ---------------------------------------------------------------------------


def parse_last_page_number(html: str, page_type: str = "sold") -> int:
    """
    Inspect the pagination bar and return the last (maximum) page number.

    Returns 1 when no pagination is present (i.e. the page is the only page).

    Strategies (tried in order, stopping at the first hit):

      1. Find <nav data-clg-id="WtPagination"> and read all [data-page] values.
         This is the most reliable because data-clg-id is stable across layouts.
         UPDATE: data-clg-id="WtPagination" if Etsy renames this component.

      2. Find <div data-item-pagination=""> and read all [data-page] values.
         This is the outer container; useful if the nav label changes.
         UPDATE: data-item-pagination attribute name if Etsy changes it.

      3. Scan the whole document for any [data-page] attributes and take the max.
         Wide fallback — avoids false positives from non-pagination data-page
         elements by filtering out values that look unreasonably large (>9999).

      4. Scan all <a href> for ?page=N or &page=N query parameters and take the
         max.  Works for sold pages that use ?ref=pagination&page=N links.
         Also covers storefront ?ref=items-pagination&page=N links.

    Args:
        html:       Full raw HTML of the page to inspect.
        page_type:  "sold" or "storefront" — used only for debug logging labels.

    Returns:
        Integer last page number, minimum 1.
    """
    soup = BeautifulSoup(html, "lxml")

    def _max_from_data_page(container: Tag) -> int:
        """Return maximum integer data-page value found within *container*."""
        max_p = 0
        for a in container.find_all("a", attrs={"data-page": True}):
            try:
                val = int(str(a["data-page"]))
                if 1 <= val <= 9999:      # sanity-range guard
                    max_p = max(max_p, val)
            except (ValueError, TypeError):
                pass
        return max_p

    # ---- Strategy 1: WtPagination nav  (preferred) ---------------------
    # UPDATE selector here if Etsy renames the data-clg-id attribute.
    nav = soup.find("nav", attrs={"data-clg-id": "WtPagination"})
    if nav:
        result = _max_from_data_page(nav)
        if result > 0:
            logger.debug(
                "[%s] parse_last_page: strategy 1 (WtPagination nav) -> %d",
                page_type, result,
            )
            return result

    # ---- Strategy 2: data-item-pagination container --------------------
    # UPDATE attribute name here if Etsy changes the pagination wrapper.
    pag_div = soup.find(attrs={"data-item-pagination": True})
    if pag_div:
        result = _max_from_data_page(pag_div)
        if result > 0:
            logger.debug(
                "[%s] parse_last_page: strategy 2 (data-item-pagination) -> %d",
                page_type, result,
            )
            return result

    # ---- Strategy 3: global data-page scan ----------------------------
    result = _max_from_data_page(soup)
    if result > 0:
        logger.debug(
            "[%s] parse_last_page: strategy 3 (global data-page scan) -> %d",
            page_type, result,
        )
        return result

    # ---- Strategy 4: href ?page=N pattern matching --------------------
    # Sold pages:       ?ref=pagination&page=N
    # Storefront pages: ?ref=items-pagination&page=N  or ?ref=shop_profile&page=N
    page_nums: set[int] = set()
    for a in soup.find_all("a", href=True):
        href = str(a["href"])
        m = re.search(r"[?&]page=(\d+)", href)
        if m:
            try:
                val = int(m.group(1))
                if 1 <= val <= 9999:
                    page_nums.add(val)
            except ValueError:
                pass
    if page_nums:
        result = max(page_nums)
        logger.debug(
            "[%s] parse_last_page: strategy 4 (href page param) -> %d",
            page_type, result,
        )
        return result

    logger.debug("[%s] parse_last_page: no pagination found, assuming 1 page", page_type)
    return 1


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _find_listing_cards(soup: BeautifulSoup) -> list[Tag]:
    """
    Locate listing card root elements using progressively wider strategies.

    Strategy 1 (preferred):  Elements with both data-listing-id AND
                             data-listing-card-v2 (avoids favourite buttons).
    Strategy 2:              Elements with data-listing-id AND data-behat-listing-card.
    Strategy 3 (wide):       All elements with data-listing-id, deduplicated.
    """
    # Strategy 1
    cards = soup.find_all(attrs={CARD_LISTING_ID_ATTR: True, CARD_CARD_V2_ATTR: True})
    if cards:
        return cards  # type: ignore[return-value]

    # Strategy 2
    cards = soup.find_all(attrs={CARD_LISTING_ID_ATTR: True, CARD_BEHAT_ATTR: True})
    if cards:
        return cards  # type: ignore[return-value]

    # Strategy 3 – deduplicate by listing_id to avoid double-counting
    logger.debug("Falling back to wide card search (data-listing-id only)")
    seen: set[str] = set()
    unique: list[Tag] = []
    for el in soup.find_all(attrs={CARD_LISTING_ID_ATTR: True}):
        lid = el.get(CARD_LISTING_ID_ATTR)
        if lid and lid not in seen:
            seen.add(str(lid))
            unique.append(el)
    return unique


def _get_attr(tag: Tag, attr: str) -> Optional[str]:
    """Safely return a tag attribute as a stripped string, or None."""
    val = tag.get(attr)
    if isinstance(val, list):
        joined = " ".join(str(v) for v in val).strip()
        return joined or None
    return str(val).strip() if val else None


def _extract_listing_url(card: Tag, host: str) -> Optional[str]:
    """
    Extract the listing URL from the card.

    Primary:  <a data-listing-link href="…">  – strips tracking query params.
    Fallback: First <a href> containing '/listing/'.

    Note: Etsy listing hrefs in card HTML are already absolute, so the host
    parameter is only used as a fallback for relative-path hrefs (rare).
    """
    # Primary
    link = card.find("a", attrs={LISTING_LINK_ATTR: True})
    if link:
        href = str(link.get("href", ""))
        # Remove tracking query string; keep only the clean slug path.
        href = href.split("?")[0]
        if href.startswith("//"):
            return "https:" + href
        if href.startswith("/"):
            return f"https://www.{host}{href}"
        if href.startswith("http"):
            return href

    # Fallback: any <a> with /listing/ in href
    for a_tag in card.find_all("a", href=True):
        href = str(a_tag["href"])
        if "/listing/" in href:
            return href.split("?")[0]

    return None


def _extract_title(card: Tag, listing_id: str) -> Optional[str]:
    """
    Extract the product title from the card.

    Priority:
      1. title attribute on <a data-listing-link>
      2. <h3 id="listing-title-{listing_id}"> text
      3. First <h3> anywhere inside the card
    """
    link = card.find("a", attrs={LISTING_LINK_ATTR: True})
    if link:
        title_attr = link.get("title", "").strip()
        if title_attr:
            return title_attr

    h3 = card.find("h3", id=f"{TITLE_H3_ID_PREFIX}{listing_id}")
    if h3:
        return h3.get_text(" ", strip=True)

    h3 = card.find("h3")
    if h3:
        return h3.get_text(" ", strip=True)

    return None


def _extract_image(card: Tag) -> Optional[str]:
    """
    Extract the first listing image URL.

    Primary:  <img data-listing-card-listing-image src="…">
    Fallback: First <img> with 'etsystatic' in the src URL.
    """
    img = card.find("img", attrs={LISTING_IMAGE_ATTR: True})
    if img and img.get("src"):
        return str(img["src"])

    for img_tag in card.find_all("img", src=True):
        src = str(img_tag["src"])
        if "etsystatic" in src:
            return src

    return None


def _extract_price_info(
    card: Tag,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract (card_text_status, price_raw_string, currency_symbol) from a card.

    On sold pages:
        card_text_status = "Sold"  (the <p.wt-text-title-01> text)
        price_raw        = None    (no price shown)
        currency         = None

    On storefront pages:
        card_text_status = None
        price_raw        = "34.43"  (from <span.currency-value>)
        currency         = "€"      (from <span.currency-symbol>)

    Both values can be None if the card layout is unexpected.
    """
    price_span = card.select_one(PRICE_VALUE_SEL)
    currency_span = card.select_one(CURRENCY_SYMBOL_SEL)
    price_raw = price_span.get_text(strip=True) if price_span else None
    currency = currency_span.get_text(strip=True) if currency_span else None

    # Check for "Sold" status text (overrides any price value found).
    for p_tag in card.select(SOLD_STATUS_SEL):
        text = p_tag.get_text(strip=True)
        if text.lower() in ("sold", "sold out"):
            return text, price_raw, currency

    return None, price_raw, currency


def _normalise_price(price_raw: Optional[str]) -> Optional[float]:
    """
    Convert a raw price string to a float.

    Handles:
      "34.43"    → 34.43
      "1,234.56" → 1234.56   (US thousand separator)
      "1.234,56" → 1234.56   (European thousand separator)
      "Sold"     → None
      ""         → None
    """
    if not price_raw:
        return None

    cleaned = re.sub(r"[^\d.,]", "", price_raw)
    if not cleaned:
        return None

    # European format: digits, dots as thousand sep, comma as decimal.
    if re.match(r"^\d{1,3}(\.\d{3})+(,\d+)?$", cleaned):
        cleaned = cleaned.replace(".", "").replace(",", ".")
    else:
        # US format or plain integer: remove thousand commas.
        cleaned = cleaned.replace(",", "")

    try:
        return float(cleaned)
    except ValueError:
        logger.debug("Could not parse price: %r", price_raw)
        return None


def _build_notes(card: Tag) -> str:
    """
    Build a brief comma-separated notes string documenting any missing fields.
    Useful for diagnosing parsing gaps without crashing.
    """
    issues: list[str] = []
    if not card.find("a", attrs={LISTING_LINK_ATTR: True}):
        issues.append("no_listing_link")
    if not card.find("img", attrs={LISTING_IMAGE_ATTR: True}):
        issues.append("no_image")
    has_price = bool(card.select_one(PRICE_VALUE_SEL))
    has_sold = any(
        "sold" in p.get_text(strip=True).lower()
        for p in card.select(SOLD_STATUS_SEL)
    )
    if not has_price and not has_sold:
        issues.append("no_price_or_sold_text")
    return ",".join(issues)


def _utcnow() -> str:
    """Return current UTC timestamp as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()
