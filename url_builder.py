"""
url_builder.py - URL construction helpers for Etsy scraping.

Etsy URL model (verified against live pages, Q1 2025):
─────────────────────────────────────────────────────────────────────────────
  The market/country code is part of the PATH, not the hostname.
  The host is always www.etsy.com.

  Sold page 1:   https://www.etsy.com/{market}/shop/{shop}/sold
  Sold page N:   https://www.etsy.com/{market}/shop/{shop}/sold?ref=pagination&page={N}
  Shop page 1:   https://www.etsy.com/{market}/shop/{shop}
  Shop page N:   https://www.etsy.com/{market}/shop/{shop}?ref=shop_profile&page={N}#items

  US market (no prefix):
  Sold page 1:   https://www.etsy.com/shop/{shop}/sold
  Shop page 1:   https://www.etsy.com/shop/{shop}

UPDATE THIS FILE if Etsy changes its URL structure.
─────────────────────────────────────────────────────────────────────────────

Backward compatibility:
  parse_domain_legacy("etsy.ie")    -> ("etsy.com", "ie")
  parse_domain_legacy("etsy.co.uk") -> ("etsy.com", "uk")
  parse_domain_legacy("etsy.com")   -> ("etsy.com", "")   # US / no prefix
"""

from __future__ import annotations

import re


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------


def normalise_market(market: str) -> str:
    """
    Clean and normalise a raw market/country code string.

    Strips whitespace, lowercases, and removes any leading slash.

    Examples:
        "IE"  -> "ie"
        "/ie" -> "ie"
        " uk" -> "uk"
        ""    -> ""        (US / no market prefix)
    """
    return market.strip().lstrip("/").lower()


def build_market_prefix(market: str) -> str:
    """
    Return the URL path segment for a given market code.

    Examples:
        "ie" -> "/ie"
        "uk" -> "/uk"
        ""   -> ""       (US – no prefix inserted)
        "us" -> ""       (explicit US also maps to no prefix)
    """
    m = normalise_market(market)
    if not m or m == "us":
        return ""
    return f"/{m}"


def parse_domain_legacy(domain: str) -> tuple[str, str]:
    """
    Parse a legacy '--domain' value into (host, market).

    This exists purely for backward compatibility with the old CLI that accepted
    values like 'etsy.ie' or 'etsy.co.uk'.  New code should use --host / --market.

    Mapping rules:
        'etsy.ie'     -> ('etsy.com', 'ie')
        'etsy.co.uk'  -> ('etsy.com', 'uk')    # Etsy path uses /uk/, not /co.uk/
        'etsy.de'     -> ('etsy.com', 'de')
        'etsy.fr'     -> ('etsy.com', 'fr')
        'etsy.com'    -> ('etsy.com', '')       # US / no prefix
        'www.etsy.com'-> ('etsy.com', '')
        anything else -> (domain, '')           # best-effort passthrough

    Args:
        domain: Raw value from --domain CLI arg.

    Returns:
        Tuple of (host, market).
    """
    raw = domain.strip().lower().removeprefix("www.")

    if raw == "etsy.com":
        return "etsy.com", ""

    if raw.startswith("etsy."):
        suffix = raw[len("etsy."):]        # e.g. "ie", "co.uk", "de", "fr"
        # Map known compound TLDs to their Etsy path equivalents.
        _compound_map = {
            "co.uk": "uk",
            "co.nz": "nz",
            "co.in": "in",
        }
        market = _compound_map.get(suffix, suffix)
        return "etsy.com", market

    # Unknown input – return as host with empty market (safe passthrough).
    return raw, ""


# ---------------------------------------------------------------------------
# URL builders
# ---------------------------------------------------------------------------


def build_sold_url(host: str, market: str, shop_name: str, page: int = 1) -> str:
    """
    Build a sold-items page URL.

    Args:
        host:      Etsy host, e.g. 'etsy.com'.
        market:    Market/country code, e.g. 'ie', 'uk', '' (US).
        shop_name: Shop slug as it appears in Etsy URLs.
        page:      1-based page number.

    Returns:
        Fully-qualified URL string.

    Examples:
        build_sold_url("etsy.com", "ie", "GearShiftAccessories", 1)
          -> "https://www.etsy.com/ie/shop/GearShiftAccessories/sold"
        build_sold_url("etsy.com", "ie", "GearShiftAccessories", 2)
          -> "https://www.etsy.com/ie/shop/GearShiftAccessories/sold?ref=pagination&page=2"
        build_sold_url("etsy.com", "", "MyShop", 1)
          -> "https://www.etsy.com/shop/MyShop/sold"
    """
    prefix = build_market_prefix(market)
    base = f"https://www.{host}{prefix}/shop/{shop_name}/sold"
    if page <= 1:
        return base
    return f"{base}?ref=pagination&page={page}"


def build_storefront_url(host: str, market: str, shop_name: str, page: int = 1) -> str:
    """
    Build a storefront (active listings) page URL.

    Args:
        host:      Etsy host.
        market:    Market/country code.
        shop_name: Shop slug.
        page:      1-based page number.

    Returns:
        Fully-qualified URL string.

    Examples:
        build_storefront_url("etsy.com", "ie", "GearShiftAccessories", 1)
          -> "https://www.etsy.com/ie/shop/GearShiftAccessories"
        build_storefront_url("etsy.com", "ie", "GearShiftAccessories", 2)
          -> "https://www.etsy.com/ie/shop/GearShiftAccessories?ref=shop_profile&page=2#items"
    """
    prefix = build_market_prefix(market)
    base = f"https://www.{host}{prefix}/shop/{shop_name}"
    if page <= 1:
        return base
    return f"{base}?ref=shop_profile&page={page}#items"
