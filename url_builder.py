"""
url_builder.py - URL construction helpers for Etsy scraping.

Builds paginated sold-items and storefront URLs from (domain, shop_name, page).

UPDATE HERE if Etsy changes its URL structure.

Supported patterns (as of Q1 2025):
  Sold page 1:  https://www.{domain}/shop/{shop_name}/sold
  Sold page N:  https://www.{domain}/shop/{shop_name}/sold?ref=pagination&page={N}
  Shop page 1:  https://www.{domain}/shop/{shop_name}
  Shop page N:  https://www.{domain}/shop/{shop_name}?ref=shop_profile&page={N}#items
"""

from __future__ import annotations


def normalise_domain(domain: str) -> str:
    """
    Strip any leading 'www.' so the caller can safely prepend it.

    Examples:
        'www.etsy.com'  -> 'etsy.com'
        'etsy.ie'       -> 'etsy.ie'
        'etsy.co.uk'    -> 'etsy.co.uk'
    """
    domain = domain.strip().lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def build_sold_url(domain: str, shop_name: str, page: int = 1) -> str:
    """
    Build a sold-items page URL.

    Args:
        domain:    Etsy domain string, e.g. 'etsy.com', 'etsy.ie', 'etsy.co.uk'.
        shop_name: The shop slug exactly as it appears in Etsy URLs.
        page:      Page number (1-based).

    Returns:
        Fully-qualified URL string.
    """
    base = f"https://www.{normalise_domain(domain)}/shop/{shop_name}/sold"
    if page <= 1:
        return base
    return f"{base}?ref=pagination&page={page}"


def build_storefront_url(domain: str, shop_name: str, page: int = 1) -> str:
    """
    Build a storefront (active listings) page URL.

    Args:
        domain:    Etsy domain string.
        shop_name: The shop slug.
        page:      Page number (1-based).

    Returns:
        Fully-qualified URL string.
    """
    base = f"https://www.{normalise_domain(domain)}/shop/{shop_name}"
    if page <= 1:
        return base
    return f"{base}?ref=shop_profile&page={page}#items"
