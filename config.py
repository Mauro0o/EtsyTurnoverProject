"""
config.py - Centralized configuration dataclasses for the Etsy Turnover Scraper.

All tuneable parameters live here.  The CLI (main.py) maps args into these
dataclasses so scraping logic never touches sys.argv directly.

Adjust timing, retry, and output settings here without touching scraping logic.

URL model (as of Q1 2025):
  Etsy localized pages embed the market/country code in the PATH, not the host.
  The host is always www.etsy.com.

  Examples:
    https://www.etsy.com/ie/shop/MyShop/sold   (Ireland)
    https://www.etsy.com/uk/shop/MyShop/sold   (UK)
    https://www.etsy.com/shop/MyShop/sold      (US / no market prefix)

  Config fields:
    host   = "etsy.com"   (always, unless Etsy ever uses a different host)
    market = "ie"         (path prefix; empty string = US / no prefix)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class BrowserConfig:
    """Controls Playwright browser launch and rendering settings."""

    headless: bool = False
    browser_type: str = "chromium"          # chromium | firefox | webkit
    viewport_width: int = 1280
    viewport_height: int = 900
    # Default UA – update here if Etsy sharpens bot-detection heuristics.
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
    rotate_user_agents: bool = False
    # Persistent browser context directory (preserves cookies across runs).
    profile_dir: Optional[Path] = None


# Small rotation pool.  Add/remove entries to keep pace with browser versions.
USER_AGENT_POOL: list[str] = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) "
        "Gecko/20100101 Firefox/123.0"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
]


@dataclass
class TimingConfig:
    """Pacing/delay settings to keep requests human-like and low-volume."""

    min_delay_after_nav: float = 0.6         # seconds after page load
    max_delay_after_nav: float = 1.5
    min_delay_between_pages: float = 0.8
    max_delay_between_pages: float = 2.0
    # Human-like mode multiplies all delays by human_like_multiplier.
    human_like: bool = False
    human_like_multiplier: float = 1.5      # was 2.5; kept gentler
    page_load_timeout_ms: int = 25_000      # ms until navigation timeout

    # Test-mode overrides (applied automatically when AppConfig.test_mode=True).
    test_mode_min_delay: float = 0.3
    test_mode_max_delay: float = 0.7


@dataclass
class RetryConfig:
    """Retry behaviour on navigation failures."""

    max_retries: int = 3
    retry_backoff_base: float = 1.5         # Base seconds; grows exponentially
    retry_jitter: float = 0.4              # Random ± jitter added per backoff


@dataclass
class PaginationConfig:
    """Controls how many pages to scrape from each listing source."""

    start_page_sold: int = 1
    max_pages_sold: int = 100
    start_page_storefront: int = 1
    max_pages_storefront: int = 100
    stop_on_empty: bool = True              # Halt pagination on first empty page


@dataclass
class OutputConfig:
    """Output path and format settings."""

    sqlite_path: Path = field(default_factory=lambda: Path("etsy_turnover.db"))
    excel_path: Path = field(default_factory=lambda: Path("etsy_turnover.xlsx"))
    save_html: bool = False
    html_snapshot_dir: Path = field(default_factory=lambda: Path("html_snapshots"))
    csv_export: bool = False


@dataclass
class AppConfig:
    """Top-level application configuration assembled from CLI args and defaults."""

    shop_id: str = ""
    shop_name: str = ""
    # --- URL routing ---
    # host:   always "etsy.com" in practice; exposed for future flexibility.
    # market: path prefix encoding the Etsy market/country (e.g. "ie", "uk").
    #         Empty string means no prefix → US default URLs.
    host: str = "etsy.com"
    market: str = ""            # e.g. "ie" → /ie/shop/...  |  "" → /shop/...
    browser: BrowserConfig = field(default_factory=BrowserConfig)
    timing: TimingConfig = field(default_factory=TimingConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    pagination: PaginationConfig = field(default_factory=PaginationConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    resume: bool = False
    test_mode: bool = False                 # Limits to 2 pages per source
    log_level: str = "INFO"
