"""
config.py - Centralized configuration dataclasses for the Etsy Turnover Scraper.

All tuneable parameters live here.  The CLI (main.py) maps args into these
dataclasses so scraping logic never touches sys.argv directly.

Adjust timing, retry, and output settings here without touching scraping logic.
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

    min_delay_after_nav: float = 2.0         # seconds
    max_delay_after_nav: float = 5.0
    min_delay_between_pages: float = 3.0
    max_delay_between_pages: float = 8.0
    # Human-like mode multiplies all delays by human_like_multiplier.
    human_like: bool = False
    human_like_multiplier: float = 2.5
    page_load_timeout_ms: int = 30_000       # ms until navigation timeout


@dataclass
class RetryConfig:
    """Retry behaviour on navigation failures."""

    max_retries: int = 3
    retry_backoff_base: float = 2.0         # Base seconds; grows exponentially
    retry_jitter: float = 1.0              # Random ± jitter added per backoff


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
    domain: str = "etsy.com"
    browser: BrowserConfig = field(default_factory=BrowserConfig)
    timing: TimingConfig = field(default_factory=TimingConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    pagination: PaginationConfig = field(default_factory=PaginationConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    resume: bool = False
    test_mode: bool = False                 # Limits to 2 pages per source
    log_level: str = "INFO"
