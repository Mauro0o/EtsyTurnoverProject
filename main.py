"""
main.py - CLI entry point for the Etsy Turnover Scraper.

Usage:
    python main.py --shop-name MyShop --domain etsy.ie

Run with --help to see all options and defaults.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from config import (
    AppConfig,
    BrowserConfig,
    OutputConfig,
    PaginationConfig,
    RetryConfig,
    TimingConfig,
)
from scraper import EtsyTurnoverScraper


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="etsy-turnover-scraper",
        description=(
            "Estimate turnover for an Etsy shop by cross-matching "
            "sold listings against active storefront listings.\n\n"
            "IMPORTANT: The turnover figure is an ESTIMATE only.  "
            "Sold pages do not expose historical sale prices; the active "
            "listing price is used as a proxy."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # ---- Shop identity -----------------------------------------------
    shop = parser.add_argument_group("Shop identity")
    shop.add_argument(
        "--shop-name",
        required=True,
        help="Etsy shop name (slug exactly as it appears in URLs)",
    )
    shop.add_argument(
        "--shop-id",
        default="",
        help="Etsy seller/shop numeric ID (optional; used for metadata)",
    )
    shop.add_argument(
        "--domain",
        default="etsy.com",
        help="Etsy domain to scrape from (e.g. etsy.com, etsy.ie, etsy.co.uk)",
    )

    # ---- Browser ---------------------------------------------------------
    browser = parser.add_argument_group("Browser settings")
    browser.add_argument(
        "--headless",
        action="store_true",
        default=False,
        help="Run browser in headless mode (no visible window)",
    )
    browser.add_argument(
        "--no-headless",
        dest="headless",
        action="store_false",
        help="Run in visible browser mode (default – lower bot-detection risk)",
    )
    browser.add_argument(
        "--browser",
        dest="browser_type",
        default="chromium",
        choices=["chromium", "firefox", "webkit"],
        help="Playwright browser engine",
    )
    browser.add_argument(
        "--rotate-user-agents",
        action="store_true",
        default=False,
        help="Randomly rotate user-agent string from a small pool",
    )
    browser.add_argument(
        "--profile-dir",
        type=Path,
        default=None,
        help="Persistent browser profile directory (Chromium only; preserves cookies)",
    )

    # ---- Pagination -----------------------------------------------------
    pag = parser.add_argument_group("Pagination")
    pag.add_argument("--start-page-sold", type=int, default=1,
                     help="First sold page to scrape")
    pag.add_argument("--max-pages-sold", type=int, default=100,
                     help="Maximum number of sold pages to scrape")
    pag.add_argument("--start-page-storefront", type=int, default=1,
                     help="First storefront page to scrape")
    pag.add_argument("--max-pages-storefront", type=int, default=100,
                     help="Maximum number of storefront pages to scrape")
    pag.add_argument(
        "--stop-on-empty",
        action="store_true",
        default=True,
        help="Stop pagination automatically when a page returns zero listings",
    )

    # ---- Output ---------------------------------------------------------
    out = parser.add_argument_group("Output")
    out.add_argument(
        "--output-sqlite",
        type=Path,
        default=Path("etsy_turnover.db"),
        help="SQLite output file path",
    )
    out.add_argument(
        "--output-excel",
        type=Path,
        default=Path("etsy_turnover.xlsx"),
        help="Excel output file path",
    )
    out.add_argument(
        "--save-html",
        action="store_true",
        default=False,
        help="Save raw HTML snapshots to disk (useful for debugging selectors)",
    )
    out.add_argument(
        "--html-snapshot-dir",
        type=Path,
        default=Path("html_snapshots"),
        help="Directory for HTML snapshots (created automatically)",
    )
    out.add_argument(
        "--csv-export",
        action="store_true",
        default=False,
        help="Also export three CSV files alongside the SQLite database",
    )

    # ---- Behaviour ------------------------------------------------------
    behaviour = parser.add_argument_group("Behaviour")
    behaviour.add_argument(
        "--resume",
        action="store_true",
        default=False,
        help="Resume the most recent incomplete run for this shop/domain",
    )
    behaviour.add_argument(
        "--human-like",
        action="store_true",
        default=False,
        help="Apply longer, more human-like pacing delays between requests",
    )
    behaviour.add_argument(
        "--test-mode",
        action="store_true",
        default=False,
        help="Scrape only 2 pages per source (fast debugging mode)",
    )
    behaviour.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity level",
    )

    # ---- Retry ----------------------------------------------------------
    retry = parser.add_argument_group("Retry")
    retry.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retries per page navigation before giving up",
    )

    return parser.parse_args()


def build_config(args: argparse.Namespace) -> AppConfig:
    """Map parsed CLI args to a fully-typed AppConfig dataclass."""
    return AppConfig(
        shop_id=args.shop_id,
        shop_name=args.shop_name,
        domain=args.domain,
        browser=BrowserConfig(
            headless=args.headless,
            browser_type=args.browser_type,
            rotate_user_agents=args.rotate_user_agents,
            profile_dir=args.profile_dir,
        ),
        timing=TimingConfig(
            human_like=args.human_like,
        ),
        retry=RetryConfig(
            max_retries=args.max_retries,
        ),
        pagination=PaginationConfig(
            start_page_sold=args.start_page_sold,
            max_pages_sold=args.max_pages_sold,
            start_page_storefront=args.start_page_storefront,
            max_pages_storefront=args.max_pages_storefront,
            stop_on_empty=args.stop_on_empty,
        ),
        output=OutputConfig(
            sqlite_path=args.output_sqlite,
            excel_path=args.output_excel,
            save_html=args.save_html,
            html_snapshot_dir=args.html_snapshot_dir,
            csv_export=args.csv_export,
        ),
        resume=args.resume,
        test_mode=args.test_mode,
        log_level=args.log_level,
    )


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)
    log = logging.getLogger("main")

    config = build_config(args)
    log.info(
        "Starting Etsy Turnover Scraper — shop: %s | domain: %s | test_mode: %s",
        config.shop_name,
        config.domain,
        config.test_mode,
    )

    scraper = EtsyTurnoverScraper(config)

    try:
        summary = asyncio.run(scraper.run())
        log.info(
            "Run complete. Coverage: %.1f%% | Estimated turnover: %.2f",
            summary.price_match_coverage_pct,
            summary.estimated_turnover_sum,
        )
        log.info("SQLite: %s", config.output.sqlite_path.resolve())
        log.info("Excel:  %s", config.output.excel_path.resolve())

    except KeyboardInterrupt:
        log.warning("Interrupted by user (Ctrl+C).  Use --resume to continue.")
        sys.exit(1)

    except Exception as exc:
        log.error("Fatal error: %s", exc, exc_info=True)
        sys.exit(2)


if __name__ == "__main__":
    main()
