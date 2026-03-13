"""
scraper.py - Orchestration logic for the Etsy Turnover Scraper.

Full run flow:
  1. Connect to SQLite and checkpoint system.
  2. Resume or start a fresh run.
  3. Phase 1 - Scrape all sold listings (paginated /sold pages).
  4. Phase 2 - Scrape all active storefront listings (paginated /shop pages).
  5. Deduplicate each set by listing_id.
  6. Persist raw listings to SQLite (also persisted incrementally per page).
  7. Cross-match sold vs active listings by exact listing_id.
  8. Persist matched_turnover table.
  9. Export to Excel workbook.
 10. Optionally export CSV files.
 11. Compute and log summary metrics.

Matching logic and limitations:
----------------------------------------------------------------------
  Strategy: exact_listing_id
    A sold listing is matched to an active listing when their listing_id
    values are identical.  The active listing's current price is used as the
    estimated_price (and estimated_turnover) for the sold record.

  Limitations:
    - Sold pages do NOT expose the actual sale price.  The price used is
      the *current* listing price at scrape time.
    - Listings removed from the storefront after sale will not match.
      This typically under-represents turnover.
    - Prices may have changed since the item was sold.

  Future extension point:
    Fuzzy title matching is NOT implemented.  To add it, implement a new
    match pass in _build_matched_turnover() after the exact-match pass.
    Update the 'match_type' field accordingly (e.g. "fuzzy_title").
----------------------------------------------------------------------
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from browser_playwright import BrowserManager
from checkpoint import STAGE_SOLD, STAGE_STOREFRONT, CheckpointManager
from config import AppConfig
from exporter import ExcelExporter, SQLiteExporter, export_csv
from models import ActiveListing, MatchedTurnoverRow, RunSummary, SoldListing
from parser import parse_last_page_number, parse_sold_page, parse_storefront_page
from url_builder import build_sold_url, build_storefront_url

logger = logging.getLogger(__name__)


class EtsyTurnoverScraper:
    """Top-level orchestrator: coordinates browser, parser, storage and export."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.browser = BrowserManager(config)
        self.db = SQLiteExporter(config.output.sqlite_path)
        self.checkpoint = CheckpointManager(config.output.sqlite_path)

    async def run(self) -> RunSummary:
        """
        Execute the full scrape -> match -> export pipeline.

        Returns a RunSummary with aggregated metrics.
        Raises the underlying exception if a fatal error occurs after cleanup.
        """
        cfg = self.config

        # Connect storage layer (checkpoint and db share the same SQLite file).
        self.db.connect()
        self.checkpoint.connect()

        # A stable string identifier for checkpoint lookups.
        # Combines host + market so runs for different markets are distinct.
        domain_key = f"{cfg.host}/{cfg.market}" if cfg.market else cfg.host

        # Resolve or create run.
        run_id: Optional[str] = None
        if cfg.resume:
            run_id = self.checkpoint.find_incomplete_run(cfg.shop_name, domain_key)
            if run_id:
                self.checkpoint.run_id = run_id
                logger.info(
                    "Resuming run %s for %s (%s)", run_id, cfg.shop_name, domain_key
                )
            else:
                logger.info(
                    "No resumable run found for %s (%s) - starting fresh.",
                    cfg.shop_name, domain_key,
                )
        if not run_id:
            run_id = self.checkpoint.start_run(cfg.shop_name, cfg.shop_id, domain_key)

        await self.browser.start()

        sold_listings: list[SoldListing] = []
        active_listings: list[ActiveListing] = []

        try:
            # ---- Phase 1: sold listings --------------------------------
            sold_listings = await self._scrape_sold(
                cfg.shop_name, cfg.shop_id, cfg.host, cfg.market
            )

            # ---- Phase 2: active storefront ----------------------------
            active_listings = await self._scrape_storefront(
                cfg.shop_name, cfg.shop_id, cfg.host, cfg.market
            )

            # ---- Phase 3: deduplicate ----------------------------------
            sold_listings = _deduplicate_sold(sold_listings)
            active_listings = _deduplicate_active(active_listings)
            logger.info(
                "After deduplication - sold: %d unique, active: %d unique",
                len(sold_listings), len(active_listings),
            )

            # ---- Phase 4: persist raw listings -------------------------
            # Pages are also persisted incrementally during scraping, so this
            # final upsert is idempotent and catches any in-memory-only state.
            self.db.upsert_sold_listings(sold_listings)
            self.db.upsert_active_listings(active_listings)

            # ---- Phase 5: match and build turnover ---------------------
            now_ts = datetime.now(timezone.utc).isoformat()
            matched_rows = _build_matched_turnover(
                sold_listings, active_listings,
                domain_key, cfg.shop_name, now_ts,
            )
            self.db.upsert_matched_turnover(matched_rows)

            # ---- Phase 6: export Excel ---------------------------------
            self._export_excel()

            # ---- Phase 7: optional CSV export --------------------------
            if cfg.output.csv_export:
                self._export_csv()

            # ---- Phase 8: summary metrics ------------------------------
            summary = _compute_summary(sold_listings, active_listings, matched_rows)
            _log_summary(summary, cfg.shop_name, domain_key)

            self.checkpoint.finish_run("completed")
            return summary

        except Exception as exc:
            logger.error("Run failed: %s", exc, exc_info=True)
            self.checkpoint.finish_run("failed")
            raise

        finally:
            await self.browser.stop()
            # Yield to the event loop so Windows proactor can drain any pending
            # pipe/transport callbacks before asyncio.run() tears down the loop.
            await asyncio.sleep(0)
            self.db.close()
            self.checkpoint.close()

    # ------------------------------------------------------------------
    # Phase 1: Sold listings
    # ------------------------------------------------------------------

    async def _scrape_sold(
        self, shop_name: str, shop_id: str, host: str, market: str
    ) -> list[SoldListing]:
        """
        Scrape paginated sold pages and return all SoldListing records found.

        Pagination behavior:
          1. Load page 1 and parse detected_last_page from the pagination bar.
          2. Clamp the loop upper bound to min(detected_last_page, max_pages_sold).
          3. In test_mode cap at 2 pages regardless.
          4. Stop early if a page returns zero listings (unexpected gap).
          5. On resume, skip already-completed pages.
        """
        cfg = self.config
        pag = cfg.pagination
        start_page = pag.start_page_sold

        # Advance start page past already-completed pages when resuming.
        if cfg.resume and self.checkpoint.run_id:
            last_done = self.checkpoint.get_last_completed_page(STAGE_SOLD)
            if last_done >= start_page:
                start_page = last_done + 1
                logger.info("[SOLD] Resuming from page %d", start_page)

        all_results: list[SoldListing] = []
        detected_last_page: Optional[int] = None  # discovered after page 1

        page = start_page
        while True:
            # ---- Determine loop ceiling --------------------------------
            if detected_last_page is not None:
                user_limit = 2 if cfg.test_mode else pag.max_pages_sold
                crawl_limit = min(detected_last_page, start_page + user_limit - 1)
            else:
                crawl_limit = start_page + (1 if cfg.test_mode else pag.max_pages_sold) - 1

            if page > crawl_limit:
                break

            # Skip already-completed pages on resume.
            if cfg.resume and self.checkpoint.is_page_completed(STAGE_SOLD, page):
                logger.debug("[SOLD] Skipping completed page %d", page)
                page += 1
                continue

            url = build_sold_url(host, market, shop_name, page)
            self.checkpoint.mark_page_started(STAGE_SOLD, page)

            try:
                html = await self.browser.navigate(url)
                snapshot_path: Optional[str] = None

                if cfg.output.save_html:
                    snap = await self.browser.save_html_snapshot(
                        html,
                        cfg.output.html_snapshot_dir,
                        f"sold_{shop_name}_p{page:04d}.html",
                    )
                    snapshot_path = str(snap)

                # Detect last page from pagination bar (done once, on page 1,
                # or whenever not yet detected).
                if detected_last_page is None:
                    detected_last_page = parse_last_page_number(html, "sold")
                    user_limit = 2 if cfg.test_mode else pag.max_pages_sold
                    crawl_limit = min(
                        detected_last_page,
                        start_page + user_limit - 1,
                    )
                    logger.info(
                        "[SOLD] Detected last page: %d | User limit: %d | "
                        "Will crawl up to page: %d",
                        detected_last_page, user_limit, crawl_limit,
                    )

                listings = parse_sold_page(
                    html=html,
                    page_url=url,
                    page_number=page,
                    host=host,
                    shop_name=shop_name,
                    shop_id=shop_id,
                    snapshot_path=snapshot_path,
                )
                logger.info("[SOLD] Page %d/%d -> %d listings", page, crawl_limit, len(listings))

                if not listings:
                    if pag.stop_on_empty:
                        logger.warning(
                            "[SOLD] Page %d returned 0 listings (expected ≤%d). "
                            "Stopping early.",
                            page, crawl_limit,
                        )
                        self.checkpoint.mark_page_completed(STAGE_SOLD, page)
                        break
                    # No stop_on_empty: log and continue anyway.
                    logger.warning("[SOLD] Page %d returned 0 listings.", page)

                all_results.extend(listings)
                self.checkpoint.mark_page_completed(STAGE_SOLD, page)

                if listings:
                    self.db.upsert_sold_listings(listings)

                if page >= crawl_limit:
                    logger.info("[SOLD] Reached crawl limit (page %d). Done.", page)
                    break

                await self.browser.inter_page_delay()
                page += 1

            except Exception as exc:
                self.checkpoint.mark_page_failed(STAGE_SOLD, page, str(exc))
                logger.error("[SOLD] Page %d failed: %s - continuing.", page, exc)
                page += 1

        logger.info("[SOLD] Total collected: %d listings", len(all_results))
        return all_results

    # ------------------------------------------------------------------
    # Phase 2: Active storefront listings
    # ------------------------------------------------------------------

    async def _scrape_storefront(
        self, shop_name: str, shop_id: str, host: str, market: str
    ) -> list[ActiveListing]:
        """
        Scrape paginated storefront pages and return all ActiveListing records.

        Same pagination-aware loop as _scrape_sold: detect last page from the
        pagination bar on page 1, clamp to user's max_pages_storefront limit.
        """
        cfg = self.config
        pag = cfg.pagination
        start_page = pag.start_page_storefront

        if cfg.resume and self.checkpoint.run_id:
            last_done = self.checkpoint.get_last_completed_page(STAGE_STOREFRONT)
            if last_done >= start_page:
                start_page = last_done + 1
                logger.info("[STOREFRONT] Resuming from page %d", start_page)

        all_results: list[ActiveListing] = []
        detected_last_page: Optional[int] = None

        page = start_page
        while True:
            if detected_last_page is not None:
                user_limit = 2 if cfg.test_mode else pag.max_pages_storefront
                crawl_limit = min(detected_last_page, start_page + user_limit - 1)
            else:
                crawl_limit = start_page + (1 if cfg.test_mode else pag.max_pages_storefront) - 1

            if page > crawl_limit:
                break

            if cfg.resume and self.checkpoint.is_page_completed(STAGE_STOREFRONT, page):
                logger.debug("[STOREFRONT] Skipping completed page %d", page)
                page += 1
                continue

            url = build_storefront_url(host, market, shop_name, page)
            self.checkpoint.mark_page_started(STAGE_STOREFRONT, page)

            try:
                html = await self.browser.navigate(url)
                snapshot_path: Optional[str] = None

                if cfg.output.save_html:
                    snap = await self.browser.save_html_snapshot(
                        html,
                        cfg.output.html_snapshot_dir,
                        f"storefront_{shop_name}_p{page:04d}.html",
                    )
                    snapshot_path = str(snap)

                if detected_last_page is None:
                    detected_last_page = parse_last_page_number(html, "storefront")
                    user_limit = 2 if cfg.test_mode else pag.max_pages_storefront
                    crawl_limit = min(
                        detected_last_page,
                        start_page + user_limit - 1,
                    )
                    logger.info(
                        "[STOREFRONT] Detected last page: %d | User limit: %d | "
                        "Will crawl up to page: %d",
                        detected_last_page, user_limit, crawl_limit,
                    )

                listings = parse_storefront_page(
                    html=html,
                    page_url=url,
                    page_number=page,
                    host=host,
                    shop_name=shop_name,
                    shop_id=shop_id,
                    snapshot_path=snapshot_path,
                )
                logger.info(
                    "[STOREFRONT] Page %d/%d -> %d listings", page, crawl_limit, len(listings)
                )

                if not listings:
                    if pag.stop_on_empty:
                        logger.warning(
                            "[STOREFRONT] Page %d returned 0 listings. Stopping early.", page
                        )
                        self.checkpoint.mark_page_completed(STAGE_STOREFRONT, page)
                        break
                    logger.warning("[STOREFRONT] Page %d returned 0 listings.", page)

                all_results.extend(listings)
                self.checkpoint.mark_page_completed(STAGE_STOREFRONT, page)

                if listings:
                    self.db.upsert_active_listings(listings)

                if page >= crawl_limit:
                    logger.info("[STOREFRONT] Reached crawl limit (page %d). Done.", page)
                    break

                await self.browser.inter_page_delay()
                page += 1

            except Exception as exc:
                self.checkpoint.mark_page_failed(STAGE_STOREFRONT, page, str(exc))
                logger.error("[STOREFRONT] Page %d failed: %s - continuing.", page, exc)
                page += 1

        logger.info("[STOREFRONT] Total collected: %d listings", len(all_results))
        return all_results

    # ------------------------------------------------------------------
    # Export helpers
    # ------------------------------------------------------------------

    def _export_excel(self) -> None:
        """Read all rows from SQLite and write the Excel workbook."""
        ExcelExporter().export(
            sold_rows=self.db.fetch_all_sold(),
            active_rows=self.db.fetch_all_active(),
            matched_rows=self.db.fetch_all_matched(),
            output_path=self.config.output.excel_path,
        )

    def _export_csv(self) -> None:
        """Export three CSV files alongside the SQLite database."""
        base = self.config.output.sqlite_path.parent
        export_csv(self.db.fetch_all_sold(), base / "sold_listings.csv")
        export_csv(self.db.fetch_all_active(), base / "active_listings.csv")
        export_csv(self.db.fetch_all_matched(), base / "matched_turnover.csv")


# ---------------------------------------------------------------------------
# Matching / turnover computation
# ---------------------------------------------------------------------------


def _build_matched_turnover(
    sold_listings: list[SoldListing],
    active_listings: list[ActiveListing],
    domain: str,
    shop_name: str,
    scrape_timestamp: str,
) -> list[MatchedTurnoverRow]:
    """
    Cross-match sold listings against active listings by exact listing_id.

    IMPORTANT - ESTIMATION ONLY:
      The active/current listing price is used as a proxy for the sold price.
      This will be wrong (or unavailable) when:
        - The item is no longer on the storefront (no match).
        - The price has changed since the item sold.
        - Discounts, variants, or bulk pricing applied at sale time.

    matched_flag=1  -> exact match; estimated_price = active price.
    matched_flag=0  -> no match; estimated_price = None.

    To extend with fuzzy title matching, add a second pass here and set
    match_type = "fuzzy_title".  Do not implement it until exact matching
    coverage is measured and found insufficient.
    """
    # Build a quick lookup of active listings by listing_id.
    active_by_id: dict[str, ActiveListing] = {a.listing_id: a for a in active_listings}

    results: list[MatchedTurnoverRow] = []

    for sold in sold_listings:
        active = active_by_id.get(sold.listing_id)

        if active:
            results.append(
                MatchedTurnoverRow(
                    scrape_timestamp=scrape_timestamp,
                    domain=domain,
                    shop_name=shop_name,
                    sold_listing_id=sold.listing_id,
                    active_listing_id=active.listing_id,
                    match_type="exact_listing_id",
                    sold_title=sold.product_title,
                    active_title=active.product_title,
                    estimated_price=active.price,
                    currency=active.currency,
                    # For the initial strategy, turnover per unit = price.
                    estimated_turnover=active.price,
                    matched_flag=1,
                    notes=(
                        "Estimated from current active listing price. "
                        "NOT the actual historical sale price."
                    ),
                )
            )
        else:
            results.append(
                MatchedTurnoverRow(
                    scrape_timestamp=scrape_timestamp,
                    domain=domain,
                    shop_name=shop_name,
                    sold_listing_id=sold.listing_id,
                    active_listing_id=None,
                    match_type=None,
                    sold_title=sold.product_title,
                    active_title=None,
                    estimated_price=None,
                    currency=sold.currency,
                    estimated_turnover=None,
                    matched_flag=0,
                    notes="No matching active listing found for this sold listing ID.",
                )
            )

    exact = sum(1 for r in results if r.matched_flag == 1)
    logger.info(
        "Matching complete: %d/%d sold listings matched to an active price.",
        exact, len(results),
    )
    return results


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------


def _deduplicate_sold(listings: list[SoldListing]) -> list[SoldListing]:
    """Keep only the first occurrence of each listing_id."""
    seen: dict[str, SoldListing] = {}
    for listing in listings:
        if listing.listing_id not in seen:
            seen[listing.listing_id] = listing
    dupes = len(listings) - len(seen)
    if dupes:
        logger.debug("Removed %d duplicate sold listings", dupes)
    return list(seen.values())


def _deduplicate_active(listings: list[ActiveListing]) -> list[ActiveListing]:
    """Keep only the first occurrence of each listing_id."""
    seen: dict[str, ActiveListing] = {}
    for listing in listings:
        if listing.listing_id not in seen:
            seen[listing.listing_id] = listing
    dupes = len(listings) - len(seen)
    if dupes:
        logger.debug("Removed %d duplicate active listings", dupes)
    return list(seen.values())


# ---------------------------------------------------------------------------
# Summary computation and logging
# ---------------------------------------------------------------------------


def _compute_summary(
    sold: list[SoldListing],
    active: list[ActiveListing],
    matched: list[MatchedTurnoverRow],
) -> RunSummary:
    exact = sum(1 for r in matched if r.matched_flag == 1)
    turnover_sum = sum(
        r.estimated_turnover
        for r in matched
        if r.matched_flag == 1 and r.estimated_turnover is not None
    )
    coverage = (exact / len(sold) * 100.0) if sold else 0.0
    return RunSummary(
        total_sold_rows=len(sold),
        total_active_rows=len(active),
        exact_matches=exact,
        unmatched_sold_rows=len(sold) - exact,
        price_match_coverage_pct=round(coverage, 2),
        estimated_turnover_sum=round(turnover_sum, 2),
    )


def _log_summary(summary: RunSummary, shop_name: str, domain: str) -> None:
    sep = "=" * 64
    logger.info(sep)
    logger.info("  ETSY TURNOVER ESTIMATION SUMMARY")
    logger.info("  Shop : %s   Domain : %s", shop_name, domain)
    logger.info(sep)
    logger.info("  Sold listings scraped   : %d", summary.total_sold_rows)
    logger.info("  Active listings scraped : %d", summary.total_active_rows)
    logger.info("  Exact ID matches        : %d", summary.exact_matches)
    logger.info("  Unmatched sold rows     : %d", summary.unmatched_sold_rows)
    logger.info("  Price match coverage    : %.1f%%", summary.price_match_coverage_pct)
    logger.info("  Estimated turnover sum  : %.2f", summary.estimated_turnover_sum)
    logger.info(sep)
    logger.info("  NOTE: Turnover is an ESTIMATE based on current active listing prices,")
    logger.info("  NOT actual historical sale prices.  See README.md for limitations.")
    logger.info(sep)
