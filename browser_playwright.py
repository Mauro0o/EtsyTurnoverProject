"""
browser_playwright.py - Playwright browser management with anti-detection measures.

Responsibilities:
  - Launch/close Playwright browser context
  - Navigate pages with retries and exponential backoff
  - Apply anti-detection: custom UA, hide webdriver flag, configurable viewport
  - Dismiss cookie/privacy banners (GDPR consent overlays on Etsy)
  - Inject randomised delays to pace requests
  - Optionally save HTML snapshots for offline debugging
  - Clean shutdown of all browser resources

Anti-detection notes:
  - navigator.webdriver is removed via init script
  - Automation CLI args are suppressed where possible
  - Visible browser mode is the default (headless=False) to reduce fingerprint risk
  - User-agent can be rotated from a small pool
  - No aggressive techniques (proxy rotation, CAPTCHA solving) are used
"""

from __future__ import annotations

import asyncio
import logging
import random
from pathlib import Path
from typing import Optional

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    async_playwright,
)

from config import AppConfig, USER_AGENT_POOL

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cookie / GDPR banner selectors – tried in order.
# UPDATE this list if Etsy changes its consent overlay selectors.
# ---------------------------------------------------------------------------
COOKIE_DISMISS_SELECTORS: list[str] = [
    "button[data-gdpr-single-choice-accept]",
    "button#onetrust-accept-btn-handler",
    "button.gdpr-overlay-header__save-settings",
    "[data-selector='gdpr-single-choice-overlay'] button",
    "button:has-text('Accept')",
    "button:has-text('I Accept')",
    "button:has-text('Accept all')",
    "button:has-text('Accept All')",
    "button:has-text('Accept cookies')",
]


class BrowserManager:
    """Manages the full Playwright browser lifecycle for one scraping session."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Launch Playwright, create a browser and a page ready for navigation."""
        self._playwright = await async_playwright().start()
        browser_cfg = self.config.browser

        # Suppress automation signals where possible.
        launch_args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ]
        launch_kwargs: dict = {
            "headless": browser_cfg.headless,
            "args": launch_args,
        }

        # Firefox / WebKit do not support all Chromium launch args – strip them.
        if browser_cfg.browser_type == "firefox":
            launch_kwargs["args"] = []
            self._browser = await self._playwright.firefox.launch(**launch_kwargs)
        elif browser_cfg.browser_type == "webkit":
            launch_kwargs["args"] = []
            self._browser = await self._playwright.webkit.launch(**launch_kwargs)
        else:
            self._browser = await self._playwright.chromium.launch(**launch_kwargs)

        ua = self._select_user_agent()

        context_kwargs: dict = {
            "viewport": {
                "width": browser_cfg.viewport_width,
                "height": browser_cfg.viewport_height,
            },
            "user_agent": ua,
            "locale": "en-GB",
            "timezone_id": "Europe/London",
        }

        if browser_cfg.profile_dir and browser_cfg.browser_type == "chromium":
            # Persistent context preserves cookies/session across runs.
            # Only supported for Chromium.
            logger.info("Using persistent browser profile: %s", browser_cfg.profile_dir)
            self._context = await self._playwright.chromium.launch_persistent_context(
                str(browser_cfg.profile_dir),
                **launch_kwargs,
                **context_kwargs,
            )
        else:
            self._context = await self._browser.new_context(**context_kwargs)

        # Remove the webdriver fingerprint from navigator.
        await self._context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )

        self._page = await self._context.new_page()
        logger.info(
            "Browser started. headless=%s | engine=%s | UA=%.50s…",
            browser_cfg.headless,
            browser_cfg.browser_type,
            ua,
        )

    async def stop(self) -> None:
        """Cleanly close all browser resources.  Swallows errors during shutdown."""
        # Close in dependency order: page → context → browser → playwright.
        # Playwright uses .stop(), not .close() — calling .close() raises AttributeError.
        for resource, label, use_stop in [
            (self._page, "page", False),
            (self._context, "context", False),
            (self._browser, "browser", False),
            (self._playwright, "playwright", True),
        ]:
            if resource is None:
                continue
            try:
                if use_stop:
                    await resource.stop()
                else:
                    await resource.close()
            except Exception as exc:
                logger.debug("Error closing %s: %s", label, exc)
        self._page = None
        self._context = None
        self._browser = None
        self._playwright = None

    # ------------------------------------------------------------------
    # Navigation
    # ------------------------------------------------------------------

    async def navigate(self, url: str) -> str:
        """
        Navigate to *url* and return the page's full HTML content.

        Retries up to config.retry.max_retries times with exponential backoff.
        Raises RuntimeError when all retries are exhausted.
        """
        retry_cfg = self.config.retry
        timing_cfg = self.config.timing

        assert self._page is not None, "Browser not started – call start() first."

        for attempt in range(1, retry_cfg.max_retries + 1):
            try:
                logger.info(
                    "Navigating (attempt %d/%d): %s",
                    attempt,
                    retry_cfg.max_retries,
                    url,
                )
                await self._page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=timing_cfg.page_load_timeout_ms,
                )
                await self._wait_after_nav()
                await self._dismiss_cookie_banner()
                html = await self._page.content()
                logger.debug("Received %d bytes from %s", len(html), url)
                return html

            except Exception as exc:
                jitter = random.uniform(0, retry_cfg.retry_jitter)
                backoff = retry_cfg.retry_backoff_base ** attempt + jitter
                if attempt == retry_cfg.max_retries:
                    raise RuntimeError(
                        f"Failed to load {url} after {retry_cfg.max_retries} attempts: {exc}"
                    ) from exc
                logger.warning(
                    "Navigation error (attempt %d/%d) for %s: %s — retrying in %.1fs.",
                    attempt,
                    retry_cfg.max_retries,
                    url,
                    exc,
                    backoff,
                )
                await asyncio.sleep(backoff)

        raise RuntimeError("Unreachable – exhausted retry loop without returning or raising")

    # ------------------------------------------------------------------
    # Anti-detection helpers
    # ------------------------------------------------------------------

    async def _wait_after_nav(self) -> None:
        """Randomised pause after a navigation to simulate reading time."""
        t = self.config.timing
        # test_mode uses dedicated short delays to keep debug runs fast.
        if self.config.test_mode:
            delay = random.uniform(t.test_mode_min_delay, t.test_mode_max_delay)
        else:
            lo, hi = t.min_delay_after_nav, t.max_delay_after_nav
            if t.human_like:
                lo *= t.human_like_multiplier
                hi *= t.human_like_multiplier
            delay = random.uniform(lo, hi)
        logger.debug("Post-nav delay: %.2fs", delay)
        await asyncio.sleep(delay)

    async def inter_page_delay(self) -> None:
        """Randomised pause between page requests.  Call from the scraper loop."""
        t = self.config.timing
        if self.config.test_mode:
            delay = random.uniform(t.test_mode_min_delay, t.test_mode_max_delay)
        else:
            lo, hi = t.min_delay_between_pages, t.max_delay_between_pages
            if t.human_like:
                lo *= t.human_like_multiplier
                hi *= t.human_like_multiplier
            delay = random.uniform(lo, hi)
        logger.debug("Inter-page delay: %.2fs", delay)
        await asyncio.sleep(delay)

    async def _dismiss_cookie_banner(self) -> None:
        """
        Attempt to dismiss GDPR/cookie consent banners.

        Tries each selector in COOKIE_DISMISS_SELECTORS and clicks the first
        match found.  Silently continues if no banner is present.
        """
        assert self._page is not None
        for selector in COOKIE_DISMISS_SELECTORS:
            try:
                btn = await self._page.query_selector(selector)
                if btn:
                    await btn.click()
                    logger.debug("Dismissed cookie banner via: %s", selector)
                    await asyncio.sleep(0.8)
                    return
            except Exception:
                pass  # Banner absent or already dismissed

    def _select_user_agent(self) -> str:
        """Return a UA string, rotating from the pool if enabled."""
        cfg = self.config.browser
        if cfg.rotate_user_agents:
            return random.choice(USER_AGENT_POOL)
        return cfg.user_agent

    # ------------------------------------------------------------------
    # HTML snapshot saving
    # ------------------------------------------------------------------

    async def save_html_snapshot(
        self,
        html: str,
        directory: Path,
        filename: str,
    ) -> Path:
        """
        Write *html* to disk for offline debugging.

        Args:
            html:      The full page HTML string.
            directory: Directory to write into (created if absent).
            filename:  Target filename (e.g. 'sold_MyShop_p1.html').

        Returns:
            Absolute Path of the saved file.
        """
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / filename
        path.write_text(html, encoding="utf-8")
        logger.debug("Saved HTML snapshot: %s", path)
        return path
