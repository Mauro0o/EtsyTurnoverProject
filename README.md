# Etsy Turnover Scraper

An exploratory Python tool that estimates turnover for a specific Etsy shop by
cross-matching sold listings with active storefront listings.

> **IMPORTANT – ESTIMATION ONLY**
> Etsy sold pages display the word "Sold" in place of the sale price.  The
> actual historical sale price is not exposed.  This tool uses the *current*
> active listing price as a proxy.  The resulting turnover figure is an
> **estimate** and should be treated as indicative only.

---

## URL model

Etsy embeds the market/country code in the **path**, not the hostname:

```
https://www.etsy.com/ie/shop/GearShiftAccessories/sold    ← Ireland
https://www.etsy.com/uk/shop/GearShiftAccessories/sold    ← UK
https://www.etsy.com/shop/GearShiftAccessories/sold       ← US (no prefix)
```

Configure this with `--market ie` (new) or `--domain etsy.ie` (legacy alias).
The host is always `www.etsy.com`.

---

## What it does

1. Scrapes all sold listings from `/{market}/shop/{shop_name}/sold` (paginated).
2. Scrapes all active listings from `/{market}/shop/{shop_name}` (paginated), optionally filtered by one or more keywords.
3. Detects the real last page number from the pagination bar — no blind crawling.
4. Matches sold listings to active listings by exact `listing_id`.
5. Aggregates repeated sold `listing_id`s — each occurrence = one sale event.
6. `estimated_turnover = active_price × sales_count` per matched listing.
7. Records which keyword found each active listing (`storefront_keyword` column).
8. Includes both `sold_listing_url` and `active_listing_url` in matched_turnover for visual verification.
9. Stores everything in SQLite and exports an Excel workbook.

---

## Sold listing deduplication modes

By default the scraper treats every row on the sold pages as a **distinct sale event**.
If the same `listing_id` appears twice, that means the item was sold twice.

| Mode | CLI flag | Behaviour |
|---|---|---|
| `preserve_all` | *(default)* | Every scraped row is kept. Repeated IDs → multiple sales. |
| `unique_listing_id` | `--sold-dedup-mode unique_listing_id` | Collapse to one row per `listing_id` (legacy). |

**Example — preserve_all (default):**
```
listing_id 123 appears 3× in sold pages
active price for 123 = €19.99
→ matched_turnover: sales_count=3, estimated_turnover=€59.97
```

**Example — unique_listing_id:**
```
listing_id 123 appears 3× in sold pages → collapsed to 1 sold row
active price for 123 = €19.99
→ matched_turnover: sales_count=1, estimated_turnover=€19.99
```

> **Note:** If you have an existing `etsy_turnover.db` from a previous version,
> delete it before running.  The `active_listings` PK changed to include
> `storefront_keyword`, and `matched_turnover` has two new URL columns.
> SQLite cannot migrate schemas in-place.

---

## Storefront keyword filter

Use `--storefront-keywords` to focus the storefront crawl on a specific subset of products.

- **Applies only to storefront pages** — sold pages are always scraped without a filter.
- Each keyword triggers its own separate paginated crawl.
- Pagination is detected from the filtered result set for each keyword.
- The keyword used to find each listing is stored in `active_listings.storefront_keyword`.
- If the same `listing_id` is found under two different keywords, both rows are kept in `active_listings`.
- For `matched_turnover`, a single canonical active row per `listing_id` is chosen (first encountered).

**URL structure with keyword:**
```
page 1:  https://www.etsy.com/ie/shop/{shop}/search_query=toyota#items
page 2:  https://www.etsy.com/ie/shop/{shop}?ref=condensed_trust_header_title_sold&search_query=toyota&page=2#items
```

**Single keyword:**
```bash
python main.py --shop-name GearShiftAccessories --market ie --storefront-keywords toyota
```

**Multiple keywords (separate crawl per keyword):**
```bash
python main.py --shop-name GearShiftAccessories --market ie --storefront-keywords toyota "gazoo racing" keyring
```

**No keyword — full storefront crawl (default):**
```bash
python main.py --shop-name GearShiftAccessories --market ie
```

---

## Matched turnover URL columns

`matched_turnover` now includes `sold_listing_url` and `active_listing_url` so you can open both sides of the match directly from Excel to verify they are the same product.

- `sold_listing_url` — URL of the sold listing (populated for all matched and unmatched rows when available).
- `active_listing_url` — URL of the matched active listing (NULL for unmatched rows).

---

## Project structure

```
EtsyTurnoverProject/
├── main.py                 CLI entry point
├── scraper.py              Orchestration logic
├── browser_playwright.py   Playwright browser management + anti-detection
├── parser.py               HTML parsing (BeautifulSoup)
├── models.py               Dataclasses: SoldListing, ActiveListing, ...
├── exporter.py             SQLite + Excel + CSV export
├── checkpoint.py           Run tracking and resume support
├── config.py               Centralized configuration dataclasses
├── url_builder.py          URL construction helpers
├── requirements.txt
└── README.md
```

---

## Installation

```bash
pip install -r requirements.txt
playwright install chromium
```

Python 3.11+ is recommended.

---

## Quick start

```bash
python main.py --shop-name stutututees --market ie
```

---

## Example commands (one-line format)

**Ireland shop (recommended style):**
```bash
python main.py --shop-name stutututees --market ie
```

**UK shop:**
```bash
python main.py --shop-name MyShop --market uk
```

**US shop (no market prefix):**
```bash
python main.py --shop-name MyShop
```

**Legacy --domain alias (still works, prints a deprecation warning):**
```bash
python main.py --shop-name stutututees --domain etsy.ie
```

**Headless mode with a custom output path:**
```bash
python main.py --shop-name stutututees --market ie --headless --output-sqlite data/shop.db --output-excel data/shop.xlsx
```

**Test mode (2 pages only, short delays, save HTML snapshots):**
```bash
python main.py --shop-name stutututees --market ie --test-mode --save-html --log-level DEBUG
```

**Resume an interrupted run:**
```bash
python main.py --shop-name stutututees --market ie --resume
```

**Human-like pacing (longer delays):**
```bash
python main.py --shop-name stutututees --market ie --human-like
```

**Rotate user-agents and export CSV files too:**
```bash
python main.py --shop-name stutututees --market ie --rotate-user-agents --csv-export
```

**Limit to first 5 pages of each source:**
```bash
python main.py --shop-name stutututees --market ie --max-pages-sold 5 --max-pages-storefront 5
```

**US shop with a shop ID:**
```bash
python main.py --shop-name MyShopName --shop-id 12345678
```

**Firefox browser engine:**
```bash
python main.py --shop-name stutututees --market ie --browser firefox
```

**Persistent browser profile (preserves cookies between runs):**
```bash
python main.py --shop-name stutututees --domain etsy.ie --profile-dir ./browser_profile
```

**Count each repeated sold listing as a separate sale (default — no flag needed):**
```bash
python main.py --shop-name stutututees --market ie
```

**Legacy unique mode — deduplicate sold rows by listing_id:**
```bash
python main.py --shop-name stutututees --market ie --sold-dedup-mode unique_listing_id
```

**Storefront keyword filter — single keyword:**
```bash
python main.py --shop-name GearShiftAccessories --market ie --storefront-keywords toyota
```

**Storefront keyword filter — multiple keywords:**
```bash
python main.py --shop-name GearShiftAccessories --market ie --storefront-keywords toyota "gazoo racing" keyring
```

---

## CLI reference

| Argument | Default | Description |
|---|---|---|
| `--shop-name` | *(required)* | Etsy shop slug as it appears in URLs |
| `--shop-id` | `""` | Numeric shop/seller ID (optional metadata) |
| `--market` | `""` | Market/country path prefix: `ie`, `uk`, `de`, etc. Empty = US |
| `--host` | `etsy.com` | Etsy host (rarely needs changing) |
| `--domain` | *(legacy)* | Deprecated alias for `--market`. `etsy.ie` → `--market ie` |
| `--headless` | `False` | Run browser without a visible window |
| `--browser` | `chromium` | Browser engine: `chromium`, `firefox`, `webkit` |
| `--rotate-user-agents` | `False` | Randomly pick a UA from a built-in pool |
| `--profile-dir` | `None` | Persistent browser profile path |
| `--start-page-sold` | `1` | First sold page number |
| `--max-pages-sold` | `100` | Maximum sold pages to scrape |
| `--start-page-storefront` | `1` | First storefront page number |
| `--max-pages-storefront` | `100` | Maximum storefront pages to scrape |
| `--output-sqlite` | `etsy_turnover.db` | SQLite output path |
| `--output-excel` | `etsy_turnover.xlsx` | Excel output path |
| `--save-html` | `False` | Save raw HTML pages to disk |
| `--html-snapshot-dir` | `html_snapshots/` | Directory for HTML files |
| `--csv-export` | `False` | Also write CSV files |
| `--resume` | `False` | Resume the last incomplete run |
| `--human-like` | `False` | Use longer, more natural delays |
| `--test-mode` | `False` | Scrape only 2 pages per source |
| `--log-level` | `INFO` | Verbosity: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `--max-retries` | `3` | Retries per page before skipping |
| `--sold-dedup-mode` | `preserve_all` | `preserve_all` = each sold row is a sale; `unique_listing_id` = deduplicate |
| `--storefront-keywords` | *(none)* | One or more keywords to filter storefront pages. Each triggers a separate crawl. Omit for full store. |

---

## Output files

### SQLite database (`etsy_turnover.db`)

| Table | Contents |
|---|---|
| `sold_listings` | All scraped sold listing cards |
| `active_listings` | All scraped active storefront listings |
| `matched_turnover` | Cross-match results with estimated prices |
| `scrape_runs` | One row per scraper invocation |
| `scrape_checkpoints` | Per-page progress for resume support |

### Excel workbook (`etsy_turnover.xlsx`)

Three sheets: `sold_listings`, `active_listings`, `matched_turnover`.

---

## Matching logic and limitations

### How matching works

The scraper performs **exact listing_id matching**:

- Each sold listing has a `listing_id` (extracted from `data-listing-id` HTML attribute).
- Each active listing also has a `listing_id`.
- A sold listing is **matched** when an identical `listing_id` is found on the storefront.
- When matched: `estimated_price` = active listing price; `matched_flag` = 1.
- When unmatched: `estimated_price` = NULL; `matched_flag` = 0.

### Why coverage is typically below 100%

- Items sold out and removed from the storefront will not match (most common).
- Listings retired after selling will not appear on the active storefront.
- New listings added after the sold data was captured also won't match.

### Why the price estimate may be wrong

- The actual sale price is not available on Etsy sold pages.
- Prices change over time (discounts, promotions, repricing).
- Listing variants may have different prices not reflected in the card price.

### Extending the matching strategy

The `_build_matched_turnover()` function in `scraper.py` is the right place to
add fuzzy title matching or other strategies in the future.  The `match_type`
field in the database is designed to accommodate multiple strategies.

---

## Updating selectors

If Etsy changes its HTML structure, update the constants at the top of
`parser.py`:

```python
CARD_LISTING_ID_ATTR = "data-listing-id"       # attribute on card root
CARD_CARD_V2_ATTR    = "data-listing-card-v2"  # secondary card identifier
LISTING_LINK_ATTR    = "data-listing-link"     # attribute on the <a> link tag
LISTING_IMAGE_ATTR   = "data-listing-card-listing-image"  # on <img>
PRICE_VALUE_SEL      = "span.currency-value"   # numeric price
CURRENCY_SYMBOL_SEL  = "span.currency-symbol"  # currency symbol
SOLD_STATUS_SEL      = "p.wt-text-title-01"    # "Sold" text on sold pages
```

Use `--save-html` combined with `--test-mode` to capture page snapshots, then
open them in a browser and use DevTools to find updated selectors.

---

## Anti-detection measures

The scraper uses defensive, low-volume techniques:

- Visible browser mode by default (headless must be explicitly requested).
- `navigator.webdriver` is removed via an init script.
- Randomised delays between navigations and between pages.
- Optional human-like mode doubles all delays.
- Configurable viewport matching a typical desktop resolution.
- GDPR/cookie banner auto-dismissal to avoid navigation blocks.
- Optional user-agent rotation from a small pool.
- Persistent browser profile support to preserve session cookies.
- Exponential backoff on retries.

**Not implemented**: proxy rotation, CAPTCHA solving, account-based scraping,
or any technique that would violate Etsy's Terms of Service.

This tool is for exploratory internal research only.  Use it responsibly and
at low volume.

---

## Resuming after interruption

If a run is interrupted (Ctrl+C, crash, network error), re-run with `--resume`:

```bash
python main.py --shop-name stutututees --domain etsy.ie --resume
```

The scraper will find the last incomplete run in the database and skip all
already-completed pages.

---

## License

For internal/exploratory use only.  Not affiliated with Etsy.
