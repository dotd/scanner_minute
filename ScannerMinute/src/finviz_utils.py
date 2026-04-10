import logging
import os
import time
import datetime as dt
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field
from io import StringIO

import pandas as pd
import requests

# Finviz shows 20 rows per page
ROWS_PER_PAGE = 20
BASE_URL = "https://finviz.com/screener.ashx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36",
}

INTERESTING_FIELDS = [
    "Ticker",
    "Company",
    "Sector",
    "Industry",
    "Market Cap",
    "P/E",
    "Fwd P/E",
    "EPS",
    "Income",
    "Short Ratio",
    "Perf Week",
    "Perf Month",
    "Perf Quart",
    "Perf Half",
    "Perf Year",
    "Perf YTD",
    "Beta",
    "Volatility W",
    "Volatility M",
    "SMA20",
    "SMA50",
    "SMA200",
    "50D High",
    "50D Low",
    "52W High",
    "52W Low",
    "RSI",
    "Earnings",
    "Employees",
    "Avg Volume",
    "Rel Volume",
    "Volume",
    "Target Price",
    "Prev Close",
    "Price",
    "Change",
]


# ---------------------------------------------------------------------------
# URL generation
# ---------------------------------------------------------------------------


def build_screener_urls(size_filter=None, max_pages=None):
    """
    Build paginated Finviz screener URLs.
    size_filter: e.g. 'cap_smallover', 'cap_largeover', 'cap_midover', or None.
    max_pages: limit the number of pages (for testing).
    """
    filter_part = f"&f={size_filter}" if size_filter else ""
    # All numeric column IDs for the full data set
    col_ids = ",".join(str(i) for i in range(86))
    urls = []
    for start_row in range(1, 10000, ROWS_PER_PAGE):
        url = (
            f"{BASE_URL}?v=151{filter_part}&o=-marketcap" f"&c={col_ids}&r={start_row}"
        )
        urls.append(url)
    if max_pages:
        urls = urls[:max_pages]
    return urls


# ---------------------------------------------------------------------------
# Threaded download
# ---------------------------------------------------------------------------


def _download_one(url, session):
    """Download a single page. Returns (url, html, error_msg) tuple."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return url, resp.text, None
    except Exception as e:
        return url, None, str(e)


def download_pages(
    urls, initial_delay=5.0, delay_step=0.5, min_delay=0.05, max_retries=5
):
    """
    Download all URLs sequentially with adaptive delay.

    Starts with initial_delay seconds between requests. On each consecutive
    success the delay is reduced by delay_step (down to min_delay). On failure
    the URL is queued for retry and the delay is reset to initial_delay.

    Failed URLs are retried up to max_retries times. All permanent failures
    are logged at the end.

    Returns dict of {url: html}.
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    results = {}
    failures = {}  # url -> last error message
    retry_counts = {}  # url -> how many times retried
    delay = initial_delay
    total_pages = len(urls)
    t_start = time.time()

    pending = list(urls)
    attempt = 0

    while pending:
        attempt += 1
        next_round_retries = []

        logging.info(
            f"Download round {attempt}: {len(pending)} pages, delay={delay:.1f}s"
        )

        for i, url in enumerate(pending):
            if i > 0:
                time.sleep(delay)

            # Extract page number from URL for logging
            page_num = url.split("r=")[-1] if "r=" in url else "?"

            url, html, error = _download_one(url, session)

            done = len(results) + len(failures)
            remaining = total_pages - done
            elapsed = time.time() - t_start

            if html:
                # Check if the page has actual data; empty table means no more results
                page_df = _parse_page(html)
                if page_df is None or page_df.empty:
                    logging.info(
                        f"  page r={page_num} returned empty table — "
                        f"all data fetched, stopping early"
                    )
                    pending = []  # clear remaining pending
                    next_round_retries = []
                    break

                results[url] = html
                # Shorten delay on success (adaptive)
                delay = max(min_delay, delay - delay_step)

                # Estimate time remaining
                avg_per_page = elapsed / len(results) if results else 0
                eta = avg_per_page * remaining

                logging.info(
                    f"  page r={page_num} OK | "
                    f"{len(results)}/{total_pages} done, {remaining} left | "
                    f"delay={delay:.1f}s | "
                    f"elapsed={elapsed:.0f}s, ETA={eta:.0f}s"
                )
            else:
                count = retry_counts.get(url, 0) + 1
                retry_counts[url] = count
                if count < max_retries:
                    next_round_retries.append(url)
                    logging.warning(
                        f"  page r={page_num} FAILED ({count}/{max_retries}): {error} | "
                        f"{remaining} left | delay reset to {initial_delay:.1f}s"
                    )
                else:
                    failures[url] = error
                    logging.warning(
                        f"  page r={page_num} GAVE UP after {max_retries} attempts: {error}"
                    )
                # Reset delay on failure
                delay = initial_delay

        pending = next_round_retries

    # Log summary
    total_time = time.time() - t_start
    logging.info(
        f"Download complete: {len(results)} OK, {len(failures)} failed, "
        f"total time={total_time:.0f}s (final delay={delay:.1f}s)"
    )
    if failures:
        logging.warning(f"=== {len(failures)} permanently failed URLs ===")
        for url, error in failures.items():
            logging.warning(f"  FAILED: {url} — {error}")

    return results


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------


def _parse_page(html):
    """
    Extract the main data table from a Finviz screener page.
    Returns a DataFrame or None.
    """
    try:
        tables = pd.read_html(StringIO(html), header=0)
    except Exception as e:
        logging.debug(f"read_html failed: {e}")
        return None

    if not tables:
        return None

    # The data table is the largest one with >10 rows
    tables.sort(key=lambda t: t.shape[0], reverse=True)
    df = tables[0]
    if df.shape[0] < 2:
        return None
    return df


def parse_all_pages(page_htmls):
    """
    Parse all downloaded HTML pages and concatenate into one DataFrame.
    page_htmls: dict of {url: html}.
    """
    frames = []
    for url, html in page_htmls.items():
        df = _parse_page(html)
        if df is not None and not df.empty:
            frames.append(df)
        else:
            logging.warning(f"No usable table from {url}")

    if not frames:
        logging.error("No data tables found in any page")
        return pd.DataFrame()

    big_table = pd.concat(frames, ignore_index=True)

    # Drop the row-number column if present
    if "No." in big_table.columns:
        big_table = big_table.drop(columns=["No."])

    # Drop fully-duplicate rows (from overlapping pages)
    if "Ticker" in big_table.columns:
        big_table = big_table.drop_duplicates(subset=["Ticker"], keep="first")

    logging.info(
        f"Combined table: {big_table.shape[0]} rows, {big_table.shape[1]} columns"
    )
    return big_table


# ---------------------------------------------------------------------------
# Type transformations
# ---------------------------------------------------------------------------


def _float_safe(s, default=0.0):
    """
    Convert a Finviz string value to float.
    Handles B (billions), M (millions), K (thousands), % (percent as fraction).
    """
    if pd.isna(s) or s == "-":
        return default
    s = str(s).strip()
    if not s:
        return default
    try:
        return float(s)
    except ValueError:
        pass
    suffix = s[-1]
    try:
        num = float(s[:-1])
    except ValueError:
        return default
    multipliers = {"B": 1e9, "M": 1e6, "K": 1e3, "%": 0.01}
    return num * multipliers.get(suffix, 1.0)


def _get_transformations():
    """Column name -> (transform_fn, dtype) for all known Finviz columns."""
    d = OrderedDict()
    _s = lambda x: str(x) if pd.notna(x) else ""
    _f = lambda x: _float_safe(x)

    str_cols = [
        "Ticker",
        "Company",
        "Sector",
        "Industry",
        "Country",
        "Index",
        "Earnings",
        "Optionable",
        "Shortable",
    ]
    for col in str_cols:
        d[col] = (_s, str)

    float_cols = [
        "Market Cap",
        "P/E",
        "Fwd P/E",
        "PEG",
        "P/S",
        "P/B",
        "P/C",
        "P/FCF",
        "Book/sh",
        "Cash/sh",
        "Dividend",
        "Dividend.1",
        "Payout Ratio",
        "EPS",
        "EPS next Q",
        "EPS this Y",
        "EPS next Y",
        "EPS past 5Y",
        "EPS next 5Y",
        "Sales past 5Y",
        "Sales Q/Q",
        "EPS Q/Q",
        "Sales",
        "Income",
        "Outstanding",
        "Float",
        "Insider Own",
        "Insider Trans",
        "Inst Own",
        "Inst Trans",
        "Float Short",
        "Short Ratio",
        "Short Interest",
        "ROA",
        "ROE",
        "ROI",
        "Curr R",
        "Quick R",
        "LTDebt/Eq",
        "Debt/Eq",
        "Gross M",
        "Oper M",
        "Profit M",
        "Perf Week",
        "Perf Month",
        "Perf Quart",
        "Perf Half",
        "Perf Year",
        "Perf YTD",
        "Beta",
        "ATR",
        "Volatility W",
        "Volatility M",
        "SMA20",
        "SMA50",
        "SMA200",
        "50D High",
        "50D Low",
        "52W High",
        "52W Low",
        "RSI",
        "Employees",
        "from Open",
        "Change from Open",
        "Gap",
        "Recom",
        "Avg Volume",
        "Rel Volume",
        "Volume",
        "Target Price",
        "Prev Close",
        "Price",
        "Change",
        "Short Float",
    ]
    for col in float_cols:
        d[col] = (_f, float)

    return d


def apply_transformations(table):
    """Apply type transformations to all known columns."""
    transformations = _get_transformations()
    for col in table.columns:
        if col in transformations:
            fn, _ = transformations[col]
            table[col] = table[col].apply(fn)
    return table


# ---------------------------------------------------------------------------
# Main API
# ---------------------------------------------------------------------------


def download_finviz(
    size_filter=None,
    max_pages=None,
    fields=None,
    initial_delay=5.0,
    delay_step=0.5,
    min_delay=0.05,
    max_retries=5,
):
    """
    Download and parse the full Finviz screener.

    Args:
        size_filter: 'cap_smallover', 'cap_largeover', 'cap_midover', or None.
        max_pages: limit pages to download (None = all).
        fields: list of columns to keep (None = all).
        initial_delay: starting delay between requests in seconds.
        delay_step: reduce delay by this much on each consecutive success.
        min_delay: minimum delay between requests.
        max_retries: max retry attempts per failed URL.

    Returns a cleaned pandas DataFrame.
    """
    urls = build_screener_urls(size_filter=size_filter, max_pages=max_pages)
    logging.info(f"Finviz: {len(urls)} pages to download (filter={size_filter})")

    htmls = download_pages(
        urls,
        initial_delay=initial_delay,
        delay_step=delay_step,
        min_delay=min_delay,
        max_retries=max_retries,
    )
    table = parse_all_pages(htmls)

    if table.empty:
        return table

    table = apply_transformations(table)

    if fields:
        available = [f for f in fields if f in table.columns]
        table = table[available]

    return table


def download_finviz_cached(
    size_filter=None,
    max_pages=200,
    fields=None,
    initial_delay=1.0,
    delay_step=0.1,
    min_delay=0.05,
    max_retries=5,
    cache_folder="./data/finviz",
    cache_max_age_seconds=1000,
):
    """
    Like download_finviz, but caches to CSV. If a recent-enough cache exists,
    loads from disk instead of re-downloading.

    Returns (DataFrame, filename).
    """
    os.makedirs(cache_folder, exist_ok=True)
    prefix = "finviz"
    filter_tag = f"_{size_filter}" if size_filter else ""

    # Check for recent cache
    cached_files = sorted(
        [
            f
            for f in os.listdir(cache_folder)
            if f.startswith(f"{prefix}{filter_tag}_") and f.endswith(".csv")
        ],
    )
    if cached_files:
        latest = cached_files[-1]
        try:
            ts_str = latest.replace(f"{prefix}{filter_tag}_", "").replace(".csv", "")
            file_time = dt.datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
            age = (dt.datetime.now() - file_time).total_seconds()
            if age < cache_max_age_seconds:
                path = os.path.join(cache_folder, latest)
                logging.info(f"Loading cached Finviz data from {path} (age={age:.0f}s)")
                return pd.read_csv(path), latest
        except (ValueError, OSError) as e:
            logging.debug(f"Could not parse cache file {latest}: {e}")

    # Download fresh
    table = download_finviz(
        size_filter=size_filter,
        max_pages=max_pages,
        fields=fields,
        initial_delay=initial_delay,
        delay_step=delay_step,
        min_delay=min_delay,
        max_retries=max_retries,
    )

    if table.empty:
        return table, None

    # Save
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}{filter_tag}_{timestamp}.csv"
    path = os.path.join(cache_folder, filename)
    table.to_csv(path, index=False)
    logging.info(f"Saved Finviz data to {path}")

    return table, filename


@dataclass
class SectorIndustryMap:
    sectors: list
    industries: list
    tickers_by_sector: dict
    tickers_by_industry: dict
    ticker_to_sector: dict
    ticker_to_industry: dict


def get_sector_industry_mappings(df):
    """
    Build sector/industry mappings from a Finviz DataFrame.

    Args:
        df: DataFrame with columns 'Ticker', 'Sector', 'Industry'
            (as returned by download_finviz or download_finviz_cached).

    Returns:
        SectorIndustryMap with:
            sectors            — sorted list of unique sectors
            industries         — sorted list of unique industries
            tickers_by_sector  — dict mapping sector -> sorted list of tickers
            tickers_by_industry — dict mapping industry -> sorted list of tickers
            ticker_to_sector   — dict mapping ticker -> sector
            ticker_to_industry — dict mapping ticker -> industry
    """
    ticker_to_sector = {}
    ticker_to_industry = {}
    tickers_by_sector = defaultdict(list)
    tickers_by_industry = defaultdict(list)

    for _, row in df.iterrows():
        ticker = row["Ticker"]
        sector = row.get("Sector", "")
        industry = row.get("Industry", "")

        ticker_to_sector[ticker] = sector
        ticker_to_industry[ticker] = industry
        tickers_by_sector[sector].append(ticker)
        tickers_by_industry[industry].append(ticker)

    # Sort all lists
    for k in tickers_by_sector:
        tickers_by_sector[k].sort()
    for k in tickers_by_industry:
        tickers_by_industry[k].sort()

    return SectorIndustryMap(
        sectors=sorted(tickers_by_sector.keys()),
        industries=sorted(tickers_by_industry.keys()),
        tickers_by_sector=dict(tickers_by_sector),
        tickers_by_industry=dict(tickers_by_industry),
        ticker_to_sector=ticker_to_sector,
        ticker_to_industry=ticker_to_industry,
    )
