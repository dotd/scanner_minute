import csv
import logging
import os
import threading
import time
from queue import Queue
from datetime import datetime, timezone, timedelta

from ScannerMinute.src import polygon_utils
from ScannerMinute.definitions import PROJECT_ROOT_DIR


DEFAULT_NUM_THREADS = 30
INDUSTRIES_DIR = os.path.join(PROJECT_ROOT_DIR, "data_industries")
DEFAULT_YEARS_BACK = 5


def _download_worker(worker_id, task_queue, result_dict, lock, progress, tickers_set):
    """Worker thread: download grouped daily bars for each date task."""
    tag = f"[W{worker_id}]"
    client = polygon_utils.get_polygon_client()
    while True:
        task = task_queue.get()
        if task is None:
            break
        date_str = task
        try:
            aggs = client.get_grouped_daily_aggs(date=date_str)
            with lock:
                for agg in aggs:
                    ticker = agg.ticker
                    if tickers_set and ticker not in tickers_set:
                        continue
                    if ticker not in result_dict:
                        result_dict[ticker] = []
                    result_dict[ticker].append(
                        {
                            "date": date_str,
                            "open": agg.open,
                            "high": agg.high,
                            "low": agg.low,
                            "close": agg.close,
                            "volume": agg.volume,
                            "timestamp": agg.timestamp,
                        }
                    )
                progress["done"] += 1
                done = progress["done"]
                total = progress["total"]
                elapsed = time.time() - progress["t0"]
                rate = done / elapsed if elapsed > 0 else 0
                remaining = (total - done) / rate if rate > 0 else 0
                n_tickers = len(
                    [t for t in aggs if not tickers_set or t.ticker in tickers_set]
                )
                logging.info(
                    f"{tag} {date_str}: {n_tickers} tickers | "
                    f"{done}/{total} ({done*100/total:.1f}%) | "
                    f"elapsed={elapsed:.1f}s | ETA={remaining:.1f}s"
                )
        except Exception as e:
            with lock:
                progress["done"] += 1
            logging.error(f"{tag} Error downloading {date_str}: {e}")


def _details_worker(worker_id, task_queue, result_dict, lock, progress):
    """Worker thread: fetch ticker details (sector/industry) from Polygon."""
    tag = f"[W{worker_id}]"
    client = polygon_utils.get_polygon_client()
    while True:
        ticker = task_queue.get()
        if ticker is None:
            break
        try:
            details = client.get_ticker_details(ticker)
            sic = getattr(details, "sic_description", None) or ""
            with lock:
                result_dict[ticker] = sic
                progress["done"] += 1
                done = progress["done"]
                total = progress["total"]
                elapsed = time.time() - progress["t0"]
                rate = done / elapsed if elapsed > 0 else 0
                remaining = (total - done) / rate if rate > 0 else 0
                logging.info(
                    f"{tag} {ticker}: {sic} | "
                    f"{done}/{total} ({done*100/total:.1f}%) | "
                    f"elapsed={elapsed:.1f}s | ETA={remaining:.1f}s"
                )
        except Exception as e:
            with lock:
                result_dict[ticker] = ""
                progress["done"] += 1
            logging.error(f"{tag} Error fetching details for {ticker}: {e}")


def _market_cap_worker(worker_id, task_queue, result_dict, lock, progress):
    """Worker thread: fetch market cap (weighted_shares_outstanding * close) from Polygon."""
    tag = f"[W{worker_id}]"
    client = polygon_utils.get_polygon_client()
    while True:
        ticker = task_queue.get()
        if ticker is None:
            break
        try:
            details = client.get_ticker_details(ticker)
            shares = getattr(details, "weighted_shares_outstanding", None)
            if not shares:
                shares = getattr(details, "share_class_shares_outstanding", None)
            market_cap = None
            if shares:
                # Get last close price from snapshot
                try:
                    snapshot = client.get_snapshot_ticker("stocks", ticker)
                    close = getattr(getattr(snapshot, "prev_day", None), "close", None)
                    if close:
                        market_cap = shares * close
                except Exception:
                    pass
            with lock:
                result_dict[ticker] = market_cap
                progress["done"] += 1
                done = progress["done"]
                total = progress["total"]
                elapsed = time.time() - progress["t0"]
                rate = done / elapsed if elapsed > 0 else 0
                remaining = (total - done) / rate if rate > 0 else 0
                cap_str = f"${market_cap / 1e9:.2f}B" if market_cap else "N/A"
                logging.info(
                    f"{tag} {ticker}: {cap_str} | "
                    f"{done}/{total} ({done*100/total:.1f}%) | "
                    f"elapsed={elapsed:.1f}s | ETA={remaining:.1f}s"
                )
        except Exception as e:
            with lock:
                result_dict[ticker] = None
                progress["done"] += 1
            logging.error(f"{tag} Error fetching market cap for {ticker}: {e}")


def fetch_market_caps(tickers, num_threads=DEFAULT_NUM_THREADS):
    """
    Multi-threaded fetch of market cap for each ticker.
    Market cap = weighted_shares_outstanding * prev_day close price.
    Returns dict: ticker -> market_cap (float in dollars), or None if unavailable.
    """
    logging.info(f"[fetch_market_caps] Fetching market caps for {len(tickers)} tickers...")
    task_queue = Queue()
    result_dict = {}
    lock = threading.Lock()
    progress = {"done": 0, "total": len(tickers), "t0": time.time()}

    for ticker in tickers:
        task_queue.put(ticker)
    for _ in range(num_threads):
        task_queue.put(None)

    threads = []
    for i in range(num_threads):
        t = threading.Thread(
            target=_market_cap_worker, args=(i, task_queue, result_dict, lock, progress)
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    logging.info(f"[fetch_market_caps] Done. Got market caps for {sum(1 for v in result_dict.values() if v)} / {len(tickers)} tickers")
    return result_dict


def _get_industries_csv_path(tickers):
    """Return the expected CSV path for today and the given ticker count."""
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    return os.path.join(INDUSTRIES_DIR, f"industries_{today}_{len(tickers)}.csv")


def _load_industries_csv(path):
    """Load ticker -> industry dict from a CSV file."""
    result = {}
    with open(path, "r") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            if len(row) >= 2:
                result[row[0]] = row[1]
    return result


def _save_industries_csv(path, result_dict):
    """Save ticker -> industry dict to a CSV file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ticker", "industry"])
        for ticker in sorted(result_dict.keys()):
            writer.writerow([ticker, result_dict[ticker]])


def fetch_ticker_industries(tickers, num_threads=DEFAULT_NUM_THREADS):
    """
    Multi-threaded fetch of SIC industry description for each ticker.
    Caches results as a CSV in data_industries/ per day and ticker count.
    Returns dict: ticker -> sic_description string.
    """
    csv_path = _get_industries_csv_path(tickers)

    # Check if today's cache exists
    if os.path.exists(csv_path):
        logging.info(f"[fetch_ticker_industries] Loading from cache: {csv_path}")
        return _load_industries_csv(csv_path)

    # Download from Polygon API
    logging.info(f"[fetch_ticker_industries] Downloading industries for {len(tickers)} tickers...")
    task_queue = Queue()
    result_dict = {}
    lock = threading.Lock()
    progress = {"done": 0, "total": len(tickers), "t0": time.time()}

    for ticker in tickers:
        task_queue.put(ticker)
    for _ in range(num_threads):
        task_queue.put(None)

    threads = []
    for i in range(num_threads):
        t = threading.Thread(
            target=_details_worker, args=(i, task_queue, result_dict, lock, progress)
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    # Save to CSV
    _save_industries_csv(csv_path, result_dict)
    logging.info(f"[fetch_ticker_industries] Saved {len(result_dict)} industries to {csv_path}")

    return result_dict


def get_trading_days(years_back=DEFAULT_YEARS_BACK):
    """Get list of trading days using Polygon, from years_back ago to today."""
    client = polygon_utils.get_polygon_client()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    date_start = (
        datetime.now(timezone.utc) - timedelta(days=years_back * 365)
    ).strftime("%Y-%m-%d")
    trading_days = polygon_utils.get_trading_days(client, from_=date_start, to=today)
    logging.info(f"Found {len(trading_days)} trading days from {date_start} to {today}")
    return trading_days


def download_all_daily_bars(tickers, num_threads=DEFAULT_NUM_THREADS, years_back=DEFAULT_YEARS_BACK):
    """
    Multi-threaded download of grouped daily bars for all trading days.
    Uses get_grouped_daily_aggs (regular session only) — one API call per day.
    Returns dict: ticker -> list of bar dicts.
    """
    trading_days = get_trading_days(years_back=years_back)
    tickers_set = set(tickers)

    task_queue = Queue()
    result_dict = {}
    lock = threading.Lock()
    progress = {"done": 0, "total": len(trading_days), "t0": time.time()}

    for day in trading_days:
        task_queue.put(day)

    for _ in range(num_threads):
        task_queue.put(None)

    threads = []
    for i in range(num_threads):
        t = threading.Thread(
            target=_download_worker,
            args=(i, task_queue, result_dict, lock, progress, tickers_set),
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    return result_dict


def _minute_download_worker(worker_id, task_queue, result_dict, lock, progress):
    """Worker thread: download 1-minute bars for a (ticker, date_start, date_end) task."""
    tag = f"[W{worker_id}]"
    client = polygon_utils.get_polygon_client()
    while True:
        task = task_queue.get()
        if task is None:
            break
        ticker, date_start, date_end = task
        try:
            data = polygon_utils.get_ticker_data_from_polygon(
                client, ticker, "minute", date_start, date_end
            )
            with lock:
                if ticker not in result_dict:
                    result_dict[ticker] = []
                result_dict[ticker].extend(data)
                progress["done"] += 1
                done = progress["done"]
                total = progress["total"]
                elapsed = time.time() - progress["t0"]
                rate = done / elapsed if elapsed > 0 else 0
                remaining = (total - done) / rate if rate > 0 else 0
                logging.info(
                    f"{tag} {ticker}: {len(data)} bars | "
                    f"{done}/{total} ({done*100/total:.1f}%) | "
                    f"elapsed={elapsed:.1f}s | ETA={remaining:.1f}s"
                )
        except Exception as e:
            with lock:
                progress["done"] += 1
            logging.error(f"{tag} Error downloading {ticker}: {e}")


def download_minute_daily_bars(tickers, date_start, date_end=None, num_threads=DEFAULT_NUM_THREADS):
    """
    Multi-threaded download of 1-minute bars for the given tickers and date range.
    Assumes the date range per ticker won't exceed the 50k bar API limit.

    Parameters:
        tickers: list[str] — tickers to download
        date_start: str — start date "YYYY-MM-DD"
        date_end: str or None — end date "YYYY-MM-DD" (None = today)
        num_threads: int — number of download threads

    Returns:
        dict: ticker -> list of bar rows (polygon_utils.COLUMNS format)
    """
    if date_end is None:
        date_end = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    logging.info(
        f"[download_minute_daily_bars] {len(tickers)} tickers, "
        f"{date_start} to {date_end}, {num_threads} threads"
    )

    task_queue = Queue()
    result_dict = {}
    lock = threading.Lock()
    progress = {"done": 0, "total": len(tickers), "t0": time.time()}

    for ticker in tickers:
        task_queue.put((ticker, date_start, date_end))
    for _ in range(num_threads):
        task_queue.put(None)

    threads = []
    for i in range(num_threads):
        t = threading.Thread(
            target=_minute_download_worker,
            args=(i, task_queue, result_dict, lock, progress),
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    return result_dict
