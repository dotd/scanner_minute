import logging
import threading
import time
from queue import Queue
from datetime import datetime, timezone, timedelta

from ScannerMinute.src import polygon_utils


DEFAULT_NUM_THREADS = 30
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


def fetch_ticker_industries(tickers, num_threads=DEFAULT_NUM_THREADS):
    """
    Multi-threaded fetch of SIC industry description for each ticker.
    Returns dict: ticker -> sic_description string.
    """
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
