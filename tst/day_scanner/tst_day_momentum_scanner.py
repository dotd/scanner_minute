import logging
import threading
import time
from queue import Queue
from datetime import datetime, timezone, timedelta

from ScannerMinute.src import logging_utils, polygon_utils
from ScannerMinute.src.ticker_utils import ALL_TICKERS


NUM_THREADS = 30
YEARS_BACK = 5


def _download_worker(worker_id, task_queue, result_dict, lock, progress):
    """Worker thread: download daily bars for each ticker task."""
    tag = f"[W{worker_id}]"
    client = polygon_utils.get_polygon_client()
    while True:
        task = task_queue.get()
        if task is None:
            break
        ticker, date_start, date_end = task
        try:
            data = polygon_utils.get_ticker_data_from_polygon(
                client, ticker, "day", date_start, date_end
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


def download_all_daily_bars(tickers, num_threads=NUM_THREADS):
    """
    Multi-threaded download of daily bars for all tickers, last YEARS_BACK years + today.
    Returns dict: ticker -> list of bar rows.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    date_start = (
        datetime.now(timezone.utc) - timedelta(days=YEARS_BACK * 365)
    ).strftime("%Y-%m-%d")

    task_queue = Queue()
    result_dict = {}
    lock = threading.Lock()
    progress = {"done": 0, "total": len(tickers), "t0": time.time()}

    for ticker in tickers:
        task_queue.put((ticker, date_start, today))

    for _ in range(num_threads):
        task_queue.put(None)

    threads = []
    for i in range(num_threads):
        t = threading.Thread(
            target=_download_worker, args=(i, task_queue, result_dict, lock, progress)
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    return result_dict


def count_consecutive_green_days(bars):
    """
    Count consecutive green days (close > open) going backward from the most recent bar.
    Returns (count, daily_changes) where daily_changes is a list of
    (close-open)/open * 100 percentages from latest to earliest green day.
    """
    if not bars:
        return 0, []

    sorted_bars = sorted(bars, key=lambda b: b[8])

    count = 0
    daily_changes = []
    for bar in reversed(sorted_bars):
        # bar: [ticker, datetime_utc, open, high, low, close, volume, vwap, timestamp, transactions, otc]
        bar_open = bar[2]
        bar_close = bar[5]
        if bar_close > bar_open:
            count += 1
            pct = (bar_close - bar_open) / bar_open * 100 if bar_open != 0 else 0
            daily_changes.append(pct)
        else:
            break
    return count, daily_changes


def run_day_momentum_scanner():
    logging_utils.setup_logging(
        log_level="INFO", include_time=True, log_folder="./logs/"
    )

    tickers = ALL_TICKERS
    logging.info(
        f"Downloading daily bars for {len(tickers)} tickers ({YEARS_BACK} years + today)..."
    )

    t0 = time.time()
    all_bars = download_all_daily_bars(tickers)
    elapsed = time.time() - t0
    logging.info(
        f"Download complete in {elapsed:.1f}s. Got data for {len(all_bars)} tickers."
    )

    # Score each ticker by consecutive green days from today backward
    scores = []
    for ticker in tickers:
        bars = all_bars.get(ticker, [])
        green_days, daily_changes = count_consecutive_green_days(bars)
        scores.append((ticker, green_days, daily_changes))

    # Sort descending by green days, skip 0
    scores = [(t, g, c) for t, g, c in scores if g > 0]
    scores.sort(key=lambda x: x[1], reverse=True)

    # Print ticker scores with daily change percentages
    header = (
        f"{'Rank':>5}  {'Ticker':<8}  {'Days':>5}  Daily Changes (latest -> earliest)"
    )
    sep = "-" * 80
    print("\n=== Day Momentum Scanner: Consecutive Green Days (0 excluded) ===")
    print(header)
    print(sep)
    for rank, (ticker, green_days, daily_changes) in enumerate(scores, 1):
        changes_str = ", ".join(f"{c:.2f}%" for c in daily_changes)
        tv_link = f"https://www.tradingview.com/chart/?symbol={ticker}&interval=D"
        print(f"{rank:>5}  {ticker:<8}  {green_days:>5}  {changes_str}  {tv_link}")

    # Save to file
    output_path = "day_momentum_scores.txt"
    with open(output_path, "w") as f:
        f.write(
            f"Day Momentum Scanner - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n"
        )
        f.write("=== Consecutive Green Days (0 excluded) ===\n")
        f.write(header + "\n")
        f.write(sep + "\n")
        for rank, (ticker, green_days, daily_changes) in enumerate(scores, 1):
            changes_str = ", ".join(f"{c:.2f}%" for c in daily_changes)
            tv_link = f"https://www.tradingview.com/chart/?symbol={ticker}&interval=D"
            f.write(f"{rank:>5}  {ticker:<8}  {green_days:>5}  {changes_str}  {tv_link}\n")

    logging.info(f"Scores saved to {output_path}")


if __name__ == "__main__":
    run_day_momentum_scanner()
