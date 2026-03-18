import logging
import threading
import time
from queue import Queue
from datetime import datetime, timezone, timedelta

from ScannerMinute.src import logging_utils, polygon_utils
from ScannerMinute.src.ticker_utils import ALL_TICKERS


NUM_THREADS = 30
YEARS_BACK = 5


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
                    result_dict[ticker].append({
                        "date": date_str,
                        "open": agg.open,
                        "high": agg.high,
                        "low": agg.low,
                        "close": agg.close,
                        "volume": agg.volume,
                        "timestamp": agg.timestamp,
                    })
                progress["done"] += 1
                done = progress["done"]
                total = progress["total"]
                elapsed = time.time() - progress["t0"]
                rate = done / elapsed if elapsed > 0 else 0
                remaining = (total - done) / rate if rate > 0 else 0
                n_tickers = len([t for t in aggs if not tickers_set or t.ticker in tickers_set])
                logging.info(
                    f"{tag} {date_str}: {n_tickers} tickers | "
                    f"{done}/{total} ({done*100/total:.1f}%) | "
                    f"elapsed={elapsed:.1f}s | ETA={remaining:.1f}s"
                )
        except Exception as e:
            with lock:
                progress["done"] += 1
            logging.error(f"{tag} Error downloading {date_str}: {e}")


def get_trading_days(years_back=YEARS_BACK):
    """Get list of trading days using Polygon, from years_back ago to today."""
    client = polygon_utils.get_polygon_client()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    date_start = (
        datetime.now(timezone.utc) - timedelta(days=years_back * 365)
    ).strftime("%Y-%m-%d")
    trading_days = polygon_utils.get_trading_days(client, from_=date_start, to=today)
    logging.info(f"Found {len(trading_days)} trading days from {date_start} to {today}")
    return trading_days


def download_all_daily_bars(tickers, num_threads=NUM_THREADS):
    """
    Multi-threaded download of grouped daily bars for all trading days.
    Uses get_grouped_daily_aggs (regular session only) — one API call per day.
    Returns dict: ticker -> list of bar dicts.
    """
    trading_days = get_trading_days()
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


def count_consecutive_green_days(bars):
    """
    Count consecutive green days going backward from the most recent bar.
    A green day = current close > previous trading day's close.
    Returns (count, daily_changes) where daily_changes is a list of
    (close / prev_close - 1) * 100 percentages from latest to earliest green day.
    """
    if not bars or len(bars) < 2:
        return 0, []

    sorted_bars = sorted(bars, key=lambda b: b["timestamp"])

    count = 0
    daily_changes = []
    # Walk backward from the last bar, comparing to the previous bar's close
    for i in range(len(sorted_bars) - 1, 0, -1):
        cur_close = sorted_bars[i]["close"]
        prev_close = sorted_bars[i - 1]["close"]
        if prev_close and cur_close > prev_close:
            count += 1
            pct = (cur_close / prev_close - 1) * 100
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
        f"Downloading grouped daily bars for {len(tickers)} tickers ({YEARS_BACK} years + today)..."
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
    print("Percentages are close-to-close: (today's close / prev day's close - 1) * 100")
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
        f.write("Percentages are close-to-close: (today's close / prev day's close - 1) * 100\n")
        f.write(header + "\n")
        f.write(sep + "\n")
        for rank, (ticker, green_days, daily_changes) in enumerate(scores, 1):
            changes_str = ", ".join(f"{c:.2f}%" for c in daily_changes)
            tv_link = f"https://www.tradingview.com/chart/?symbol={ticker}&interval=D"
            f.write(f"{rank:>5}  {ticker:<8}  {green_days:>5}  {changes_str}  {tv_link}\n")

    logging.info(f"Scores saved to {output_path}")


if __name__ == "__main__":
    run_day_momentum_scanner()
