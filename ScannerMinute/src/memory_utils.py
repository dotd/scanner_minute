import logging
import threading
import time
from datetime import datetime, timezone
from queue import Queue

from ScannerMinute.src import polygon_utils


class ProgressTracker:
    """Thread-safe progress tracker for download tasks."""

    def __init__(self, total_tasks, total_tickers):
        self._lock = threading.Lock()
        self._total_tasks = total_tasks
        self._total_tickers = total_tickers
        self._done_tasks = 0
        self._completed_tickers = set()
        self._start_time = time.time()

    def tick(self, tag, ticker, ticker_done):
        with self._lock:
            self._done_tasks += 1
            if ticker_done:
                self._completed_tickers.add(ticker)
            elapsed = time.time() - self._start_time
            avg_per_task = elapsed / self._done_tasks
            remaining_tasks = self._total_tasks - self._done_tasks
            remaining_tickers = self._total_tickers - len(self._completed_tickers)
            eta = avg_per_task * remaining_tasks
            logging.info(
                f"{tag} Tasks: {self._done_tasks}/{self._total_tasks} | "
                f"Tickers: {len(self._completed_tickers)}/{self._total_tickers} | "
                f"avg {avg_per_task:.2f}s/task | "
                f"remaining: {remaining_tasks} tasks, {remaining_tickers} tickers | "
                f"ETA {eta:.1f}s | last: {ticker}"
            )


def build_ticker_task_counts(tasks):
    """Build a dict mapping '_total_{ticker}' → count of tasks for that ticker."""
    counts = {}
    for task in tasks:
        ticker = task[0]
        counts[f"_total_{ticker}"] = counts.get(f"_total_{ticker}", 0) + 1
    return counts


def check_ticker_done(ticker_task_counts, ticker):
    """Increment ticker's done count and return whether all its tasks are complete."""
    ticker_task_counts[ticker] = ticker_task_counts.get(ticker, 0) + 1
    return ticker_task_counts[ticker] == ticker_task_counts.get(f"_total_{ticker}", 0)


def _download_worker(
    worker_id, task_queue, result_dict, result_lock, progress, ticker_task_counts
):
    """Worker thread: downloads tasks and stores results in a shared dict."""
    tag = f"[W{worker_id}]"
    logging.info(f"{tag} Starting download worker")
    client = polygon_utils.get_polygon_client()
    while True:
        task = task_queue.get()
        if task is None:
            break
        ticker, start_date, end_date, timespan, idx_task = task
        try:
            data = polygon_utils.get_ticker_data_from_polygon(
                client, ticker, timespan, start_date, end_date
            )
            with result_lock:
                if ticker not in result_dict:
                    result_dict[ticker] = []
                result_dict[ticker].extend(data)
                ticker_done = check_ticker_done(ticker_task_counts, ticker)
            logging.info(
                f"{tag} Downloaded {len(data)} bars for {ticker} ({start_date} to {end_date})"
            )
            progress.tick(tag, ticker, ticker_done)
        except Exception as e:
            logging.error(f"{tag} Error downloading {ticker}: {e}")
            with result_lock:
                ticker_done = check_ticker_done(ticker_task_counts, ticker)
            progress.tick(tag, ticker, ticker_done)
    logging.info(f"{tag} Worker finished")


def download_tickers_multithread(
    tickers: list[str],
    date_start: str,
    date_end: str = None,
    num_threads: int = 4,
    timespan: str = "minute",
) -> dict[str, list[list]]:
    """
    Download ticker data from Polygon using multiple threads.

    Parameters:
        tickers: list of ticker symbols
        date_start: start date "YYYY-MM-DD"
        date_end: end date "YYYY-MM-DD", defaults to today
        num_threads: number of download threads
        timespan: e.g. "minute"

    Returns:
        dict mapping ticker → list of bar lists (matching COLUMNS schema)
    """
    if date_end is None:
        date_end = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    tasks = polygon_utils.generate_tasks(tickers, date_start, date_end)
    num_threads = min(num_threads, len(tasks)) if tasks else 0

    ticker_task_counts = build_ticker_task_counts(tasks)

    task_queue = Queue()
    for task in tasks:
        task_queue.put(task)
    for _ in range(num_threads):
        task_queue.put(None)

    result_dict = {}
    result_lock = threading.Lock()
    progress = ProgressTracker(len(tasks), len(tickers))

    threads = []
    for i in range(num_threads):
        t = threading.Thread(
            target=_download_worker,
            args=(
                i,
                task_queue,
                result_dict,
                result_lock,
                progress,
                ticker_task_counts,
            ),
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    logging.info(
        f"Downloaded {sum(len(v) for v in result_dict.values())} total bars "
        f"for {len(result_dict)} tickers"
    )
    return result_dict
