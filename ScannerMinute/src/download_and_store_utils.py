import logging
import os
import pickle
import threading
import time
from datetime import datetime, timedelta
from queue import Queue

from ScannerMinute.src.polygon_utils import (
    get_polygon_client,
    get_trading_days,
    get_ticker_data_from_polygon,
)
from ScannerMinute.src.memory_utils import (
    ProgressTracker,
    build_ticker_task_counts,
    check_ticker_done,
)
from ScannerMinute.definitions import SEPARATOR


def generate_daily_tasks(
    tickers: list[str] | str,
    time_span: list[str] | str,
    start_date: str,
    end_date: str,
    client=None,
) -> list[tuple]:
    """
    Generate (ticker, time_span, start_date, end_date, task_idx, est_samples) task tuples
    for every ticker x time_span x trading day in the given range.

    tickers can be a single string (e.g. "AAPL") or a list of strings.
    time_span can be a single string (e.g. "minute") or a list (e.g. ["minute", "second"]).

    est_samples is the estimated number of bars per day:
      - "minute": 16 * 60 = 960
      - "second": 16 * 60 * 60 = 57600

    Non-trading days (weekends, holidays) are filtered out using AAPL daily bars
    from Polygon as the reference calendar.

    Input format for dates: "YYYY-MM-DD"
    """
    if isinstance(tickers, str):
        tickers = [tickers]
    if isinstance(time_span, str):
        time_span = [time_span]

    samples_per_day = {
        "minute": 16 * 60,
        "second": 16 * 60 * 60,
    }

    if client is None:
        client = get_polygon_client()
    trading_days = set(get_trading_days(client, from_=start_date, to=end_date))

    tasks = []
    task_idx = 0
    for ticker in tickers:
        for ts in time_span:
            est_samples = samples_per_day.get(ts, 0)
            current = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            while current <= end:
                day_str = current.strftime("%Y-%m-%d")
                if day_str in trading_days:
                    tasks.append((ticker, ts, day_str, day_str, task_idx, est_samples))
                    task_idx += 1
                current += timedelta(days=1)

    logging.info(f"Generated {len(tasks)} daily tasks for {len(tickers)} tickers, {len(time_span)} timespans")
    return tasks


MAX_SAMPLES_PER_REQUEST = 50000


def merge_consecutive_tasks(tasks, max_samples=MAX_SAMPLES_PER_REQUEST):
    """
    Merge consecutive-day tasks for the same (ticker, time_span) into fewer tasks,
    as long as the total estimated samples stay within max_samples.

    Input tasks:  (ticker, time_span, start_date, end_date, task_idx, est_samples)
    Output tasks: same format, with merged date ranges and updated est_samples/task_idx.
    """
    from collections import defaultdict

    # Group tasks by (ticker, time_span), preserving order within each group
    groups = defaultdict(list)
    for task in tasks:
        ticker, ts, sd, ed, idx, est = task
        groups[(ticker, ts)].append((sd, ed, est))

    merged = []
    task_idx = 0
    for (ticker, ts), day_tasks in groups.items():
        # Sort by start_date
        day_tasks.sort(key=lambda x: x[0])

        # est_samples per day (same for all tasks in this group)
        est_per_day = day_tasks[0][2]

        chunk_start = day_tasks[0][0]
        chunk_end = day_tasks[0][1]
        chunk_samples = est_per_day

        for i in range(1, len(day_tasks)):
            sd, ed, est = day_tasks[i]
            if chunk_samples + est_per_day <= max_samples:
                # Extend the current chunk
                chunk_end = ed
                chunk_samples += est_per_day
            else:
                # Flush current chunk, start new one
                merged.append((ticker, ts, chunk_start, chunk_end, task_idx, chunk_samples))
                task_idx += 1
                chunk_start = sd
                chunk_end = ed
                chunk_samples = est_per_day

        # Flush last chunk
        merged.append((ticker, ts, chunk_start, chunk_end, task_idx, chunk_samples))
        task_idx += 1

    logging.info(f"Merged {len(tasks)} daily tasks into {len(merged)} tasks (max {max_samples} samples/request)")
    return merged


def _download_worker(worker_id, task_queue, result_queue, progress, ticker_task_counts, ticker_task_lock):
    """Worker thread: creates its own Polygon client, pulls tasks, pushes results."""
    tag = f"[W{worker_id}]"
    logging.info(f"{tag} Starting download worker")
    client = get_polygon_client()
    while True:
        task = task_queue.get()
        if task is None:
            break
        ticker, timespan, start_date, end_date, idx_task, est_samples = task
        try:
            data = get_ticker_data_from_polygon(client, ticker, timespan, start_date, end_date)
            result_queue.put((ticker, timespan, data, idx_task))
            with ticker_task_lock:
                ticker_done = check_ticker_done(ticker_task_counts, ticker)
            logging.info(f"{tag} Downloaded {len(data)} bars for {ticker} ({start_date} to {end_date})")
            progress.tick(tag, ticker, ticker_done)
        except Exception as e:
            logging.error(f"{tag} Error downloading {ticker}: {e}")
            with ticker_task_lock:
                ticker_done = check_ticker_done(ticker_task_counts, ticker)
            progress.tick(tag, ticker, ticker_done, failed=True)
    logging.info(f"{tag} Worker finished")


def _writer_worker(db_path, result_queue):
    """Writer thread: pulls from result queue, batch-writes to RocksDB."""
    from rocksdict import Rdict, WriteBatch

    db = Rdict(db_path)
    while True:
        item = result_queue.get()
        if item is None:
            break
        ticker, timespan, data, idx_task = item
        if not data:
            continue
        wb = WriteBatch()
        for bar in data:
            timestamp_ms = bar[8]
            iso_dt = datetime.utcfromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%dT%H:%M:%S")
            key = f"{timespan}{SEPARATOR}{ticker}{SEPARATOR}{iso_dt}"
            wb[key] = pickle.dumps(bar)
        db.write(wb)
        logging.info(f"[WRITER] Wrote {len(data)} bars for {ticker} to RocksDB (Task {idx_task})")
    db.close()


def get_latest_time_per_ticker(tickers, db_path, timespan="minute", num_threads=4):
    """
    For each ticker, find the latest timestamp stored in RocksDB.

    Each thread opens its own read-only DB handle and processes a chunk of tickers.
    Returns a dict of {ticker: latest_iso_timestamp} (tickers with no data are omitted).
    Also logs the elapsed time.
    """
    from rocksdict import Rdict, AccessType

    t0 = time.time()
    results = {}
    lock = threading.Lock()

    def _worker(ticker_chunk):
        db = Rdict(db_path, access_type=AccessType.read_only())
        local_results = {}
        for ticker in ticker_chunk:
            prefix = f"{timespan}{SEPARATOR}{ticker}{SEPARATOR}"
            it = db.iter()
            it.seek(prefix + "\x7f")
            it.prev()
            if it.valid():
                key = it.key()
                if key.startswith(prefix):
                    local_results[ticker] = key[len(prefix):]
        db.close()
        with lock:
            results.update(local_results)

    # Split tickers into chunks, one per thread
    num_threads = min(num_threads, len(tickers))
    chunk_size = (len(tickers) + num_threads - 1) // num_threads
    chunks = [tickers[i:i + chunk_size] for i in range(0, len(tickers), chunk_size)]

    threads = []
    for chunk in chunks:
        t = threading.Thread(target=_worker, args=(chunk,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    elapsed = time.time() - t0
    logging.info(
        f"[LATEST] Got latest time for {len(results)}/{len(tickers)} tickers "
        f"in {elapsed:.2f}s ({num_threads} threads)"
    )
    return results


def update_download_and_store(
    tickers: list[str],
    date_end: str,
    default_start_date: str,
    timespan: str = "minute",
    db_path: str = None,
    num_threads: int = 4,
    merge: bool = True,
):
    """
    Incremental update: for each ticker, download data from its latest timestamp
    in the DB up to date_end, then store into RocksDB.

    Tickers not yet in the DB are downloaded from default_start_date.
    Re-downloading a partial day overlap is safe — RocksDB upserts duplicate keys.

    Parameters:
        tickers: list of tickers to update
        date_end: end date "YYYY-MM-DD" (may be mid-day — all available data up to this date is fetched)
        default_start_date: start date "YYYY-MM-DD" for tickers not yet in the DB
        timespan: "minute" or "second"
        db_path: path to RocksDB directory
        num_threads: number of download worker threads
        merge: if True, merge consecutive-day tasks to reduce API calls
    """
    from ScannerMinute.definitions import PROJECT_ROOT_DIR

    if db_path is None:
        db_path = f"{PROJECT_ROOT_DIR}/data/rocksdict/"
    os.makedirs(db_path, exist_ok=True)

    # Get the latest timestamp per ticker from the DB
    latest = get_latest_time_per_ticker(tickers, db_path, timespan=timespan, num_threads=num_threads)

    # Determine per-ticker start date
    # For tickers in DB: start from the date of their latest sample (re-downloads that partial day)
    # For tickers not in DB: start from default_start_date
    new_tickers = []
    existing_tickers = []
    for ticker in tickers:
        if ticker in latest:
            # Extract date portion from ISO timestamp "YYYY-MM-DDTHH:MM:SS"
            start_date = latest[ticker][:10]
            existing_tickers.append((ticker, start_date))
        else:
            new_tickers.append(ticker)

    logging.info(
        f"[UPDATE] {len(tickers)} tickers: "
        f"{len(existing_tickers)} existing (updating from latest), "
        f"{len(new_tickers)} new (from {default_start_date})"
    )

    # Generate tasks: new tickers all share the same start date
    client = get_polygon_client()
    all_tasks = []

    if new_tickers:
        new_tasks = generate_daily_tasks(
            new_tickers, timespan, default_start_date, date_end, client=client,
        )
        all_tasks.extend(new_tasks)

    # Existing tickers may each have a different start date — group by start date
    # to reduce generate_daily_tasks calls
    from collections import defaultdict
    by_start = defaultdict(list)
    for ticker, start_date in existing_tickers:
        by_start[start_date].append(ticker)

    for start_date, group_tickers in by_start.items():
        if start_date >= date_end:
            continue
        tasks = generate_daily_tasks(
            group_tickers, timespan, start_date, date_end, client=client,
        )
        all_tasks.extend(tasks)

    # Re-index task_idx
    all_tasks = [
        (t[0], t[1], t[2], t[3], idx, t[5])
        for idx, t in enumerate(all_tasks)
    ]

    if not all_tasks:
        logging.info("[UPDATE] No tasks generated — everything is up to date.")
        return

    if merge:
        original_count = len(all_tasks)
        all_tasks = merge_consecutive_tasks(all_tasks)
        logging.info(f"[UPDATE] Merged {original_count} -> {len(all_tasks)} tasks")

    logging.info(f"[UPDATE] Starting download of {len(all_tasks)} tasks for {len(tickers)} tickers")

    # Reuse the same worker pipeline as download_and_store
    actual_threads = min(num_threads, len(all_tasks))
    t0 = time.time()

    task_queue = Queue()
    result_queue = Queue()

    for task in all_tasks:
        task_queue.put(task)
    for _ in range(actual_threads):
        task_queue.put(None)

    ticker_task_counts = build_ticker_task_counts(all_tasks)
    total_tickers = len(set(task[0] for task in all_tasks))
    progress = ProgressTracker(len(all_tasks), total_tickers)
    ticker_task_lock = threading.Lock()

    download_threads = []
    for i in range(actual_threads):
        t = threading.Thread(
            target=_download_worker,
            args=(i, task_queue, result_queue, progress, ticker_task_counts, ticker_task_lock),
        )
        t.start()
        download_threads.append(t)

    writer_thread = threading.Thread(target=_writer_worker, args=(db_path, result_queue))
    writer_thread.start()

    for t in download_threads:
        t.join()
    t_download = time.time() - t0

    result_queue.put(None)
    writer_thread.join()
    t_total = time.time() - t0

    logging.info(
        f"[UPDATE] Download: {t_download:.1f}s | Total: {t_total:.1f}s | "
        f"Tasks: {len(all_tasks)} | Tickers: {total_tickers}"
    )


def download_and_store(
    tickers: list[str] | str,
    date_start: str,
    date_end: str,
    timespan: list[str] | str = "minute",
    db_path: str = None,
    num_threads: int = 4,
    merge: bool = True,
):
    """
    Multi-threaded download from Polygon + single-writer batch insert to RocksDB.

    Parameters:
        tickers: ticker(s) to download — string or list of strings
        date_start: start date "YYYY-MM-DD"
        date_end: end date "YYYY-MM-DD"
        timespan: "minute", "second", or list like ["minute", "second"]
        db_path: path to RocksDB directory (defaults to PROJECT_ROOT_DIR/data_rocksdict/)
        num_threads: number of download worker threads
        merge: if True, merge consecutive-day tasks to reduce API calls (up to 50k samples)
    """
    from ScannerMinute.definitions import PROJECT_ROOT_DIR

    if db_path is None:
        db_path = f"{PROJECT_ROOT_DIR}/data/rocksdict/"
    os.makedirs(db_path, exist_ok=True)

    # Generate tasks (filters non-trading days)
    tasks = generate_daily_tasks(tickers, timespan, date_start, date_end)
    if not tasks:
        logging.info("No tasks generated — nothing to download.")
        return

    if merge:
        original_count = len(tasks)
        original_samples = sum(t[5] for t in tasks)
        tasks = merge_consecutive_tasks(tasks)
        merged_count = len(tasks)
        merged_samples = sum(t[5] for t in tasks)
        reduction = original_count - merged_count
        reduction_pct = (reduction / original_count * 100) if original_count else 0
        avg_days_per_task = original_count / merged_count if merged_count else 0
        logging.info(
            f"[MERGE] Before: {original_count} tasks | After: {merged_count} tasks | "
            f"Reduced by: {reduction} tasks ({reduction_pct:.1f}%) | "
            f"Avg days/task: {avg_days_per_task:.1f} | "
            f"Est samples: {original_samples:,} -> {merged_samples:,}"
        )

    num_threads = min(num_threads, len(tasks))
    t0 = time.time()

    task_queue = Queue()
    result_queue = Queue()

    for task in tasks:
        task_queue.put(task)
    for _ in range(num_threads):
        task_queue.put(None)

    # Progress tracking
    ticker_task_counts = build_ticker_task_counts(tasks)
    total_tickers = len(set(task[0] for task in tasks))
    progress = ProgressTracker(len(tasks), total_tickers)
    ticker_task_lock = threading.Lock()

    # Start download workers
    download_threads = []
    for i in range(num_threads):
        t = threading.Thread(
            target=_download_worker,
            args=(i, task_queue, result_queue, progress, ticker_task_counts, ticker_task_lock),
        )
        t.start()
        download_threads.append(t)

    # Start single writer thread
    writer_thread = threading.Thread(target=_writer_worker, args=(db_path, result_queue))
    writer_thread.start()

    # Wait for all downloaders to finish
    for t in download_threads:
        t.join()
    t_download = time.time() - t0

    # Signal writer to stop, then wait
    result_queue.put(None)
    writer_thread.join()
    t_total = time.time() - t0

    logging.info(
        f"Download: {t_download:.1f}s | Total: {t_total:.1f}s | "
        f"Tasks: {len(tasks)} | Tickers: {total_tickers}"
    )
