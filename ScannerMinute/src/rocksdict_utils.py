import logging
import os
import pickle
import threading
import time
from queue import Queue
from datetime import datetime

from rocksdict import Rdict, WriteBatch, AccessType

from ScannerMinute.src import polygon_utils
from ScannerMinute.definitions import PROJECT_ROOT_DIR, SEPARATOR

DEFAULT_DB_PATH = f"{PROJECT_ROOT_DIR}/data_rocksdict/"


def init_db(db_path=DEFAULT_DB_PATH):
    """Create directory if needed and return an open Rdict instance."""
    os.makedirs(db_path, exist_ok=True)
    return Rdict(db_path)


def _datetime_utc_to_iso8601(datetime_utc_str):
    """Convert YYYYMMDD_HHMMSS to ISO 8601 format."""
    dt = datetime.strptime(datetime_utc_str, "%Y%m%d_%H%M%S")
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _download_worker(worker_id, task_queue, result_queue):
    """Worker thread: creates its own Polygon client, pulls tasks, pushes results."""
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
            result_queue.put((ticker, timespan, data, idx_task))
            logging.info(
                f"{tag} Downloaded {len(data)} bars for {ticker} ({start_date} to {end_date})"
            )
        except Exception as e:
            logging.error(f"{tag} Error downloading {ticker}: {e}")
    logging.info(f"{tag} Worker finished")


def _writer_worker(db_path, result_queue):
    """Writer thread: pulls from result queue, batch-writes to RocksDB."""
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
            datetime_utc = bar[1]  # index 1 is datetime_utc per COLUMNS
            iso_dt = _datetime_utc_to_iso8601(datetime_utc)
            key = f"{timespan}{SEPARATOR}{ticker}{SEPARATOR}{iso_dt}"
            wb[key] = pickle.dumps(bar)
        db.write(wb)
        logging.info(
            f"[WRITER] Wrote {len(data)} bars for {ticker} to RocksDB (Task {idx_task}/{data[0][1]}-{data[-1][1]})"
        )
    db.close()


def download_and_store(db_path, num_threads, tasks):
    """
    Multi-threaded download from Polygon + single-writer batch insert to RocksDB.

    Parameters:
        db_path: str — path to RocksDB directory
        num_threads: int — number of download worker threads
        tasks: list of tuples (ticker, start_date, end_date, timespan)
    """
    os.makedirs(db_path, exist_ok=True)
    t0 = time.time()

    task_queue = Queue()
    result_queue = Queue()

    # Fill task queue
    for task in tasks:
        task_queue.put(task)

    # Add sentinel for each download worker
    for _ in range(num_threads):
        task_queue.put(None)

    # Start download workers
    download_threads = []
    for i in range(num_threads):
        t = threading.Thread(
            target=_download_worker, args=(i, task_queue, result_queue)
        )
        t.start()
        download_threads.append(t)

    # Start single writer thread
    writer_thread = threading.Thread(
        target=_writer_worker, args=(db_path, result_queue)
    )
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
        f"Download: {t_download:.1f}s, Total (download+write): {t_total:.1f}s, Tasks: {len(tasks)}"
    )


def get_first_and_last_time(db_path, timespan, ticker):
    """
    Return the first and last timestamps (ISO 8601 strings) stored in RocksDB
    for a given (timespan, ticker) pair.

    Parameters:
        db_path: str — path to RocksDB directory
        timespan: str — e.g. "minute"
        ticker: str — e.g. "AAPL"

    Returns:
        (str, str) or (None, None) — e.g. ("2024-01-02T09:30:00", "2024-06-15T15:59:00")
    """
    db = Rdict(db_path, access_type=AccessType.read_only())
    prefix = f"{timespan}{SEPARATOR}{ticker}{SEPARATOR}"

    # First time: seek to the prefix start
    first_time = None
    it = db.iter()
    it.seek(prefix)
    if it.valid():
        key = it.key()
        if key.startswith(prefix):
            first_time = key[len(prefix) :]

    # Last time: seek past the end of all keys with this prefix
    last_time = None
    it.seek(prefix + "\x7f")
    it.prev()
    if it.valid():
        key = it.key()
        if key.startswith(prefix):
            last_time = key[len(prefix) :]

    db.close()
    return first_time, last_time


def read_bars(db_path, timespan, tickers, start_time, end_time):
    """
    Read bars from RocksDB via prefix+range scan.

    Parameters:
        db_path: str — path to RocksDB directory
        timespan: str — e.g. "minute"
        tickers: list of str
        start_time: str — ISO 8601, e.g. "2024-01-01T00:00:00"
        end_time: str — ISO 8601, e.g. "2024-02-01T23:59:59"

    Returns:
        list of lists matching polygon_utils.COLUMNS schema
    """
    db = Rdict(db_path, access_type=AccessType.read_only())
    results = []

    for ticker in tickers:
        prefix = f"{timespan}{SEPARATOR}{ticker}{SEPARATOR}"
        seek_key = prefix + start_time
        end_key = prefix + end_time

        it = db.iter()
        it.seek(seek_key)
        while it.valid():
            key = it.key()
            if not key.startswith(prefix) or key > end_key:
                break
            results.append(pickle.loads(it.value()))
            it.next()

    db.close()
    return results
