import logging
import os
import time
from datetime import datetime, timedelta
from ScannerMinute.src import rocksdict_utils
from ScannerMinute.src import polygon_utils
from ScannerMinute.src import logging_utils
from ScannerMinute.src.ticker_utils import TICKERS


def download_and_store(
    tickers: list[str] = TICKERS,
    date_start: str = "2024-01-01",
    date_end: str = datetime.now().strftime("%Y-%m-%d"),
    num_threads: int = 10,
):
    db_path = rocksdict_utils.DEFAULT_DB_PATH

    # 1. Init DB
    db = rocksdict_utils.init_db(db_path)
    db.close()

    # 2. Download 50 tickers, monthly chunks, 10 threads
    tasks = polygon_utils.generate_tasks(tickers, date_start, date_end)

    t0 = time.time()
    rocksdict_utils.download_and_store(db_path, num_threads=num_threads, tasks=tasks)
    t_download = time.time() - t0
    logging.info(f"Download + store: {t_download:.1f}s")


def read_bars(
    tickers: list[str] = TICKERS,
    date_start: str = "2024-01-01",
    date_end: str = datetime.now().strftime("%Y-%m-%d"),
    timespan: str = "minute",
    db_path: str = rocksdict_utils.DEFAULT_DB_PATH,
    head_tail_lines: int = 10,
    print_head_tail: bool = False,
):
    # 3. Read back and print
    t0 = time.time()
    for ticker in tickers:
        t_read_start = time.time()
        results = rocksdict_utils.read_bars(
            db_path, timespan, [ticker], date_start, date_end
        )
        t_read = time.time() - t_read_start
        logging.info(f"Read {len(results)} bars in {t_read:.2f}s")
        if print_head_tail:
            for bar in results[:head_tail_lines] + results[-head_tail_lines:]:
                logging.info(bar)
    logging.info(f"Total read time: {time.time() - t0:.1f}s")
    return results


def tst_rocksdict_utils(
    limit_tickers=100000, prior_days=365, date_start=None, tickers=TICKERS
):
    t0 = time.time()
    logging_utils.setup_logging(
        log_level="INFO",
        log_folder=f"{os.path.dirname(os.path.abspath(__file__))}/logs/",
        include_time=True,
    )

    date_start = (
        (datetime.now() - timedelta(days=prior_days)).strftime("%Y-%m-%d")
        if date_start is None
        else date_start
    )
    today = datetime.now().strftime("%Y-%m-%d")
    tickers = tickers[0:limit_tickers]
    num_threads = max(
        1, min(10, len(tickers) // 2)
    )  # number of threads is at least 1 and at most 10, and at most half the number of tickers
    logging.info(
        f"Using {num_threads} threads, {len(tickers)} tickers, date range: {date_start} to {today}"
    )
    download_and_store(tickers, date_start, today, num_threads)
    read_bars(tickers, date_start, today)

    t1 = time.time()
    logging.info(f"Total script time: {t1 - t0:.1f}s")


if __name__ == "__main__":
    all_tickers = polygon_utils.get_all_tickers_from_snapshot(
        polygon_utils.get_polygon_client()
    )
    tst_rocksdict_utils(
        limit_tickers=None,
        prior_days=10,
        tickers=all_tickers,
    )
