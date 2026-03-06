import logging
import os
import time
from datetime import datetime, timedelta
from ScannerMinute.src import rocksdict_utils
from ScannerMinute.src import polygon_utils
from ScannerMinute.src import logging_utils
from ScannerMinute.src.ticker_utils import ALL_TICKERS


def download_and_store(
    tickers: list[str] = ALL_TICKERS,
    date_start: str = "2020-01-01",
    date_end: str = datetime.now().strftime("%Y-%m-%d"),
    num_threads: int = 10,
):
    db_path = rocksdict_utils.DEFAULT_DB_PATH

    db = rocksdict_utils.init_db(db_path)
    db.close()

    tasks = polygon_utils.generate_tasks(tickers, date_start, date_end, db_path=db_path)

    if not tasks:
        logging.info("No tasks to download — database is up to date.")
        return

    t0 = time.time()
    rocksdict_utils.download_and_store(db_path, num_threads=num_threads, tasks=tasks)
    t_download = time.time() - t0
    logging.info(f"Download + store: {t_download:.1f}s")


def download_all_tickers():
    t0 = time.time()
    logging_utils.setup_logging(
        log_level="INFO",
        log_folder=f"{os.path.dirname(os.path.abspath(__file__))}/logs/",
        include_time=True,
    )

    date_start = (datetime.now() - timedelta(days=5 * 365 - 1)).strftime("%Y-%m-%d")
    today = datetime.now().strftime("%Y-%m-%d")
    tickers = ALL_TICKERS
    num_threads = 30

    logging.info(
        f"Using {num_threads} threads, {len(tickers)} tickers, "
        f"date range: {date_start} to {today}"
    )
    download_and_store(tickers, date_start, today, num_threads)

    t1 = time.time()
    logging.info(f"Total script time: {t1 - t0:.1f}s")


if __name__ == "__main__":
    download_all_tickers()
