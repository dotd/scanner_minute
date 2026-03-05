import logging
import os
import time
from datetime import datetime
from ScannerMinute.src import rocksdict_utils
from ScannerMinute.src import polygon_utils
from ScannerMinute.src import logging_utils


TICKERS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "NVDA",
    "META",
    "TSLA",
    "BRK.B",
    "JPM",
    "V",
    "UNH",
    "XOM",
    "JNJ",
    "WMT",
    "PG",
    "MA",
    "HD",
    "CVX",
    "MRK",
    "ABBV",
    "LLY",
    "PEP",
    "KO",
    "COST",
    "AVGO",
    "MCD",
    "TMO",
    "CSCO",
    "ACN",
    "ABT",
    "DHR",
    "CRM",
    "NEE",
    "LIN",
    "TXN",
    "AMD",
    "PM",
    "CMCSA",
    "NKE",
    "UPS",
    "INTC",
    "HON",
    "ORCL",
    "AMGN",
    "RTX",
    "LOW",
    "QCOM",
    "BA",
    "CAT",
    "GS",
]


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
    today = datetime.now().strftime("%Y-%m-%d")
    tasks = polygon_utils.generate_tasks(tickers, date_start, date_end)

    t0 = time.time()
    rocksdict_utils.download_and_store(db_path, num_threads=num_threads, tasks=tasks)
    t_download = time.time() - t0
    logging.info(f"Download + store: {t_download:.1f}s")


def read_bars(
    tickers: list[str] = TICKERS,
    date_start: str = "2024-01-01",
    date_end: str = datetime.now().strftime("%Y-%m-%d"),
):
    db_path = rocksdict_utils.DEFAULT_DB_PATH
    # 3. Read back and print
    t0 = time.time()
    results = rocksdict_utils.read_bars(
        db_path, "minute", tickers, date_start, date_end
    )
    t_read = time.time() - t0
    logging.info(f"Read {len(results)} bars in {t_read:.2f}s")
    for bar in results:
        logging.info(bar)


if __name__ == "__main__":
    t0 = time.time()
    logging_utils.setup_logging(
        log_level="INFO",
        log_folder=f"{os.path.dirname(os.path.abspath(__file__))}/logs/",
        include_time=True,
    )

    date_start = "2026-03-01"
    today = datetime.now().strftime("%Y-%m-%d")
    tickers = TICKERS[0:5]
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
