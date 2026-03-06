import logging
import os
import time
from datetime import datetime, timedelta
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


def download_to_memory(
    tickers: list[str] = TICKERS,
    date_start: str = "2024-01-01",
    date_end: str = None,
    num_threads: int = 10,
):
    t0 = time.time()
    result_dict = polygon_utils.download_tickers_multithread(
        tickers, date_start, date_end=date_end, num_threads=num_threads
    )
    t_download = time.time() - t0
    logging.info(f"Download to memory: {t_download:.1f}s")

    for ticker, bars in result_dict.items():
        logging.info(f"{ticker}: {len(bars)} bars")
        if bars:
            logging.info(f"  first: {bars[0]}")
            logging.info(f"  last:  {bars[-1]}")

    return result_dict


def tst_download_to_memory(limit_tickers, prior_days, date_start=None):
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
    tickers = TICKERS[0:limit_tickers]
    num_threads = max(
        1, min(10, len(tickers) // 2)
    )
    logging.info(
        f"Using {num_threads} threads, {len(tickers)} tickers, date range: {date_start} to {today}"
    )
    result_dict = download_to_memory(tickers, date_start, today, num_threads)

    t1 = time.time()
    logging.info(f"Total script time: {t1 - t0:.1f}s")
    return result_dict


if __name__ == "__main__":
    tst_download_to_memory(limit_tickers=5, prior_days=365, date_start="2026-01-01")
