import logging
import os
from datetime import datetime, timedelta

from ScannerMinute.src import logging_utils
from ScannerMinute.src.polygon_utils import (
    get_polygon_client,
    get_all_tickers_from_snapshot,
)
from ScannerMinute.src.download_and_store_utils import download_and_store


DB_PATH = "./data/ver2/"


def download_data(num_threads=30):
    logging_utils.setup_logging(
        log_level="INFO",
        log_folder=f"{os.path.dirname(os.path.abspath(__file__))}/logs/",
        include_time=True,
    )

    client = get_polygon_client()
    tickers = get_all_tickers_from_snapshot(client)

    date_start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    date_end = datetime.now().strftime("%Y-%m-%d")

    logging.info(
        f"download_data: {len(tickers)} tickers, "
        f"range: {date_start} to {date_end}, "
        f"timespan: minute, threads: {num_threads}, db: {DB_PATH}"
    )

    download_and_store(
        tickers=tickers,
        date_start=date_start,
        date_end=date_end,
        timespan="minute",
        db_path=DB_PATH,
        num_threads=num_threads,
    )


if __name__ == "__main__":
    download_data()

