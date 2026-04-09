import logging
import os
from datetime import datetime, timedelta

from ScannerMinute.src import logging_utils
from ScannerMinute.src.polygon_utils import (
    get_polygon_client,
    get_all_tickers_from_snapshot,
)
from ScannerMinute.src.download_and_store_utils import download_and_store, get_latest_time_per_ticker


DB_PATH = "./data/ver2/"


def download_data(
    date_start=None,
    date_end=None,
    tickers=None,
    db_path=DB_PATH,
    num_threads=30,
    timespan="minute",
):
    logging_utils.setup_logging(
        log_level="INFO",
        log_folder=f"{os.path.dirname(os.path.abspath(__file__))}/logs/",
        include_time=True,
    )

    # Default date range: last year
    if date_end is None:
        date_end = datetime.now().strftime("%Y-%m-%d")
    if date_start is None:
        date_start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

    # Resolve tickers
    client = get_polygon_client()
    if tickers is None:
        tickers = get_all_tickers_from_snapshot(client)
    elif isinstance(tickers, int):
        all_tickers = sorted(get_all_tickers_from_snapshot(client))
        tickers = all_tickers[:tickers]

    # Check latest global time in DB and adjust date_start if needed
    if not os.path.exists(db_path):
        latest_per_ticker = {}
    else:
        latest_per_ticker = get_latest_time_per_ticker(tickers, db_path, timespan=timespan, num_threads=num_threads)
    if latest_per_ticker:
        global_latest = max(latest_per_ticker.values())
        global_latest_date = global_latest[:10]
        if global_latest_date > date_start:
            logging.info(
                f"[DOWNLOAD] DB already has data up to {global_latest}, "
                f"adjusting date_start from {date_start} to {global_latest_date}"
            )
            date_start = global_latest_date

    if date_start > date_end:
        logging.info(f"[DOWNLOAD] Nothing to download: date_start={date_start} > date_end={date_end}")
        return

    logging.info(
        f"download_data: {len(tickers)} tickers, "
        f"range: {date_start} to {date_end}, "
        f"timespan: {timespan}, threads: {num_threads}, db: {db_path}"
    )

    download_and_store(
        tickers=tickers,
        date_start=date_start,
        date_end=date_end,
        timespan=timespan,
        db_path=db_path,
        num_threads=num_threads,
    )


if __name__ == "__main__":
    download_data()
