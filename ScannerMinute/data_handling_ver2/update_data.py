import logging
import os
from datetime import datetime, timedelta

from ScannerMinute.src import logging_utils
from ScannerMinute.src.polygon_utils import (
    get_polygon_client,
    get_all_tickers_from_snapshot,
)
from ScannerMinute.src.download_and_store_utils import update_download_and_store


DB_PATH = "./data/ver2/"


def update_data(
    tickers=None,
    db_path=DB_PATH,
    num_threads=30,
    timespan="minute",
    default_lookback_days=365,
):
    logging_utils.setup_logging(
        log_level="INFO",
        log_folder=f"{os.path.dirname(os.path.abspath(__file__))}/logs/",
        include_time=True,
    )

    date_end = datetime.now().strftime("%Y-%m-%d")
    default_start_date = (datetime.now() - timedelta(days=default_lookback_days)).strftime("%Y-%m-%d")

    # Resolve tickers
    client = get_polygon_client()
    if tickers is None:
        tickers = get_all_tickers_from_snapshot(client)
    elif isinstance(tickers, int):
        all_tickers = sorted(get_all_tickers_from_snapshot(client))
        tickers = all_tickers[:tickers]

    logging.info(
        f"update_data: {len(tickers)} tickers, "
        f"date_end: {date_end}, default_start: {default_start_date}, "
        f"timespan: {timespan}, threads: {num_threads}, db: {db_path}"
    )

    update_download_and_store(
        tickers=tickers,
        date_end=date_end,
        default_start_date=default_start_date,
        timespan=timespan,
        db_path=db_path,
        num_threads=num_threads,
    )


if __name__ == "__main__":
    update_data()
