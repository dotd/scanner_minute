"""Download AAPL minute data for the last 4 days and display as candlestick chart."""
from datetime import datetime, timedelta, timezone

from ScannerMinute.src.logging_utils import setup_logging
from ScannerMinute.data_handling_ver2.download_data import download_data
from ScannerMinute.src.rocksdict_utils import read_bars
from ScannerMinute.src.candle_utils import show_candles

DB_PATH = "./tmp/data_handling_ver2/"


def run():
    setup_logging(log_level="INFO", include_time=True)

    now = datetime.now(timezone.utc)
    date_start = (now - timedelta(days=4)).strftime("%Y-%m-%d")
    date_end = now.strftime("%Y-%m-%d")

    download_data(
        date_start=date_start,
        date_end=date_end,
        tickers=["AAPL"],
        db_path=DB_PATH,
    )

    bars = read_bars(DB_PATH, "minute", ["AAPL"], f"{date_start}T00:00:00", f"{date_end}T23:59:59")
    show_candles({"AAPL": bars})


if __name__ == "__main__":
    run()
