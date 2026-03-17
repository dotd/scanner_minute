import os
import logging
import argparse
import subprocess
import time
import webbrowser

import requests
from datetime import datetime, timedelta

from ScannerMinute.src import logging_utils
from ScannerMinute.src import rocksdict_utils
from ScannerMinute.src.polygon_utils import COLUMNS
from ScannerMinute.definitions import PROJECT_ROOT_DIR

CHART_SERVER_URL = "http://127.0.0.1:3001"

# Column indices from COLUMNS schema
IDX_TICKER = COLUMNS.index("ticker")
IDX_DATETIME = COLUMNS.index("datetime_utc")
IDX_OPEN = COLUMNS.index("open")
IDX_HIGH = COLUMNS.index("high")
IDX_LOW = COLUMNS.index("low")
IDX_CLOSE = COLUMNS.index("close")
IDX_VOLUME = COLUMNS.index("volume")


def get_args():
    parser = argparse.ArgumentParser(
        description="Show downloaded data as candlestick charts"
    )
    parser.add_argument(
        "--tickers",
        type=str,
        nargs="+",
        required=True,
        help="tickers to display (e.g. AAPL MSFT TSLA)",
    )
    parser.add_argument(
        "--date_start",
        type=str,
        default=(datetime.now() - timedelta(days=0)).strftime("%Y-%m-%d"),
        help="start date (default: 7 days ago)",
    )
    parser.add_argument(
        "--date_end",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="end date (default: today)",
    )
    parser.add_argument(
        "--db_path",
        type=str,
        default=rocksdict_utils.DEFAULT_DB_PATH,
        help="path to rocksdict database",
    )
    parser.add_argument(
        "--timespan",
        type=str,
        default="minute",
        help="bar timespan (default: minute)",
    )
    args, _ = parser.parse_known_args()
    return args


def bars_to_candles(bars):
    """Convert raw bar lists (COLUMNS schema) to lightweight-charts format."""
    import calendar

    candles = []
    for bar in bars:
        dt_str = bar[IDX_DATETIME]  # format: YYYYMMDD_HHMMSS
        dt = datetime.strptime(dt_str, "%Y%m%d_%H%M%S")
        candles.append(
            {
                "time": calendar.timegm(dt.timetuple()),
                "open": bar[IDX_OPEN],
                "high": bar[IDX_HIGH],
                "low": bar[IDX_LOW],
                "close": bar[IDX_CLOSE],
                "volume": bar[IDX_VOLUME],
            }
        )
    candles.sort(key=lambda c: c["time"])
    return candles


def start_chart_server():
    """Start the Node.js chart server and open the browser."""
    server_js = os.path.join(PROJECT_ROOT_DIR, "node_server", "chart_server.js")
    proc = subprocess.Popen(
        ["node", server_js],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    time.sleep(1)
    if proc.poll() is not None:
        stderr = proc.stderr.read().decode()
        raise RuntimeError(f"Chart server failed to start: {stderr}")
    logging.info(f"Chart server started at {CHART_SERVER_URL}")
    webbrowser.open(CHART_SERVER_URL)
    return proc


def post_candles(ticker, candles):
    """Post candle data for a ticker to the chart server."""
    try:
        requests.post(
            f"{CHART_SERVER_URL}/candles",
            json={"ticker": ticker, "candles": candles},
            timeout=10,
        )
        logging.info(f"Sent {len(candles)} candles for {ticker}")
    except Exception as e:
        logging.error(f"Failed to post candles for {ticker}: {e}")


def show_downloaded_data(
    tickers,
    date_start,
    date_end,
    db_path=rocksdict_utils.DEFAULT_DB_PATH,
    timespan="minute",
):
    # Convert dates to ISO 8601 for rocksdict range scan
    start_time = f"{date_start}T00:00:00"
    end_time = f"{date_end}T23:59:59"

    server_proc = start_chart_server()

    try:
        for ticker in tickers:
            logging.info(f"Reading {ticker} from {start_time} to {end_time}...")
            bars = rocksdict_utils.read_bars(
                db_path, timespan, [ticker], start_time, end_time
            )
            if not bars:
                logging.warning(f"No data found for {ticker}")
                continue
            candles = bars_to_candles(bars)
            logging.info(
                f"{ticker}: {len(candles)} candles ({candles[0]['time']} to {candles[-1]['time']})"
            )
            post_candles(ticker, candles)

        logging.info("All tickers sent. Press Ctrl+C to stop the server.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping chart server...")
    finally:
        server_proc.terminate()


if __name__ == "__main__":
    logging_utils.setup_logging(
        log_level="INFO",
        log_folder=f"{os.path.dirname(os.path.abspath(__file__))}/logs/",
        include_time=True,
    )
    args = get_args()
    show_downloaded_data(
        tickers=args.tickers,
        date_start=args.date_start,
        date_end=args.date_end,
        db_path=args.db_path,
        timespan=args.timespan,
    )
