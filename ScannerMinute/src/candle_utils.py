"""
Utilities for displaying candlestick charts via the Node.js chart server.

Usage:
    from ScannerMinute.src.candle_utils import show_candles
    from ScannerMinute.src.rocksdict_utils import read_bars

    bars = read_bars(db_path, "minute", ["AAPL"], "2026-04-01T00:00:00", "2026-04-08T23:59:59")
    show_candles({"AAPL": bars})
"""
import calendar
import logging
import os
import subprocess
import time
import webbrowser

import requests
from datetime import datetime

from ScannerMinute.src.polygon_utils import COLUMNS
from ScannerMinute.definitions import PROJECT_ROOT_DIR

CHART_SERVER_URL = "http://127.0.0.1:3001"

IDX_DATETIME = COLUMNS.index("datetime_utc")
IDX_OPEN = COLUMNS.index("open")
IDX_HIGH = COLUMNS.index("high")
IDX_LOW = COLUMNS.index("low")
IDX_CLOSE = COLUMNS.index("close")
IDX_VOLUME = COLUMNS.index("volume")


def bars_to_candles(bars):
    """Convert raw bar lists (COLUMNS schema) to lightweight-charts candle format."""
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
    """Start the Node.js chart server (chart_server.js on port 3001)."""
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


def show_candles(ticker_bars, open_browser=True):
    """
    Show candlestick charts for one or more tickers.

    Args:
        ticker_bars: dict of {ticker: bars} where bars is a list of raw bar lists
                     (COLUMNS schema from polygon_utils), OR a dict of {ticker: candles}
                     where candles is already in lightweight-charts format (list of dicts
                     with time/open/high/low/close/volume keys).
        open_browser: if True, open Chrome/default browser to the chart page.

    The function starts the chart server, posts candle data for each ticker,
    opens the browser, and blocks until Ctrl+C is pressed.
    """
    server_proc = start_chart_server()

    if open_browser:
        webbrowser.open(CHART_SERVER_URL)

    try:
        for ticker, data in ticker_bars.items():
            if not data:
                logging.warning(f"No data for {ticker}, skipping")
                continue

            # Detect format: if first element is a list, it's raw bars; if dict, already candles
            if isinstance(data[0], (list, tuple)):
                candles = bars_to_candles(data)
            else:
                candles = data

            logging.info(f"{ticker}: {len(candles)} candles")
            post_candles(ticker, candles)

        logging.info("All tickers sent. Press Ctrl+C to stop the server.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping chart server...")
    finally:
        server_proc.terminate()
