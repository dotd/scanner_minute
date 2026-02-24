import logging
from datetime import datetime, timezone
from calendar import monthrange
from polygon import RESTClient
from ScannerMinute.src import snapshot_utils


def get_polygon_client():
    api_key = open("api_keys/polygon_api_key.txt", "r").read().strip()
    logging.info(f"Using Polygon API key: {api_key}")
    return RESTClient(api_key=api_key)


COLUMNS = [
    "ticker",
    "datetime_utc",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "timestamp",
    "transactions",
    "otc",
]


def process_bar(ticker, bar):
    datetime_utc = datetime.utcfromtimestamp(bar.timestamp / 1000).strftime(
        "%Y%m%d_%H%M%S"
    )
    # date_utc = datetime.utcfromtimestamp(bar.timestamp / 1000).strftime("%Y%m%d")
    # time_utc = datetime.utcfromtimestamp(bar.timestamp / 1000).strftime("%H%M%S")
    res = [
        ticker,
        datetime_utc,
        # date_utc,
        # time_utc,
        bar.open,
        bar.high,
        bar.low,
        bar.close,
        bar.volume,
        bar.vwap,
        bar.timestamp,
        bar.transactions,
        bar.otc,
    ]
    return res


def get_ticker_data_from_polygon(
    client,  # client for polygon API
    ticker,  # the ticker to download
    timespan,  # what timepsan to download?
    date_start,  # which date?
    date_end,  # which date?
):
    """
    This function downloads the data from polygon API and returns list of lists with the data.
    """
    logging.info(
        f"Downloading from Polygon API: {ticker} {timespan} {date_start} until {date_end}"
    )
    bars = client.get_aggs(
        ticker=ticker,
        multiplier=1,
        timespan=timespan,
        from_=date_start,
        to=date_end,
        limit=50000,
    )
    data = list()
    for bar in bars:
        vec = process_bar(ticker, bar)
        data.append(vec)
    return data


def generate_monthly_ranges(date_start: str, date_end: str) -> list[tuple[str, str]]:
    """
    Split a date range into monthly chunks.
    Input format: "YYYY-MM-DD"
    Returns list of (month_start, month_end) tuples in the same format.
    """
    start = datetime.strptime(date_start, "%Y-%m-%d")
    end = datetime.strptime(date_end, "%Y-%m-%d")
    ranges = []
    current = start
    while current <= end:
        year, month = current.year, current.month
        last_day = monthrange(year, month)[1]
        month_end = datetime(year, month, last_day)
        chunk_end = min(month_end, end)
        ranges.append((current.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        # Move to 1st of next month
        if month == 12:
            current = datetime(year + 1, 1, 1)
        else:
            current = datetime(year, month + 1, 1)
    return ranges


def download_and_save_ticker(client, ticker, timespan, date_start, date_end):
    """
    Download data month-by-month and save each chunk to DuckDB immediately.
    This avoids hitting the 50,000 sample API limit per request.
    """
    from ScannerMinute.src import duckdb_utils

    monthly_ranges = generate_monthly_ranges(date_start, date_end)
    for month_start, month_end in monthly_ranges:
        logging.info(f"Processing {ticker}: {month_start} to {month_end}")
        data = get_ticker_data_from_polygon(client, ticker, timespan, month_start, month_end)
        if data:
            duckdb_utils.save_bars(ticker, data)
            logging.info(f"Saved {len(data)} bars for {ticker} ({month_start} to {month_end})")
        else:
            logging.info(f"No data for {ticker} ({month_start} to {month_end})")


def get_rounded_time_utc(modulo_secs=10):
    t = datetime.now(timezone.utc)
    current_time = t.strftime("%H%M%S")
    key_time_HM = t.strftime("%H%M")
    key_time_S = t.strftime("%S")
    key_time_S_rounded = str(int(key_time_S) // modulo_secs * modulo_secs).zfill(2)
    key_time_round = key_time_HM + key_time_S_rounded
    return current_time, key_time_round, t


def get_snapshot_from_polygon(
    client,  # client for polygon API
    tickers=None,  # list of tickers to get snapshot for
):
    current_time, key_time_round, t = get_rounded_time_utc()
    items = client.get_snapshot_all("stocks", tickers=tickers)
    snapshots = list()
    for item in items:
        snapshot = snapshot_utils.Snapshot(item)
        snapshots.append(snapshot)
    return snapshots, current_time, key_time_round, t
