import logging
from datetime import datetime, timezone, timedelta
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
    logging.info(f"Download {ticker} {timespan} {date_start} {date_end}")
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


def generate_tasks(
    tickers: list[str], date_start: str, date_end: str, db_path: str = None
) -> list[tuple]:
    """
    Generate (ticker, month_start, month_end, "minute", idx) task tuples
    for every ticker x monthly chunk.

    If db_path is provided, checks each ticker's last timestamp in RocksDB.
    If data already exists up to some date, only generates tasks from the day
    after that date to date_end. If the DB is already up to date, skips the ticker.

    Input format for dates: "YYYY-MM-DD"
    """
    from ScannerMinute.src.rocksdict_utils import get_first_and_last_time

    tasks = []
    idx_task = 0
    for ticker in tickers:
        ticker_start = date_start

        if db_path:
            _, last_time = get_first_and_last_time(db_path, "minute", ticker)
            if last_time:
                last_date = last_time[:10]  # "2024-01-15T09:30:00" → "2024-01-15"
                if last_date >= date_end:
                    logging.info(
                        f"Skipping {ticker}: DB already has data up to {last_date}"
                    )
                    continue
                day_after_last = (
                    datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
                ).strftime("%Y-%m-%d")
                ticker_start = day_after_last
                logging.info(
                    f"{ticker}: DB has data up to {last_date}, tasks from {ticker_start}"
                )

        monthly_ranges = generate_monthly_ranges(ticker_start, date_end)
        for month_start, month_end in monthly_ranges:
            tasks.append((ticker, month_start, month_end, "minute", idx_task))
            idx_task += 1

    logging.info(f"Generated {len(tasks)} tasks for {len(tickers)} tickers")
    for idx_task, task in enumerate(tasks):
        logging.info(f"Task {idx_task}: {task}")
    return tasks


def download_and_save_ticker(client, ticker, timespan, date_start, date_end):
    """
    Download data month-by-month and save each chunk to DuckDB immediately.
    This avoids hitting the 50,000 sample API limit per request.
    """
    from ScannerMinute.src import duckdb_utils

    monthly_ranges = generate_monthly_ranges(date_start, date_end)
    for month_start, month_end in monthly_ranges:
        logging.info(f"Processing {ticker}: {month_start} to {month_end}")
        data = get_ticker_data_from_polygon(
            client, ticker, timespan, month_start, month_end
        )
        if data:
            duckdb_utils.save_bars(ticker, data)
            logging.info(
                f"Saved {len(data)} bars for {ticker} ({month_start} to {month_end})"
            )
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


def get_all_tickers_from_snapshot(client) -> list[str]:
    """
    Download a single snapshot of all stocks and extract the list of tickers.
    """
    items = client.get_snapshot_all("stocks")
    tickers = [
        item.ticker
        for item in items
        if hasattr(item, "ticker") and item.ticker is not None
    ]
    logging.info(f"Found {len(tickers)} tickers from snapshot")
    return sorted(tickers)


def get_trading_days(
    client,
    from_=(datetime.now(timezone.utc) - timedelta(days=7 * 365)).strftime(
        "%Y-%m-%d"
    ),  # a year ago from today
    to=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    ticker="AAPL",
):
    """
    Return a list of trading day strings ("YYYY-MM-DD") between from_ and to (inclusive).

    Uses daily bars from Polygon (for the given ticker) to determine which days had trading
    activity. Note: Polygon's daily bar for the current day is only finalized after market
    close, so today may not appear in the daily bars even though intraday (minute/second)
    data already exists. To handle this, if `to` is today and today is a weekday (Mon-Fri),
    it is included as a trading day regardless of whether a daily bar exists yet.
    """
    bars = client.get_aggs(
        ticker=ticker,
        multiplier=1,
        timespan="day",
        from_=from_,
        to=to,
        limit=50000,
    )
    trading_days = list()
    for bar in bars:
        if bar.volume > 0:
            value = datetime.fromtimestamp(bar.timestamp / 1000, tz=timezone.utc)
            datetime_str = value.strftime("%Y-%m-%d")
            trading_days.append(datetime_str)

    # If `to` is today and a weekday, include it even if no daily bar exists yet
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if to == today_str and today_str not in trading_days:
        today_weekday = datetime.now(timezone.utc).weekday()
        if today_weekday < 5:  # Mon=0 .. Fri=4
            trading_days.append(today_str)

    return trading_days
