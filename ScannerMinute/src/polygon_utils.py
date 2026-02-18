import logging
from datetime import datetime
from polygon import RESTClient


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
    res = [
        ticker,
        datetime_utc,
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
