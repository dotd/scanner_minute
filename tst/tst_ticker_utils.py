import logging
from ScannerMinute.src import logging_utils
from ScannerMinute.src.ticker_utils import get_nasdaq_composite_tickers


def tst_get_nasdaq_composite():
    logging_utils.setup_logging(log_level="INFO", include_time=True, log_folder="./logs/")

    tickers = get_nasdaq_composite_tickers()
    logging.info(f"Total NASDAQ Composite tickers: {len(tickers)}")

    for i in range(0, len(tickers), 10):
        chunk = [f"{i+j+1:4d}) {t}" for j, t in enumerate(tickers[i : i + 10])]
        print("  ".join(chunk))


if __name__ == "__main__":
    tst_get_nasdaq_composite()
