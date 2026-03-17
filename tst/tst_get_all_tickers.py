import os
import logging
from ScannerMinute.src import polygon_utils, logging_utils


def main():
    current_folder = os.path.dirname(os.path.abspath(__file__))
    logging_utils.setup_logging(log_level="INFO", log_folder=f"{current_folder}/logs/")

    client = polygon_utils.get_polygon_client()
    tickers = polygon_utils.get_all_tickers_from_snapshot(client)
    logging.info(f"Total tickers: {len(tickers)}")
    logging.info(f"First 20: {tickers[:20]}")


if __name__ == "__main__":
    main()
