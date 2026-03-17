import logging
import time
from ScannerMinute.src import rocksdict_utils
from ScannerMinute.src import logging_utils


def load_tqqq():
    logging_utils.setup_logging(log_level="INFO", include_time=True)

    db_path = rocksdict_utils.DEFAULT_DB_PATH
    ticker = "TQQQ"
    timespan = "minute"

    # Check what's in the DB for this ticker
    first_time, last_time = rocksdict_utils.get_first_and_last_time(db_path, timespan, ticker)
    logging.info(f"{ticker}: first={first_time}, last={last_time}")

    if first_time is None:
        logging.info(f"No data found for {ticker} in the database.")
        return []

    # Read all bars from first to last
    t0 = time.time()
    bars = rocksdict_utils.read_bars(db_path, timespan, [ticker], first_time, last_time)
    elapsed = time.time() - t0
    logging.info(f"Loaded {len(bars)} bars for {ticker} in {elapsed:.2f}s")

    if bars:
        logging.info(f"First bar: {bars[0]}")
        logging.info(f"Last bar:  {bars[-1]}")

    return bars


if __name__ == "__main__":
    load_tqqq()
