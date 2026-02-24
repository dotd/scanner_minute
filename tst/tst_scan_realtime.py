import os
import logging
import pprint
import argparse
import time
import pickle
from rocksdict import Rdict
from datetime import datetime, timezone
from collections import OrderedDict
from ScannerMinute.src import logging_utils, snapshot_utils
from ScannerMinute.src import polygon_utils
from ScannerMinute.definitions import PROJECT_ROOT_DIR
from massive.rest.models import TickerSnapshot, Agg


class MyRdict:
    """
    This class is a wrapper around the Rdict database.
    It is used to store the snapshots of the tickers.
    """

    def __init__(self, path):
        self.db = Rdict(path)

    def get(self, key):
        return self.db.get(key)

    def set(self, key, value):
        self.db.put(key, pickle.dumps(value))

    def get_last(self, K):
        # Get the last K snapshots from the Rdict database.
        it = self.db.iter()
        it.seek_to_last()

        results = []
        for _ in range(K):
            if not it.valid():
                break
            key = it.key()
            value = pickle.loads(it.value())
            results.append((key, value))
            it.prev()
        return results


def get_args_realtime():
    """
    This function returns the arguments for the Scanner02 script.
    Precedence: CLI > YAML > Script Defaults.
    """
    logging.info("get_args_realtime")
    parser = argparse.ArgumentParser(description="Scanner02")
    parser.add_argument("--config", type=str, help="Path to the YAML config file")
    parser.add_argument("--tickers", type=list, default=None, help="tickers")
    parser.add_argument(
        "--scanning_period_secs", type=int, default=10, help="scanning_period_secs"
    )
    parser.add_argument("--sleep_secs", type=int, default=2, help="sleep_secs")
    parser.add_argument("--past_samples", type=int, default=10, help="past_samples")
    parser.add_argument(
        "--breakout_threshold", type=float, default=1.03, help="breakout_threshold"
    )
    parser.add_argument("--include_time", type=bool, default=False, help="include_time")
    args, remaining_args = parser.parse_known_args()
    return args


# datetime.now(timezone.utc).strftime("%Y%m%d")


def get_tickers_data_from_time_data(time_data):
    tickers_data = dict()
    for time_snapshots in time_data:
        for ticker in time_snapshots.snapshots:
            if ticker not in tickers_data:
                tickers_data[ticker] = OrderedDict()
            tickers_data[ticker][time_snapshots.time] = time_snapshots.snapshots[ticker]
    return tickers_data


def scan_for_breakouts(day_data, past_samples, threshold):
    # If there are 1 or less samples, we can't run the breakouts
    if len(day_data) <= 1:
        logging.info(f"Not enough data to scan for breakouts: {len(day_data)}")
        return
    # Get the data of the latest sample
    latest_sample = day_data[-1]
    stocks = latest_sample.snapshots.keys()
    # Going over the tickers
    indices = range(1, min(len(day_data), past_samples + 1))
    for ticker in stocks:
        # Get the data for the latest sample. Of Noen, continue
        latest_data = latest_sample.snapshots.get(ticker, None)
        if (
            latest_data is None
            or latest_data["min.close"] is None
            or latest_data["min.close"] == 0
        ):
            continue

        # Loop that goes over the past samples
        for n in indices:
            count_errors = 0
            prev_day_data = day_data[-1 - n]
            prev_data = prev_day_data.snapshots.get(ticker, None)

            if prev_data is None:
                continue
            # Check if the change in close is above threshold
            if prev_data["min.close"] == 0:
                count_errors += 1
                continue
            rate = latest_data["min.close"] / prev_data["min.close"]
            if rate >= threshold:
                prices = [
                    day_data[-1 - n].snapshots.get(ticker, None)["min.close"]
                    for n in range(1, min(len(day_data), past_samples + 1))
                ]
                logging.info(
                    f"Breakout: {latest_data['min.timestamp_human_utc']}"
                    + f"{prev_data['min.timestamp_human_utc']} {ticker}"
                    + f" {rate:.2f} >= {threshold:.2f} {prices}"
                )


def process_items(items):
    snapshots = [snapshot_utils.Snapshot(item) for item in items]
    snapshots = {snapshot["ticker"]: snapshot for snapshot in snapshots}
    return snapshots


def run_realtime(
    tickers,  # None
    scanning_period_secs,  # 10
    sleep_secs,  # 1
    past_samples,  # 10
    breakout_threshold,  # 1.03
    include_time=False,  # False
):
    log_file = logging_utils.setup_logging(
        log_level="INFO",
        log_folder=f"{os.path.dirname(os.path.abspath(__file__))}/logs/",
        include_time=include_time,
    )

    client = polygon_utils.get_polygon_client()
    rocks_dict = MyRdict(f"{PROJECT_ROOT_DIR}/rdict_data")

    for i in range(3):
        key_time_now_utc = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )  #  ISO 8601 format: YYYY-MM-DDTHH:MM:SS.SSSSSS
        # snapshots is a list of TickerSnapshot objects where each object is the info for a single ticker
        items = client.get_snapshot_all("stocks", tickers=tickers)
        # Let's translate the items to Snapshot objects
        snapshots = process_items(items)
        rocks_dict.set(key_time_now_utc, snapshots)
        logging.info(
            f"Pushed {len(snapshots)} snapshots to Rdict with key {key_time_now_utc}"
        )

        time.sleep(sleep_secs)

    last_snapshots = rocks_dict.get_last(2)
    for key, value in last_snapshots:
        logging.info(f"key: {key}")
        logging.info(f"value:\n{pprint.pformat(value["AAPL"], indent=4)}")


def tst_run_realtime():
    current_folder = os.path.dirname(os.path.abspath(__file__))
    log_file = logging_utils.setup_logging(
        log_level="INFO", log_folder=f"{current_folder}/logs/"
    )
    logging.info(f"log_file: {log_file}")
    args = get_args_realtime()
    logging.info(f"args:\n{pprint.pformat(args, indent=4)}")

    run_realtime(
        tickers=args.tickers,
        scanning_period_secs=args.scanning_period_secs,
        sleep_secs=args.sleep_secs,
        past_samples=args.past_samples,
        breakout_threshold=args.breakout_threshold,
        include_time=args.include_time,
    )


if __name__ == "__main__":
    tst_run_realtime()
