import os
import logging
import pprint
import argparse
import time
import pickle
from datetime import datetime, timezone
from collections import OrderedDict
from ScannerMinute.src import logging_utils, snapshot_utils
from ScannerMinute.src import polygon_utils
from ScannerMinute.definitions import PROJECT_ROOT_DIR
from massive.rest.models import TickerSnapshot, Agg


class TimeSnapshots:
    def __init__(self, t=None, snapshots=None):
        self.time = t
        self.snapshots = snapshots

    def __str__(self):
        return f"TimeSnapshots(t={self.time}, snapshots={len(self.snapshots)})"

    def __repr__(self):
        return self.__str__()


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


def get_current_date_utc():
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def get_rounded_time(modulo_secs=10):
    t = datetime.now(timezone.utc)
    current_time = t.strftime("%H%M%S")
    key_time_HM = t.strftime("%H%M")
    key_time_S = t.strftime("%S")
    key_time_S_rounded = str(int(key_time_S) // modulo_secs * modulo_secs).zfill(2)
    key_time_round = key_time_HM + key_time_S_rounded
    return current_time, key_time_round


def make_folders(current_date):
    time_folder = f"{PROJECT_ROOT_DIR}/data_snapshots/time/{current_date}"
    os.makedirs(time_folder, exist_ok=True)
    return time_folder


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

    current_date = get_current_date_utc()
    snapshots_time_folder = make_folders(current_date)
    list_of_time_snapshots = load_or_prepare_day_data(snapshots_time_folder)
    tickers2snapshots = get_tickers_data_from_time_data(list_of_time_snapshots)

    client = polygon_utils.get_polygon_client()
    prev_key_time_rounded = None
    for i in range(100000):
        # snapshots is a list of TickerSnapshot objects where each object is the info for a single ticker
        snapshots = client.get_snapshot_all("stocks", tickers=tickers)
        # aapl_idx = utils.find_first_index(snapshots, lambda x: x.ticker == "AAPL"); snapshot = snapshots[aapl_idx]; pr = Snapshot(snapshot); pprint.pprint(pr);

        _, key_time_rounded = get_rounded_time(modulo_secs=scanning_period_secs)
        # logging.info(f"{i:05d} key_time: {key_time}, key_time_round: {key_time_round} len day_data: {len(day_data)}")
        # If it is a new key, let's add it to the list.
        if key_time_rounded != prev_key_time_rounded:
            # We add an empty TimeSnapshots object to the list
            list_of_time_snapshots.append(TimeSnapshots(t=key_time_rounded))

            # Save previous rounded time
            if prev_key_time_rounded is not None:
                filename = f"{snapshots_time_folder}/{prev_key_time_rounded}.pkl"
                with open(filename, "wb") as f:
                    logging.info(f"Saving snapshots to {filename}")
                    # Note that we are saving the snapshots of the previous time, not the current one
                    pickle.dump(list_of_time_snapshots[-2].snapshots, f)
            else:
                logging.info(f" No previous key time rounded")

        # Going over the snapshots and adding them to the day_data
        ticker_snapshots_dict = dict()  # ticker -> Snapshots
        for entry_idx, item in enumerate(snapshots):
            # verify this is an TickerSnapshot and verify this is an Agg
            if isinstance(item, TickerSnapshot) and isinstance(item.prev_day, Agg):
                snapshot = snapshot_utils.Snapshot(item, time_rounded=key_time_rounded)
                # logging.info(f"pr:\n{pprint.pformat(pr, indent=4)}")
                ticker = snapshot["ticker"]
                if ticker not in tickers2snapshots:
                    tickers2snapshots[ticker] = OrderedDict()
                tickers2snapshots[ticker][key_time_rounded] = snapshot
                ticker_snapshots_dict[ticker] = snapshot
        list_of_time_snapshots[-1].snapshots = ticker_snapshots_dict

        # logging.info(f"End i: {i}")
        time.sleep(sleep_secs)
        prev_key_time_rounded = key_time_rounded
        prev_snapshots = snapshots
        scan_for_breakouts(
            list_of_time_snapshots,
            past_samples=past_samples,
            threshold=breakout_threshold,
        )


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
