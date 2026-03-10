import os
import logging
import argparse
import time
import pickle
from rocksdict import Rdict
from datetime import datetime, timezone, timedelta
from ScannerMinute.src import logging_utils, snapshot_utils
from ScannerMinute.src import polygon_utils
from ScannerMinute.definitions import PROJECT_ROOT_DIR


DEFAULT_LOOKBACK_MINUTES = [1, 5, 15, 30, 60]
DEFAULT_BREAKOUT_THRESHOLD = 1.05
DEFAULT_ROCKSDICT_PATH = f"{PROJECT_ROOT_DIR}/data_rocksdict_snapshots"
DEFAULT_MIN_PRICE = 2.0
DEFAULT_MAX_PRICE = 20.0


def get_args_realtime():
    parser = argparse.ArgumentParser(description="Realtime Scanner")
    parser.add_argument(
        "--tickers",
        type=str,
        nargs="*",
        default=None,
        help="tickers to scan (None=all)",
    )
    parser.add_argument(
        "--scanning_period_secs", type=int, default=10, help="seconds between snapshots"
    )
    parser.add_argument(
        "--num_scans", type=int, default=None, help="number of scans (None=infinite)"
    )
    parser.add_argument(
        "--lookback_minutes",
        type=int,
        nargs="*",
        default=DEFAULT_LOOKBACK_MINUTES,
        help="lookback periods in minutes for breakout detection",
    )
    parser.add_argument(
        "--breakout_threshold",
        type=float,
        default=DEFAULT_BREAKOUT_THRESHOLD,
        help="breakout ratio threshold",
    )
    parser.add_argument(
        "--rocksdict_path",
        type=str,
        default=DEFAULT_ROCKSDICT_PATH,
        help="path to rocksdict database",
    )
    parser.add_argument(
        "--min_price",
        type=float,
        default=DEFAULT_MIN_PRICE,
        help="minimum prev_day.vwap price for breakout detection",
    )
    parser.add_argument(
        "--max_price",
        type=float,
        default=DEFAULT_MAX_PRICE,
        help="maximum prev_day.vwap price for breakout detection",
    )
    parser.add_argument(
        "--include_time", type=bool, default=True, help="include time in logs"
    )
    args, _ = parser.parse_known_args()
    return args


def store_snapshot(db: Rdict, key_time_utc: str, snapshots: dict):
    """Store a snapshot dict (ticker -> Snapshot) in rocksdict keyed by UTC time."""
    db[key_time_utc] = pickle.dumps(snapshots)


def load_snapshot(db: Rdict, key_time_utc: str) -> dict:
    """Load a snapshot dict from rocksdict by UTC time key."""
    raw = db.get(key_time_utc)
    if raw is None:
        return None
    return pickle.loads(raw)


def find_closest_key(db: Rdict, target_time: str) -> str:
    """Find the key closest to target_time using rocksdict's native seek."""
    target_dt = datetime.fromisoformat(target_time)
    it = db.iter()
    it.seek(target_time)

    candidates = []
    # Key at or just after target
    if it.valid():
        candidates.append(it.key())
    # Key just before target
    it.seek(target_time)
    it.prev()
    if it.valid():
        candidates.append(it.key())

    if not candidates:
        return None

    return min(
        candidates,
        key=lambda k: abs((datetime.fromisoformat(k) - target_dt).total_seconds()),
    )


def scan_breakouts(
    db: Rdict,
    current_key: str,
    current_snapshots: dict,
    lookback_minutes: list[int],
    threshold: float,
    min_price: float = DEFAULT_MIN_PRICE,
    max_price: float = DEFAULT_MAX_PRICE,
):
    """
    Compare current snapshot prices against past snapshots at each lookback period.
    Returns a list of breakout dicts.
    """
    current_dt = datetime.fromisoformat(current_key)
    breakouts = []

    for lb_minutes in lookback_minutes:
        target_time = (current_dt - timedelta(minutes=lb_minutes)).isoformat()
        past_key = find_closest_key(db, target_time)
        if past_key is None or past_key == current_key:
            continue

        # Skip if the past key is too far from the target
        past_dt = datetime.fromisoformat(past_key)
        max_tolerance_secs = max(30, lb_minutes * 60 * 0.5)
        if (
            abs((past_dt - datetime.fromisoformat(target_time)).total_seconds())
            > max_tolerance_secs
        ):
            continue

        past_snapshots = load_snapshot(db, past_key)
        if past_snapshots is None:
            continue

        for ticker, current_snap in current_snapshots.items():
            # Filter by prev_day.vwap price range
            vwap = current_snap.get("prev_day.vwap")
            if vwap is None or vwap < min_price or vwap > max_price:
                continue

            current_close = current_snap.get("min.close")
            if not current_close or current_close == 0:
                continue

            past_snap = past_snapshots.get(ticker)
            if past_snap is None:
                continue
            past_close = past_snap.get("min.close")
            if not past_close or past_close == 0:
                continue

            ratio = current_close / past_close
            if ratio >= threshold:
                breakouts.append(
                    {
                        "ticker": ticker,
                        "lookback_min": lb_minutes,
                        "ratio": ratio,
                        "current_close": current_close,
                        "past_close": past_close,
                        "current_time": current_key,
                        "past_time": past_key,
                    }
                )

    return breakouts


def log_breakouts(breakouts: list[dict]):
    """Log detected breakouts sorted by ratio descending."""
    if not breakouts:
        return
    breakouts.sort(key=lambda b: b["ratio"], reverse=True)
    logging.info(f"=== {len(breakouts)} BREAKOUTS DETECTED ===")
    for b in breakouts:
        logging.info(
            f"  {b['ticker']:6s} | {b['lookback_min']:3d}m ago | "
            f"ratio={b['ratio']:.4f} | "
            f"${b['past_close']:.2f} -> ${b['current_close']:.2f} | "
            f"past={b['past_time']}"
        )


def process_items(items):
    """Convert raw API items to a dict of ticker -> Snapshot."""
    snapshots = [snapshot_utils.Snapshot(item) for item in items]
    return {snap["ticker"]: snap for snap in snapshots}


def run_realtime(
    tickers=None,
    scanning_period_secs=10,
    num_scans=None,
    lookback_minutes=None,
    breakout_threshold=DEFAULT_BREAKOUT_THRESHOLD,
    rocksdict_path=DEFAULT_ROCKSDICT_PATH,
    min_price=DEFAULT_MIN_PRICE,
    max_price=DEFAULT_MAX_PRICE,
    include_time=True,
):
    if lookback_minutes is None:
        lookback_minutes = DEFAULT_LOOKBACK_MINUTES

    logging_utils.setup_logging(
        log_level="INFO",
        log_folder=f"{os.path.dirname(os.path.abspath(__file__))}/logs/",
        include_time=include_time,
    )

    client = polygon_utils.get_polygon_client()
    os.makedirs(rocksdict_path, exist_ok=True)
    db = Rdict(rocksdict_path)

    logging.info(
        f"Starting realtime scanner: period={scanning_period_secs}s, "
        f"lookbacks={lookback_minutes}min, threshold={breakout_threshold}"
    )

    scan_count = 0
    try:
        while num_scans is None or scan_count < num_scans:
            t0 = time.time()

            # Download snapshot
            key_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            items = client.get_snapshot_all("stocks", tickers=tickers)
            snapshots = process_items(items)

            # Store in rocksdict
            store_snapshot(db, key_time_utc, snapshots)
            scan_count += 1
            logging.info(
                f"[scan #{scan_count}] Stored {len(snapshots)} tickers at {key_time_utc}"
            )

            # Run breakout detection
            breakouts = scan_breakouts(
                db,
                key_time_utc,
                snapshots,
                lookback_minutes,
                breakout_threshold,
                min_price,
                max_price,
            )
            log_breakouts(breakouts)

            # Sleep remaining time
            elapsed = time.time() - t0
            sleep_time = max(0, scanning_period_secs - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        logging.info("Scanner stopped by user")
    finally:
        db.close()
        logging.info(f"Database closed. Total scans: {scan_count}")


if __name__ == "__main__":
    run_realtime()
