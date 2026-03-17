import glob
import logging
import os
import threading
import time
from collections import OrderedDict
from datetime import datetime, timezone
import pickle

import pytz

from ScannerMinute.src import polygon_utils

time_template = "%Y-%m-%d %H:%M:%S"


class Snapshot(OrderedDict):

    def __init__(self, item=None, time_rounded=None):
        self["time_rounded"] = time_rounded
        self["ticker"] = (
            item.ticker if hasattr(item, "ticker") and item.ticker is not None else None
        )
        self["today_change"] = (
            item.todays_change
            if hasattr(item, "todays_change") and item.todays_change is not None
            else None
        )
        self["today_change_percent"] = (
            item.todays_change_percent
            if hasattr(item, "todays_change_percent")
            and item.todays_change_percent is not None
            else None
        )
        self["updated"] = (
            item.updated
            if hasattr(item, "updated") and item.updated is not None
            else None
        )
        self["updated_human_utc"] = (
            datetime.fromtimestamp(item.updated / 1000000000, tz=pytz.UTC).strftime(
                time_template
            )
            if hasattr(item, "updated") and item.updated is not None
            else None
        )
        # -----
        pd = getattr(item, "prev_day", None)
        self["prev_day.open"] = (
            pd.open if pd and hasattr(pd, "open") and pd.open is not None else None
        )
        self["prev_day.high"] = (
            pd.high if pd and hasattr(pd, "high") and pd.high is not None else None
        )
        self["prev_day.low"] = (
            pd.low if pd and hasattr(pd, "low") and pd.low is not None else None
        )
        self["prev_day.close"] = (
            pd.close if pd and hasattr(pd, "close") and pd.close is not None else None
        )
        self["prev_day.volume"] = (
            pd.volume
            if pd and hasattr(pd, "volume") and pd.volume is not None
            else None
        )
        self["prev_day.vwap"] = (
            pd.vwap if pd and hasattr(pd, "vwap") and pd.vwap is not None else None
        )
        self["prev_day.timestamp"] = (
            pd.timestamp
            if pd and hasattr(pd, "timestamp") and pd.timestamp is not None
            else None
        )
        self["prev_day.timestamp_human_utc"] = (
            datetime.fromtimestamp(pd.timestamp / 1000000000, tz=pytz.UTC).strftime(
                time_template
            )
            if pd and hasattr(pd, "timestamp") and pd.timestamp is not None
            else None
        )
        self["prev_day.transactions"] = (
            pd.transactions
            if pd and hasattr(pd, "transactions") and pd.transactions is not None
            else None
        )
        self["prev_day.otc"] = (
            pd.otc if pd and hasattr(pd, "otc") and pd.otc is not None else None
        )
        # -----
        mn = getattr(item, "min", None)
        # minute = min
        self["min.accumulated_volume"] = (
            mn.accumulated_volume
            if mn
            and hasattr(mn, "accumulated_volume")
            and mn.accumulated_volume is not None
            else None
        )
        self["min.open"] = (
            mn.open if mn and hasattr(mn, "open") and mn.open is not None else None
        )
        self["min.high"] = (
            mn.high if mn and hasattr(mn, "high") and mn.high is not None else None
        )
        self["min.low"] = (
            mn.low if mn and hasattr(mn, "low") and mn.low is not None else None
        )
        self["min.close"] = (
            mn.close if mn and hasattr(mn, "close") and mn.close is not None else None
        )
        self["min.volume"] = (
            mn.volume
            if mn and hasattr(mn, "volume") and mn.volume is not None
            else None
        )
        self["min.vwap"] = (
            mn.vwap if mn and hasattr(mn, "vwap") and mn.vwap is not None else None
        )
        self["min.otc"] = (
            mn.otc if mn and hasattr(mn, "otc") and mn.otc is not None else None
        )
        self["min.timestamp"] = (
            mn.timestamp
            if mn and hasattr(mn, "timestamp") and mn.timestamp is not None
            else None
        )
        self["min.timestamp_human_utc"] = (
            datetime.fromtimestamp(mn.timestamp / 1000, tz=pytz.UTC).strftime(
                time_template
            )
            if mn and hasattr(mn, "timestamp") and mn.timestamp is not None
            else None
        )
        # -----
        self["last_quote"] = (
            item.last_quote
            if hasattr(item, "last_quote") and item.last_quote is not None
            else None
        )
        self["last_trade"] = (
            item.last_trade
            if hasattr(item, "last_trade") and item.last_trade is not None
            else None
        )

    def __str__(self, sep="\n"):
        """Human-readable string; sep is the separator between lines (default newline)."""
        lines = [f"  {key}: {value}" for key, value in self.items()]
        return sep.join(lines)

    def __repr__(self):
        """Unambiguous representation for repr() and debugger."""
        return f"Snapshot({dict(self)})"


def get_snapshot(client, tickers=None):
    """
    Download a snapshot of all (or specified) stocks from Polygon.

    Parameters:
        client: polygon RESTClient
        tickers: list[str] or None — if None, downloads all stocks

    Returns:
        (key_time_now_utc, items, snapshots)
        - key_time_now_utc: str — ISO 8601 timestamp e.g. "2026-03-07T14:30:05"
        - items: list — raw TickerSnapshot objects from Polygon
        - snapshots: list[Snapshot]
    """
    key_time_now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    items = client.get_snapshot_all("stocks", tickers=tickers)
    snapshots = [Snapshot(item) for item in items]
    return key_time_now_utc, items, snapshots


def _consolidation_worker(
    snapshots_downloaded, snapshot_consolidated, period_consolidation, stop_event
):
    """Background thread: periodically consolidates individual snapshot pickles into one file."""
    while not stop_event.is_set():
        stop_event.wait(period_consolidation)
        if stop_event.is_set():
            break
        try:
            pkl_files = sorted(glob.glob(os.path.join(snapshots_downloaded, "d_*.pkl")))
            if not pkl_files:
                logging.info("[CONSOLIDATION] No files to consolidate.")
                continue

            consolidated = []
            for pkl_file in pkl_files:
                with open(pkl_file, "rb") as f:
                    data = pickle.load(f)
                consolidated.append(data)

            # Name: first_timestamp _ count _ last_timestamp .pkl
            first_name = (
                os.path.basename(pkl_files[0]).replace("d_", "").replace(".pkl", "")
            )
            last_name = (
                os.path.basename(pkl_files[-1]).replace("d_", "").replace(".pkl", "")
            )
            consolidated_path = os.path.join(
                snapshot_consolidated, f"{first_name}_{len(pkl_files)}_{last_name}.pkl"
            )
            with open(consolidated_path, "wb") as f:
                pickle.dump(consolidated, f)

            deleted_names = [os.path.basename(f) for f in pkl_files]
            for pkl_file in pkl_files:
                os.remove(pkl_file)

            logging.info(
                f"[CONSOLIDATION] Consolidated {len(pkl_files)} files "
                f"({len(consolidated)} snapshots) → {consolidated_path}\n"
                f"  Deleted: {deleted_names}"
            )
        except Exception as e:
            logging.error(f"[CONSOLIDATION] Error: {e}")


def download_snapshots(
    period=10,
    snapshots_downloaded="snapshots_downloaded",
    snapshot_consolidated="snapshot_consolidated",
    snapshots_logs="snapshots_logs",
    period_consolidation=300,
    consolidate=True,
):
    """
    Infinite loop that downloads snapshots from Polygon every `period` seconds.

    Each snapshot is saved as a pickle file (key_time_now_utc, items, snapshots)
    in `snapshots_downloaded/` with filename d_YYYYMMDD_HHMMSS.pkl.

    If consolidate=True, a background thread consolidates individual pickles every
    `period_consolidation` seconds into a single pickle in `snapshot_consolidated/`,
    then removes the originals. The consolidated filename format is:
    {first_timestamp}_{count}_{last_timestamp}.pkl

    Parameters:
        period: float — seconds between downloads (default 10)
        snapshots_downloaded: str — folder for individual snapshot pickles
        snapshot_consolidated: str — folder for consolidated pickles
        snapshots_logs: str — folder for log files
        period_consolidation: float — seconds between consolidation runs (default 300)
        consolidate: bool — whether to run the consolidation thread (default True)
    """
    from ScannerMinute.src import logging_utils

    os.makedirs(snapshots_downloaded, exist_ok=True)
    os.makedirs(snapshots_logs, exist_ok=True)

    logging_utils.setup_logging(
        log_level="INFO",
        log_folder=snapshots_logs,
        include_time=True,
    )

    client = polygon_utils.get_polygon_client()

    # Start consolidation thread if enabled
    stop_event = threading.Event()
    consolidation_thread = None
    if consolidate:
        os.makedirs(snapshot_consolidated, exist_ok=True)
        consolidation_thread = threading.Thread(
            target=_consolidation_worker,
            args=(
                snapshots_downloaded,
                snapshot_consolidated,
                period_consolidation,
                stop_event,
            ),
            daemon=True,
        )
        consolidation_thread.start()

    logging.info(
        f"Started snapshot downloader: period={period}s, "
        f"consolidation={'every ' + str(period_consolidation) + 's' if consolidate else 'disabled'}"
    )

    count = 0
    try:
        while True:
            t0 = time.time()
            try:
                key_time, items, snapshots = get_snapshot(client)
                ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                pkl_path = os.path.join(snapshots_downloaded, f"d_{ts}.pkl")
                with open(pkl_path, "wb") as f:
                    pickle.dump((key_time, items, snapshots), f)
                count += 1
                logging.info(
                    f"[DOWNLOAD] #{count} | {len(snapshots)} tickers | "
                    f"key={key_time} | saved to {pkl_path}"
                )
            except Exception as e:
                logging.error(f"[DOWNLOAD] Error: {e}")

            elapsed = time.time() - t0
            sleep_time = max(0, period - elapsed)
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        logging.info("Stopping snapshot downloader...")
        stop_event.set()
        if consolidation_thread:
            consolidation_thread.join()
        logging.info("Stopped.")
