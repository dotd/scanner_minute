"""
Statistics and analysis for snapshot data stored in RocksDB.

The snapshot DB (default: ./data/rocksdict_snapshots/) stores market snapshots
keyed by ISO UTC timestamps (e.g. "2026-04-09T14:30:00"). Each value is a
pickled dict of {ticker: Snapshot(OrderedDict)} from the realtime scanner.
"""
import logging
import os
import pickle
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone

from rocksdict import Rdict, AccessType
from tqdm import tqdm

from ScannerMinute.src import logging_utils
from ScannerMinute.definitions import PROJECT_ROOT_DIR


DEFAULT_SNAPSHOT_DB_PATH = f"{PROJECT_ROOT_DIR}/data/rocksdict_snapshots"
REPORT_DIR = os.path.join(PROJECT_ROOT_DIR, "reports", "snapshots")


@dataclass
class SnapshotStats:
    total_snapshots: int
    total_unique_tickers: int
    first_snapshot_time: str
    last_snapshot_time: str
    snapshot_date_counts: Counter  # date -> number of snapshots
    tickers_per_snapshot: dict  # snapshot_key -> ticker count
    min_tickers_per_snapshot: int
    max_tickers_per_snapshot: int
    avg_tickers_per_snapshot: float
    ticker_appearance_counts: Counter  # ticker -> how many snapshots it appears in
    most_common_tickers: list  # top 20 (ticker, count) pairs
    snapshot_times: list  # all snapshot keys sorted


def collect_snapshot_stats(db_path=DEFAULT_SNAPSHOT_DB_PATH):
    """
    Scan the snapshot RocksDB and collect comprehensive statistics.

    Returns a SnapshotStats dataclass with snapshot counts, ticker coverage,
    date histograms, and per-snapshot ticker counts.
    """
    logging_utils.setup_logging(
        log_level="INFO",
        log_folder=os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs"),
        include_time=True,
    )

    logging.info(f"[SNAPSHOT STATS] Scanning DB at {db_path}")

    db = Rdict(db_path, access_type=AccessType.read_only())

    snapshot_times = []
    tickers_per_snapshot = {}
    ticker_appearance_counts = Counter()
    snapshot_date_counts = Counter()
    all_tickers = set()

    it = db.iter()
    it.seek_to_first()

    scanned = 0
    skipped = 0
    pbar = tqdm(desc="Scanning snapshot keys", unit=" snapshots")
    while it.valid():
        key = it.key()
        raw = it.value()

        try:
            snapshots = pickle.loads(raw)
        except Exception:
            skipped += 1
            it.next()
            continue

        snapshot_times.append(key)
        date_part = key[:10]
        snapshot_date_counts[date_part] += 1

        ticker_count = len(snapshots)
        tickers_per_snapshot[key] = ticker_count

        for ticker in snapshots:
            ticker_appearance_counts[ticker] += 1
            all_tickers.add(ticker)

        scanned += 1
        if scanned % 100 == 0:
            pbar.update(100)
        it.next()

    pbar.update(scanned % 100)
    pbar.close()
    db.close()

    if skipped:
        logging.warning(f"[SNAPSHOT STATS] Skipped {skipped} entries with invalid data")

    snapshot_times.sort()

    counts = list(tickers_per_snapshot.values()) if tickers_per_snapshot else [0]

    logging.info(
        f"[SNAPSHOT STATS] Scanned {scanned} snapshots, "
        f"{len(all_tickers)} unique tickers"
    )

    return SnapshotStats(
        total_snapshots=scanned,
        total_unique_tickers=len(all_tickers),
        first_snapshot_time=snapshot_times[0] if snapshot_times else "",
        last_snapshot_time=snapshot_times[-1] if snapshot_times else "",
        snapshot_date_counts=snapshot_date_counts,
        tickers_per_snapshot=tickers_per_snapshot,
        min_tickers_per_snapshot=min(counts),
        max_tickers_per_snapshot=max(counts),
        avg_tickers_per_snapshot=sum(counts) / len(counts),
        ticker_appearance_counts=ticker_appearance_counts,
        most_common_tickers=ticker_appearance_counts.most_common(20),
        snapshot_times=snapshot_times,
    )


def generate_snapshot_report(stats):
    """
    Generate a human-readable text report from SnapshotStats.
    Saves to reports/snapshots/report_<timestamp>.txt.
    Returns the report path.
    """
    lines = []
    now = datetime.now(timezone.utc)
    lines.append(f"Snapshot Data Report — {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    lines.append("=" * 80)
    lines.append("")

    lines.append("SUMMARY")
    lines.append("-" * 80)
    lines.append(f"Total snapshots:             {stats.total_snapshots:,}")
    lines.append(f"Total unique tickers:        {stats.total_unique_tickers:,}")
    lines.append(f"First snapshot:              {stats.first_snapshot_time}")
    lines.append(f"Last snapshot:               {stats.last_snapshot_time}")
    lines.append(f"Min tickers per snapshot:    {stats.min_tickers_per_snapshot:,}")
    lines.append(f"Max tickers per snapshot:    {stats.max_tickers_per_snapshot:,}")
    lines.append(f"Avg tickers per snapshot:    {stats.avg_tickers_per_snapshot:,.1f}")
    lines.append(f"Days with data:              {len(stats.snapshot_date_counts)}")
    lines.append("")

    # Date histogram
    lines.append("SNAPSHOTS PER DATE")
    lines.append("-" * 80)
    lines.append(f"{'Date':<12} {'Snapshots':>10}")
    for date in sorted(stats.snapshot_date_counts.keys()):
        lines.append(f"{date:<12} {stats.snapshot_date_counts[date]:>10}")
    lines.append("")

    # Top tickers
    lines.append("TOP 20 TICKERS BY APPEARANCE")
    lines.append("-" * 80)
    lines.append(f"{'Ticker':<10} {'Appearances':>12} {'% of snapshots':>16}")
    for ticker, count in stats.most_common_tickers:
        pct = count / stats.total_snapshots * 100 if stats.total_snapshots else 0
        lines.append(f"{ticker:<10} {count:>12,} {pct:>15.1f}%")
    lines.append("")

    # Tickers per snapshot over time (sampled if too many)
    lines.append("TICKERS PER SNAPSHOT (by time)")
    lines.append("-" * 80)
    lines.append(f"{'Time':<22} {'Tickers':>8}")
    snapshot_keys = stats.snapshot_times
    # Show all if <= 200, otherwise sample evenly
    if len(snapshot_keys) <= 200:
        display_keys = snapshot_keys
    else:
        step = len(snapshot_keys) // 200
        display_keys = snapshot_keys[::step]
    for key in display_keys:
        lines.append(f"{key:<22} {stats.tickers_per_snapshot[key]:>8}")
    lines.append("")

    report = "\n".join(lines)

    os.makedirs(REPORT_DIR, exist_ok=True)
    filename = f"report_{now.strftime('%Y%m%d_%H%M%S')}.txt"
    report_path = os.path.join(REPORT_DIR, filename)
    with open(report_path, "w") as f:
        f.write(report)

    logging.info(f"[SNAPSHOT STATS] Report written to {report_path}")
    print(report)
    return report_path


def run_snapshot_stats(db_path=DEFAULT_SNAPSHOT_DB_PATH):
    """Entry point: collect stats and generate report."""
    stats = collect_snapshot_stats(db_path=db_path)
    report_path = generate_snapshot_report(stats)
    return stats, report_path


if __name__ == "__main__":
    run_snapshot_stats()
