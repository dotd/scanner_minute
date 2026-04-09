import glob
import logging
import os
import pickle
from collections import Counter
from dataclasses import dataclass
from datetime import datetime

from rocksdict import Rdict, AccessType
from tqdm import tqdm

from ScannerMinute.src import logging_utils
from ScannerMinute.definitions import PROJECT_ROOT_DIR, SEPARATOR


DB_PATH = "./data/ver2/"


@dataclass
class ExamineStats:
    total_tickers: int
    total_data_points: int
    min_points_per_ticker: int
    max_points_per_ticker: int
    avg_points_per_ticker: float
    ticker_stats: dict
    start_datetime_counts: Counter  # keyed by (date, time)
    end_datetime_counts: Counter  # keyed by (date, time)
    date_range_counts: (
        Counter  # keyed by ((start_date, start_time), (end_date, end_time))
    )
    most_common_start: tuple  # ((date, time), count)
    most_common_end: tuple  # ((date, time), count)
    most_common_date_range: (
        tuple  # (((start_date, start_time), (end_date, end_time)), count)
    )


def collect_stats(db_path=DB_PATH, timespan="minute", limit_tickers=None):
    logging_utils.setup_logging(
        log_level="INFO",
        log_folder=f"{os.path.dirname(os.path.abspath(__file__))}/logs/",
        include_time=True,
    )

    limit_msg = f", limit_tickers={limit_tickers}" if limit_tickers else ""
    logging.info(
        f"[EXAMINE] Scanning DB at {db_path} for timespan={timespan}{limit_msg}"
    )

    ticker_stats = {}
    db = Rdict(db_path, access_type=AccessType.read_only())
    timespan_prefix = f"{timespan}{SEPARATOR}"

    it = db.iter()
    it.seek(timespan_prefix)

    scanned = 0
    pbar = tqdm(desc="Scanning DB keys", unit=" keys")
    while it.valid():
        key = it.key()
        if not key.startswith(timespan_prefix):
            break

        rest = key[len(timespan_prefix) :]
        sep_pos = rest.find(SEPARATOR)
        if sep_pos == -1:
            it.next()
            continue
        ticker = rest[:sep_pos]
        timestamp = rest[sep_pos + 1 :]

        if ticker not in ticker_stats:
            if limit_tickers and len(ticker_stats) >= limit_tickers:
                break
            ticker_stats[ticker] = {
                "count": 0,
                "first_time": timestamp,
                "last_time": timestamp,
            }
        s = ticker_stats[ticker]
        s["count"] += 1
        s["last_time"] = timestamp

        scanned += 1
        if scanned % 10000 == 0:
            pbar.update(10000)
        it.next()

    pbar.update(scanned % 10000)
    pbar.close()
    db.close()

    logging.info(
        f"[EXAMINE] Scanned {scanned:,} keys, found {len(ticker_stats)} tickers"
    )

    start_datetime_counts = Counter()
    end_datetime_counts = Counter()
    date_range_counts = Counter()
    for s in ticker_stats.values():
        start_dt = (s["first_time"][:10], s["first_time"][11:])
        end_dt = (s["last_time"][:10], s["last_time"][11:])
        start_datetime_counts[start_dt] += 1
        end_datetime_counts[end_dt] += 1
        date_range_counts[(start_dt, end_dt)] += 1

    counts = [s["count"] for s in ticker_stats.values()] if ticker_stats else [0]

    return ExamineStats(
        total_tickers=len(ticker_stats),
        total_data_points=sum(counts),
        min_points_per_ticker=min(counts),
        max_points_per_ticker=max(counts),
        avg_points_per_ticker=sum(counts) / len(counts),
        ticker_stats=ticker_stats,
        start_datetime_counts=start_datetime_counts,
        end_datetime_counts=end_datetime_counts,
        date_range_counts=date_range_counts,
        most_common_start=(
            start_datetime_counts.most_common(1)[0]
            if start_datetime_counts
            else (None, 0)
        ),
        most_common_end=(
            end_datetime_counts.most_common(1)[0] if end_datetime_counts else (None, 0)
        ),
        most_common_date_range=(
            date_range_counts.most_common(1)[0] if date_range_counts else (None, 0)
        ),
    )


def examine_data(stats):
    lines = []
    now = datetime.now()
    lines.append(f"Examine Data Report — {now.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 70)
    lines.append("")

    lines.append("SUMMARY")
    lines.append("-" * 70)
    lines.append(f"Total tickers in DB:         {stats.total_tickers}")
    if stats.ticker_stats:
        lines.append(f"Total data points:           {stats.total_data_points:,}")
        lines.append(f"Min points per ticker:       {stats.min_points_per_ticker:,}")
        lines.append(f"Max points per ticker:       {stats.max_points_per_ticker:,}")
        lines.append(f"Avg points per ticker:       {stats.avg_points_per_ticker:,.1f}")
        mc_s = stats.most_common_start
        lines.append(
            f"Most common start datetime:  {mc_s[0][0]} {mc_s[0][1]} ({mc_s[1]} tickers)"
        )
        mc_e = stats.most_common_end
        lines.append(
            f"Most common end datetime:    {mc_e[0][0]} {mc_e[0][1]} ({mc_e[1]} tickers)"
        )
        mc_r = stats.most_common_date_range
        lines.append(
            f"Most common date range:      {mc_r[0][0][0]} {mc_r[0][0][1]} → {mc_r[0][1][0]} {mc_r[0][1][1]} ({mc_r[1]} tickers)"
        )
    lines.append("")

    # Per-ticker table (sorted by ticker)
    lines.append("PER-TICKER STATISTICS")
    lines.append("-" * 70)
    lines.append(f"{'Ticker':<10} {'Count':>10} {'Start':>20} {'End':>20}")
    lines.append(f"{'-'*10} {'-'*10} {'-'*20} {'-'*20}")
    for ticker in sorted(stats.ticker_stats.keys()):
        s = stats.ticker_stats[ticker]
        lines.append(
            f"{ticker:<10} {s['count']:>10,} {s['first_time']:>20} {s['last_time']:>20}"
        )
    lines.append("")

    # Start datetime histogram
    lines.append("START DATETIME HISTOGRAM")
    lines.append("-" * 70)
    lines.append(f"{'Date':<12} {'Time':<10} {'Tickers':>8}")
    for date, time_ in sorted(stats.start_datetime_counts.keys()):
        lines.append(
            f"{date:<12} {time_:<10} {stats.start_datetime_counts[(date, time_)]:>8}"
        )
    lines.append("")

    # End datetime histogram
    lines.append("END DATETIME HISTOGRAM")
    lines.append("-" * 70)
    lines.append(f"{'Date':<12} {'Time':<10} {'Tickers':>8}")
    for date, time_ in sorted(stats.end_datetime_counts.keys()):
        lines.append(
            f"{date:<12} {time_:<10} {stats.end_datetime_counts[(date, time_)]:>8}"
        )
    lines.append("")

    # Date range histogram
    lines.append("DATE RANGE HISTOGRAM")
    lines.append("-" * 70)
    lines.append(
        f"{'Start Date':<12} {'Start Time':<12} {'End Date':<12} {'End Time':<12} {'Tickers':>8}"
    )
    for start, end in sorted(stats.date_range_counts.keys()):
        lines.append(
            f"{start[0]:<12} {start[1]:<12} {end[0]:<12} {end[1]:<12} {stats.date_range_counts[(start, end)]:>8}"
        )
    lines.append("")

    report = "\n".join(lines)

    # Write report to file
    report_dir = os.path.join(PROJECT_ROOT_DIR, "reports", "ver2")
    os.makedirs(report_dir, exist_ok=True)
    filename = f"report_{now.strftime('%Y%m%d_%H%M%S')}.txt"
    report_path = os.path.join(report_dir, filename)
    with open(report_path, "w") as f:
        f.write(report)

    logging.info(f"[EXAMINE] Report written to {report_path}")
    return report_path


def run_pipeline_get_stats(db_path=DB_PATH, timespan="minute", limit_tickers=None):
    report_dir = os.path.join(PROJECT_ROOT_DIR, "reports", "ver2")
    os.makedirs(report_dir, exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")

    # Check for existing stats pickle from today
    existing_pkl = sorted(glob.glob(os.path.join(report_dir, f"stats_{today}_*.pkl")))
    if existing_pkl:
        pkl_path = existing_pkl[-1]
        logging.info(f"[PIPELINE] Loading cached stats from {pkl_path}")
        with open(pkl_path, "rb") as f:
            stats = pickle.load(f)
        # Check for existing report too
        existing_rpt = sorted(
            glob.glob(os.path.join(report_dir, f"report_{today}_*.txt"))
        )
        if existing_rpt:
            report_path = existing_rpt[-1]
            logging.info(f"[PIPELINE] Report already exists: {report_path}")
        else:
            report_path = examine_data(stats)
        return stats, report_path

    # Compute fresh stats
    import time

    t0 = time.time()
    stats = collect_stats(
        db_path=db_path, timespan=timespan, limit_tickers=limit_tickers
    )
    elapsed = time.time() - t0

    # Save stats pickle
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    pkl_path = os.path.join(report_dir, f"stats_{now}.pkl")
    with open(pkl_path, "wb") as f:
        pickle.dump(stats, f)
    logging.info(f"[PIPELINE] Stats saved to {pkl_path}")

    report_path = examine_data(stats)

    mc = stats.most_common_date_range
    logging.info(
        f"[PIPELINE] {stats.total_tickers} tickers, {stats.total_data_points:,} data points, "
        f"computed in {elapsed:.1f}s | "
        f"top date range: {mc[0][0][0]} {mc[0][0][1]} → {mc[0][1][0]} {mc[0][1][1]} ({mc[1]} tickers)"
    )

    return stats, report_path


if __name__ == "__main__":
    run_pipeline_get_stats(limit_tickers=1000)
