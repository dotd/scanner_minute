"""
Test: download AAPL and NVDA minute data in two phases, with stats after each.

Phase 1: Download from 2 years ago to 1 year ago.
Phase 2: Complete the DB by downloading from 1 year ago to today.
"""

import logging
import shutil
from datetime import datetime, timedelta, timezone

from ScannerMinute.src.logging_utils import setup_logging
from ScannerMinute.src.polygon_utils import get_polygon_client, get_ticker_data_from_polygon
from ScannerMinute.src.rocksdict_utils import read_bars
from ScannerMinute.data_handling_ver2.download_data import download_data
from ScannerMinute.data_handling_ver2.examine_data import collect_stats


def phase1_download(tickers, date_start, date_end, db_path):
    logging.info(f"{'='*60}")
    logging.info(f"PHASE 1: Downloading {date_start} to {date_end}")
    logging.info(f"{'='*60}")
    download_data(
        date_start=date_start,
        date_end=date_end,
        tickers=tickers,
        db_path=db_path,
    )


def phase1_stats(db_path):
    logging.info(f"\n{'='*60}")
    logging.info("PHASE 1 STATS")
    logging.info(f"{'='*60}")
    stats = collect_stats(db_path=db_path)
    logging.info(
        f"Tickers: {stats.total_tickers}, "
        f"Data points: {stats.total_data_points:,}, "
        f"Range: {stats.most_common_start[0][0]} {stats.most_common_start[0][1]} → "
        f"{stats.most_common_end[0][0]} {stats.most_common_end[0][1]}"
    )
    for ticker, s in sorted(stats.ticker_stats.items()):
        logging.info(
            f"  {ticker}: {s['count']:,} bars, {s['first_time']} → {s['last_time']}"
        )
    return stats


def phase2_download(tickers, date_start, date_end, db_path):
    logging.info(f"\n{'='*60}")
    logging.info(f"PHASE 2: Completing DB from {date_start} to {date_end}")
    logging.info(f"{'='*60}")
    download_data(
        date_start=date_start,
        date_end=date_end,
        tickers=tickers,
        db_path=db_path,
    )


def phase2_stats(db_path, stats1):
    logging.info(f"\n{'='*60}")
    logging.info("PHASE 2 STATS (complete)")
    logging.info(f"{'='*60}")
    stats = collect_stats(db_path=db_path)
    logging.info(
        f"Tickers: {stats.total_tickers}, "
        f"Data points: {stats.total_data_points:,}, "
        f"Range: {stats.most_common_start[0][0]} {stats.most_common_start[0][1]} → "
        f"{stats.most_common_end[0][0]} {stats.most_common_end[0][1]}"
    )
    for ticker, s in sorted(stats.ticker_stats.items()):
        logging.info(
            f"  {ticker}: {s['count']:,} bars, {s['first_time']} → {s['last_time']}"
        )

    added = stats.total_data_points - stats1.total_data_points
    logging.info(
        f"\nPhase 2 added {added:,} data points ({stats1.total_data_points:,} → {stats.total_data_points:,})"
    )
    return stats


def phase3_verify(db_path):
    from ScannerMinute.src.polygon_utils import COLUMNS

    logging.info(f"\n{'='*60}")
    logging.info("PHASE 3: Verify AAPL 2026-04-09 — DB vs fresh API download")
    logging.info(f"{'='*60}")

    day = "2026-04-09"

    # Load from DB
    db_bars = read_bars(db_path, "minute", ["AAPL"], f"{day}T00:00:00", f"{day}T23:59:59")

    # Download fresh from Polygon
    client = get_polygon_client()
    api_bars = get_ticker_data_from_polygon(client, "AAPL", "minute", day, day)

    # Index by timestamp (index 8)
    api_by_ts = {bar[8]: bar for bar in api_bars}
    db_by_ts = {bar[8]: bar for bar in db_bars}

    intersection = api_by_ts.keys() & db_by_ts.keys()
    union = api_by_ts.keys() | db_by_ts.keys()

    logging.info(f"  DB bars:      {len(db_bars)}")
    logging.info(f"  API bars:     {len(api_bars)}")
    logging.info(f"  Intersection: {len(intersection)}")
    logging.info(f"  Union:        {len(union)}")

    # Per-column comparison across shared timestamps
    compare_cols = ["open", "high", "low", "close", "volume", "vwap", "transactions", "otc"]
    col_indices = {col: COLUMNS.index(col) for col in compare_cols}

    col_match = {col: 0 for col in compare_cols}
    all_cols_match = 0

    for ts in intersection:
        api_bar = api_by_ts[ts]
        db_bar = db_by_ts[ts]
        row_all_match = True
        for col in compare_cols:
            idx = col_indices[col]
            if api_bar[idx] == db_bar[idx]:
                col_match[col] += 1
            else:
                row_all_match = False
        if row_all_match:
            all_cols_match += 1

    n = len(intersection) if intersection else 1
    logging.info("")
    logging.info(f"  {'Column':<14} {'Match':>6} / {n:<6} {'Pct':>7}")
    logging.info(f"  {'-'*14} {'-'*6}   {'-'*6} {'-'*7}")
    for col in compare_cols:
        pct = col_match[col] / n * 100
        logging.info(f"  {col:<14} {col_match[col]:>6} / {n:<6} {pct:>6.1f}%")
    logging.info(f"  {'-'*14} {'-'*6}   {'-'*6} {'-'*7}")
    all_pct = all_cols_match / n * 100
    logging.info(f"  {'ALL COLUMNS':<14} {all_cols_match:>6} / {n:<6} {all_pct:>6.1f}%")

    iou_pct = (all_cols_match / len(union) * 100) if union else 100.0
    status = "PASS" if iou_pct == 100.0 else "FAIL"
    logging.info(f"\n  IoU (all cols): {iou_pct:.1f}%")
    logging.info(f"  Result:         {status}")
    return iou_pct


def run():
    setup_logging(log_level="INFO", include_time=True)

    # Preparation
    DB_PATH = "./tmp/data_handling_ver2/"
    TICKERS = ["AAPL", "NVDA"]
    two_years_ago = (datetime.now(timezone.utc) - timedelta(days=730)).strftime(
        "%Y-%m-%d"
    )
    one_year_ago = (datetime.now(timezone.utc) - timedelta(days=365)).strftime(
        "%Y-%m-%d"
    )
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    shutil.rmtree(DB_PATH, ignore_errors=True)

    phase1_download(TICKERS, two_years_ago, one_year_ago, DB_PATH)
    stats1 = phase1_stats(DB_PATH)
    phase2_download(TICKERS, one_year_ago, today, DB_PATH)
    stats2 = phase2_stats(DB_PATH, stats1)
    phase3_verify(DB_PATH)


if __name__ == "__main__":
    run()
