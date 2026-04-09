"""
Test: download AAPL and NVDA minute data in two phases, with stats after each.

Phase 1: Download from 2 years ago to 1 year ago.
Phase 2: Complete the DB by downloading from 1 year ago to today.
"""
import logging
import shutil
from datetime import datetime, timedelta, timezone

from ScannerMinute.src.logging_utils import setup_logging
from ScannerMinute.data_handling_ver2.download_data import download_data
from ScannerMinute.data_handling_ver2.examine_data import collect_stats

DB_PATH = "./tmp/data_handling_ver2/"
TICKERS = ["AAPL", "NVDA"]

two_years_ago = (datetime.now(timezone.utc) - timedelta(days=730)).strftime("%Y-%m-%d")
one_year_ago = (datetime.now(timezone.utc) - timedelta(days=365)).strftime("%Y-%m-%d")
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")


def run():
    setup_logging(log_level="INFO", include_time=True)

    # Clean up any previous test data
    shutil.rmtree(DB_PATH, ignore_errors=True)

    # Phase 1: download 2 years ago → 1 year ago
    logging.info(f"{'='*60}")
    logging.info(f"PHASE 1: Downloading {two_years_ago} to {one_year_ago}")
    logging.info(f"{'='*60}")
    download_data(
        date_start=two_years_ago,
        date_end=one_year_ago,
        tickers=TICKERS,
        db_path=DB_PATH,
    )

    # Stats after phase 1
    logging.info(f"\n{'='*60}")
    logging.info("PHASE 1 STATS")
    logging.info(f"{'='*60}")
    stats1 = collect_stats(db_path=DB_PATH)
    logging.info(
        f"Tickers: {stats1.total_tickers}, "
        f"Data points: {stats1.total_data_points:,}, "
        f"Range: {stats1.most_common_start[0][0]} {stats1.most_common_start[0][1]} → "
        f"{stats1.most_common_end[0][0]} {stats1.most_common_end[0][1]}"
    )
    for ticker, s in sorted(stats1.ticker_stats.items()):
        logging.info(f"  {ticker}: {s['count']:,} bars, {s['first_time']} → {s['last_time']}")

    # Phase 2: complete the DB from 1 year ago → today
    logging.info(f"\n{'='*60}")
    logging.info(f"PHASE 2: Completing DB from {one_year_ago} to {today}")
    logging.info(f"{'='*60}")
    download_data(
        date_start=one_year_ago,
        date_end=today,
        tickers=TICKERS,
        db_path=DB_PATH,
    )

    # Stats after phase 2
    logging.info(f"\n{'='*60}")
    logging.info("PHASE 2 STATS (complete)")
    logging.info(f"{'='*60}")
    stats2 = collect_stats(db_path=DB_PATH)
    logging.info(
        f"Tickers: {stats2.total_tickers}, "
        f"Data points: {stats2.total_data_points:,}, "
        f"Range: {stats2.most_common_start[0][0]} {stats2.most_common_start[0][1]} → "
        f"{stats2.most_common_end[0][0]} {stats2.most_common_end[0][1]}"
    )
    for ticker, s in sorted(stats2.ticker_stats.items()):
        logging.info(f"  {ticker}: {s['count']:,} bars, {s['first_time']} → {s['last_time']}")

    added = stats2.total_data_points - stats1.total_data_points
    logging.info(f"\nPhase 2 added {added:,} data points ({stats1.total_data_points:,} → {stats2.total_data_points:,})")


if __name__ == "__main__":
    run()
