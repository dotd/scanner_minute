import logging
import os
import random
import time
from datetime import datetime, timedelta

from ScannerMinute.src import logging_utils
from ScannerMinute.src.polygon_utils import (
    get_polygon_client,
    get_all_tickers_from_snapshot,
    get_trading_days,
    get_ticker_data_from_polygon,
)
from ScannerMinute.src.rocksdict_utils import read_bars


DB_PATH = "./data/ver2/"


def verify_data(k=100, db_path=DB_PATH, seed=42):
    """
    Verify downloaded data by randomly sampling K tickers and K days,
    then comparing fresh API downloads against what's stored in the DB.

    For each (ticker, day) pair, downloads minute bars from Polygon and
    reads the same range from RocksDB, then checks they are identical.
    """
    logging_utils.setup_logging(
        log_level="INFO",
        log_folder=f"{os.path.dirname(os.path.abspath(__file__))}/logs/",
        include_time=True,
    )

    client = get_polygon_client()

    # Get all tickers and trading days for the same period as download_data
    all_tickers = get_all_tickers_from_snapshot(client)
    date_start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    date_end = datetime.now().strftime("%Y-%m-%d")
    trading_days = get_trading_days(client, from_=date_start, to=date_end)

    # Sample K random (ticker, day) pairs
    rng = random.Random(seed)
    sampled_tickers = rng.choices(all_tickers, k=k)
    sampled_days = rng.choices(trading_days, k=k)
    pairs = list(zip(sampled_tickers, sampled_days))

    logging.info(f"[VERIFY] Sampled {len(pairs)} (ticker, day) pairs to verify")

    total_checks = 0
    passed = 0
    failed = 0
    skipped = 0
    mismatches = []
    cumul_identical = 0
    cumul_intersection = 0
    cumul_union = 0

    t0 = time.time()

    for ticker, day in pairs:
        total_checks += 1
        status = "SKIP"

        # Fresh download from API
        try:
            api_bars = get_ticker_data_from_polygon(client, ticker, "minute", day, day)
        except Exception as e:
            logging.warning(f"[VERIFY] API error for {ticker} {day}: {e}")
            skipped += 1
            status = "SKIP"
            elapsed = time.time() - t0
            avg = elapsed / total_checks
            eta = avg * (len(pairs) - total_checks)
            logging.info(
                f"[VERIFY] {total_checks}/{len(pairs)} | {ticker:<6s} {day} | {status} | "
                f"passed={passed} failed={failed} skipped={skipped} | "
                f"elapsed={elapsed:.1f}s eta={eta:.1f}s"
            )
            continue

        # Read from DB
        db_bars = read_bars(db_path, "minute", [ticker], f"{day}T00:00:00", f"{day}T23:59:59")

        # Compare by timestamp (index 8)
        api_by_ts = {bar[8]: bar for bar in api_bars}
        db_by_ts = {bar[8]: bar for bar in db_bars}

        # IoU: intersection over union of timestamps
        intersection = api_by_ts.keys() & db_by_ts.keys()
        union = api_by_ts.keys() | db_by_ts.keys()
        # Among shared timestamps, count how many have identical values
        identical = sum(1 for ts in intersection if api_by_ts[ts] == db_by_ts[ts])
        iou_pct = (identical / len(union) * 100) if union else 100.0

        cumul_identical += identical
        cumul_intersection += len(intersection)
        cumul_union += len(union)
        cumul_iou_pct = (cumul_identical / cumul_union * 100) if cumul_union else 100.0

        if iou_pct == 100.0:
            passed += 1
            status = "OK"
        else:
            failed += 1
            status = "FAIL"
            mismatches.append({
                "ticker": ticker,
                "day": day,
                "api_count": len(api_bars),
                "db_count": len(db_bars),
                "intersection": len(intersection),
                "union": len(union),
                "identical": identical,
                "iou_pct": iou_pct,
            })

        elapsed = time.time() - t0
        avg = elapsed / total_checks
        eta = avg * (len(pairs) - total_checks)
        logging.info(
            f"[VERIFY] {total_checks}/{len(pairs)} | {ticker:<6s} {day} | {status} | "
            f"IoU={iou_pct:.1f}% ({identical}/{len(union)}) | "
            f"cumul: identical={cumul_identical:,} intersect={cumul_intersection:,} union={cumul_union:,} IoU={cumul_iou_pct:.1f}% | "
            f"passed={passed} failed={failed} skipped={skipped} | "
            f"elapsed={elapsed:.1f}s eta={eta:.1f}s"
        )

    # Summary
    pass_rate = (passed / total_checks * 100) if total_checks else 0
    logging.info(f"[VERIFY] {'=' * 60}")
    logging.info(f"[VERIFY] Total checks:  {total_checks}")
    logging.info(f"[VERIFY] Passed:        {passed} ({pass_rate:.1f}%)")
    logging.info(f"[VERIFY] Failed:        {failed}")
    logging.info(f"[VERIFY] Skipped (err): {skipped}")
    total_iou_pct = (cumul_identical / cumul_union * 100) if cumul_union else 100.0
    logging.info(f"[VERIFY] Samples:       identical={cumul_identical:,} intersect={cumul_intersection:,} union={cumul_union:,}")
    logging.info(f"[VERIFY] Total IoU:     {total_iou_pct:.2f}% ({cumul_identical:,}/{cumul_union:,})")
    logging.info(f"[VERIFY] {'=' * 60}")

    if mismatches:
        logging.info(f"[VERIFY] All mismatches ({len(mismatches)}):")
        logging.info(f"[VERIFY] {'#':>4}  {'Ticker':<8}  {'Day':<12}  {'API':>6}  {'DB':>6}  {'Intersect':>9}  {'Union':>6}  {'Identical':>9}  {'IoU':>7}")
        logging.info(f"[VERIFY] {'-' * 80}")
        for i, m in enumerate(mismatches, 1):
            logging.info(
                f"[VERIFY] {i:4d}  {m['ticker']:<8}  {m['day']:<12}  "
                f"{m['api_count']:6d}  {m['db_count']:6d}  "
                f"{m['intersection']:9d}  {m['union']:6d}  "
                f"{m['identical']:9d}  {m['iou_pct']:6.1f}%"
            )

    return passed, failed, skipped, mismatches


if __name__ == "__main__":
    verify_data()
