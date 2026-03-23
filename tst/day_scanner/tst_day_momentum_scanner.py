import logging
import time
from datetime import datetime, timezone

from ScannerMinute.src import logging_utils
from ScannerMinute.src.ticker_utils import ALL_TICKERS
from ScannerMinute.src.quick_download_utils import (
    download_all_daily_bars,
    fetch_ticker_industries,
)


YEARS_BACK = 5


def count_consecutive_green_days(bars):
    """
    Count consecutive green days going backward from the most recent bar.
    A green day = current close > previous trading day's close.
    Returns (count, daily_changes) where daily_changes is a list of
    (close / prev_close - 1) * 100 percentages from latest to earliest green day.
    """
    if not bars or len(bars) < 2:
        return 0, []

    sorted_bars = sorted(bars, key=lambda b: b["timestamp"])

    count = 0
    daily_changes = []
    # Walk backward from the last bar, comparing to the previous bar's close
    for i in range(len(sorted_bars) - 1, 0, -1):
        cur_close = sorted_bars[i]["close"]
        prev_close = sorted_bars[i - 1]["close"]
        if prev_close and cur_close > prev_close:
            count += 1
            pct = (cur_close / prev_close - 1) * 100
            daily_changes.append(pct)
        else:
            break
    return count, daily_changes


def run_day_momentum_scanner():
    logging_utils.setup_logging(
        log_level="INFO", include_time=True, log_folder="./logs/"
    )

    tickers = ALL_TICKERS
    logging.info(
        f"Downloading grouped daily bars for {len(tickers)} tickers ({YEARS_BACK} years + today)..."
    )

    t0 = time.time()
    all_bars = download_all_daily_bars(tickers, years_back=YEARS_BACK)
    elapsed = time.time() - t0
    logging.info(
        f"Download complete in {elapsed:.1f}s. Got data for {len(all_bars)} tickers."
    )

    # Score each ticker by consecutive green days from today backward
    scores = []
    for ticker in tickers:
        bars = all_bars.get(ticker, [])
        green_days, daily_changes = count_consecutive_green_days(bars)
        scores.append((ticker, green_days, daily_changes))

    # Sort descending by green days, skip 0
    scores = [(t, g, c) for t, g, c in scores if g > 0]
    scores.sort(key=lambda x: x[1], reverse=True)

    # Fetch industry info for tickers with green days
    score_tickers = [t for t, _, _ in scores]
    logging.info(f"Fetching industry info for {len(score_tickers)} tickers...")
    t1 = time.time()
    industries = fetch_ticker_industries(score_tickers)
    logging.info(f"Industry fetch complete in {time.time() - t1:.1f}s")

    # Print ticker scores with daily change percentages
    header = f"{'Rank':>5}  {'Ticker':<8}  {'Days':>5}  {'Industry':<35}  Daily Changes (latest -> earliest)"
    sep = "-" * 120
    print("\n=== Day Momentum Scanner: Consecutive Green Days (0 excluded) ===")
    print(
        "Percentages are close-to-close: (today's close / prev day's close - 1) * 100"
    )
    print(header)
    print(sep)
    for rank, (ticker, green_days, daily_changes) in enumerate(scores, 1):
        avg = sum(daily_changes) / len(daily_changes) if daily_changes else 0
        changes_str = ", ".join(f"{c:.2f}%" for c in daily_changes)
        industry = industries.get(ticker, "")
        tv_link = f"https://www.tradingview.com/chart/?symbol={ticker}&interval=D"
        print(
            f"{rank:>5}  {ticker:<8}  {green_days:>5}  {industry:<35}  [{avg:.2f}%] {changes_str} {tv_link}"
        )

    # Save to file
    output_path = "tst/day_scanner/day_momentum_scores.txt"
    with open(output_path, "w") as f:
        f.write(
            f"Day Momentum Scanner - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n"
        )
        f.write("=== Consecutive Green Days (0 excluded) ===\n")
        f.write(
            "Percentages are close-to-close: (today's close / prev day's close - 1) * 100\n"
        )
        f.write(header + "\n")
        f.write(sep + "\n")
        for rank, (ticker, green_days, daily_changes) in enumerate(scores, 1):
            avg = sum(daily_changes) / len(daily_changes) if daily_changes else 0
            changes_str = ", ".join(f"{c:.2f}%" for c in daily_changes)
            industry = industries.get(ticker, "")
            tv_link = f"https://www.tradingview.com/chart/?symbol={ticker}&interval=D"
            f.write(
                f"{rank:>5}  {ticker:<8}  {green_days:>5}  {industry:<35}  [{avg:.2f}%] {changes_str}  {tv_link}\n"
            )

    logging.info(f"Scores saved to {output_path}")


if __name__ == "__main__":
    run_day_momentum_scanner()
