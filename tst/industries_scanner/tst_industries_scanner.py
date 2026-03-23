import logging
import math
import time
from datetime import datetime, timezone
from collections import defaultdict

from ScannerMinute.src import logging_utils
from ScannerMinute.src.ticker_utils import SP500_AND_NASDAQ100_TICKERS
from ScannerMinute.src.quick_download_utils import (
    download_all_daily_bars,
    fetch_ticker_industries,
)


K = 5  # lookback days
N_YEARS = 4  # ~1000 trading days
M = 5  # top stocks per industry


def compute_k_day_return(bars, k):
    """
    Compute the return over the last k trading days.
    Return = (last_close / close_k_days_ago - 1) * 100
    Returns None if not enough data.
    """
    sorted_bars = sorted(bars, key=lambda b: b["timestamp"])
    if len(sorted_bars) < k + 1:
        return None
    close_now = sorted_bars[-1]["close"]
    close_k_ago = sorted_bars[-(k + 1)]["close"]
    if not close_k_ago or close_k_ago == 0:
        return None
    return (close_now / close_k_ago - 1) * 100


def run_industries_scanner(k=K, m=M):
    logging_utils.setup_logging(
        log_level="INFO", include_time=True, log_folder="./logs/"
    )

    tickers = SP500_AND_NASDAQ100_TICKERS
    logging.info(
        f"Industries Scanner: {len(tickers)} tickers, K={k} days, top M={m} stocks per group"
    )

    # Stage 1: Download daily bars and industry info
    logging.info("=== Stage 1: Download daily bars ===")
    t0 = time.time()
    all_bars = download_all_daily_bars(tickers, years_back=N_YEARS)
    logging.info(
        f"Download complete in {time.time() - t0:.1f}s. Got data for {len(all_bars)} tickers."
    )

    logging.info("=== Stage 1b: Fetch industries ===")
    industries = fetch_ticker_industries(tickers)

    # Stage 2: Group tickers by industry
    logging.info("=== Stage 2: Group by industry ===")
    industry_groups = defaultdict(list)
    for ticker in tickers:
        industry = industries.get(ticker, "")
        if industry:
            industry_groups[industry].append(ticker)
        else:
            industry_groups["Unknown"].append(ticker)
    logging.info(f"Found {len(industry_groups)} industry groups")

    # Stage 3: Compute K-day return for each ticker, then average per industry
    logging.info(f"=== Stage 3: Compute {k}-day returns ===")
    industry_stats = {}
    for industry, group_tickers in industry_groups.items():
        ticker_returns = []
        for ticker in group_tickers:
            bars = all_bars.get(ticker, [])
            ret = compute_k_day_return(bars, k)
            if ret is not None:
                ticker_returns.append((ticker, ret))

        if not ticker_returns:
            continue

        returns = [r for _, r in ticker_returns]
        avg = sum(returns) / len(returns)
        std = (
            math.sqrt(sum((r - avg) ** 2 for r in returns) / len(returns))
            if len(returns) > 1
            else 0.0
        )
        # Sort tickers by return descending
        ticker_returns.sort(key=lambda x: x[1], reverse=True)
        industry_stats[industry] = {
            "avg": avg,
            "std": std,
            "count": len(ticker_returns),
            "top": ticker_returns[:m],
            "all": ticker_returns,
        }

    # Stage 4: Rank industries by average return
    logging.info(f"=== Stage 4: Rank industries ===")
    ranked = sorted(industry_stats.items(), key=lambda x: x[1]["avg"], reverse=True)

    # Print results
    header = f"=== Industry Rankings: {k}-Day Performance ==="
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    sub_header = (
        f"Date: {date_str} | Tickers: {len(tickers)} | Industries: {len(ranked)}"
    )

    print(f"\n{header}")
    print(sub_header)
    print("-" * 120)
    for rank, (industry, stats) in enumerate(ranked, 1):
        top_str = ", ".join(f"{t} {r:+.2f}%" for t, r in stats["top"])
        line = (
            f"{rank:3d}. {industry:<40s}  avg={stats['avg']:+7.2f}%  "
            f"std={stats['std']:5.2f}%  ({stats['count']:3d} stocks)  "
            f"Top: {top_str}"
        )
        print(line)
        logging.info(line)

    # Stage 5: Save to file
    output_path = "tst/industries_scanner/industries_ranking.txt"
    with open(output_path, "w") as f:
        f.write(f"Industry Rankings - {date_str}\n")
        f.write(f"Tickers: {len(tickers)} | Industries: {len(ranked)}\n\n")

        f.write(f"{'=' * 120}\n")
        f.write(f"Period: {k}-Day Performance\n")
        f.write(f"{'=' * 120}\n\n")

        col_header = (
            f"{'Rank':>4}  {'Industry':<40s}  {'Avg':>8}  {'Std':>7}  "
            f"{'Count':>5}  Top {m} Stocks"
        )
        f.write(col_header + "\n")
        f.write("-" * 120 + "\n")

        for rank, (industry, stats) in enumerate(ranked, 1):
            top_str = ", ".join(f"{t} {r:+.2f}%" for t, r in stats["top"])
            f.write(
                f"{rank:4d}  {industry:<40s}  {stats['avg']:+7.2f}%  "
                f"{stats['std']:5.2f}%  {stats['count']:5d}  {top_str}\n"
            )

        # Detailed breakdown per industry
        f.write(f"\n\n{'=' * 120}\n")
        f.write(f"Detailed Breakdown: {k}-Day Performance by Industry\n")
        f.write(f"{'=' * 120}\n\n")

        for rank, (industry, stats) in enumerate(ranked, 1):
            f.write(
                f"{rank}. {industry} (avg={stats['avg']:+.2f}%, "
                f"std={stats['std']:.2f}%, {stats['count']} stocks)\n"
            )
            for ticker, ret in stats["all"]:
                f.write(f"    {ticker:<8s}  {ret:+7.2f}%\n")
            f.write("\n")

    logging.info(f"Results saved to {output_path}")


if __name__ == "__main__":
    run_industries_scanner()
