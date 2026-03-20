import os
import logging
import argparse
import subprocess
import time
import pickle
import random
import webbrowser

import requests
from rocksdict import Rdict
from datetime import datetime, timezone, timedelta
from ScannerMinute.src import logging_utils, snapshot_utils
from ScannerMinute.src import polygon_utils
from ScannerMinute.src.quick_download_utils import download_minute_daily_bars
from ScannerMinute.src.ticker_utils import ALL_TICKERS
from ScannerMinute.definitions import PROJECT_ROOT_DIR


DEFAULT_LOOKBACK_MINUTES = [1, 5, 10]
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
    parser.add_argument(
        "--random_inject",
        action="store_true",
        default=False,
        help="inject a random fake breakout when none are detected",
    )
    parser.add_argument(
        "--random_inject_prob",
        type=float,
        default=0.5,
        help="probability of injecting a random breakout per cycle (default 0.5)",
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
            f"{b['ticker']:6s}|{b['lookback_min']:3d}m| "
            f"r={b['ratio']:.3f}|"
            f"${b['past_close']:.2f}->${b['current_close']:.2f}|"
            f"{b['past_time']} | "
            f"https://www.tradingview.com/chart/?symbol={b['ticker']}&interval=1"
        )


def process_items(items):
    """Convert raw API items to a dict of ticker -> Snapshot."""
    snapshots = [snapshot_utils.Snapshot(item) for item in items]
    return {snap["ticker"]: snap for snap in snapshots}


SERVER_URL = "http://127.0.0.1:3000"

# Track tickers we've already sent candle data for this session
_sent_candle_tickers = set()


def fetch_and_post_candles(client, tickers):
    """Fetch today's 1-minute candles for new breakout tickers and POST to server."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for ticker in tickers:
        if ticker in _sent_candle_tickers:
            continue
        try:
            data = polygon_utils.get_ticker_data_from_polygon(
                client, ticker, "minute", today, today
            )
            if not data:
                continue
            # Convert to chart-friendly format: {time (unix), open, high, low, close, volume}
            candles = []
            for row in data:
                # row: [ticker, datetime_utc, open, high, low, close, volume, vwap, timestamp, transactions, otc]
                ts_ms = row[8]  # timestamp in milliseconds
                candles.append(
                    {
                        "time": int(ts_ms / 1000),
                        "open": row[2],
                        "high": row[3],
                        "low": row[4],
                        "close": row[5],
                        "volume": row[6],
                    }
                )
            post_to_server("candles", {"ticker": ticker, "candles": candles})
            _sent_candle_tickers.add(ticker)
            logging.info(f"[candles] Sent {len(candles)} candles for {ticker}")
        except Exception as e:
            logging.warning(f"[candles] Failed to fetch candles for {ticker}: {e}")


DEFAULT_NEWS_HOURS_BACK = 24
DEFAULT_NEWS_LIMIT = 5

# Track tickers we've already sent news for this session
_sent_news_tickers = set()


def fetch_and_post_news(client, tickers, hours_back=DEFAULT_NEWS_HOURS_BACK, limit=DEFAULT_NEWS_LIMIT):
    """Fetch recent news for breakout tickers from Polygon (Benzinga) and POST to server."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    for ticker in tickers:
        if ticker in _sent_news_tickers:
            continue
        try:
            news_iter = client.list_ticker_news(
                ticker=ticker,
                published_utc_gte=cutoff,
                limit=limit,
                sort="published_utc",
                order="desc",
            )
            articles = []
            for article in news_iter:
                # Extract sentiment for this ticker if available
                sentiment = ""
                for insight in (getattr(article, "insights", None) or []):
                    if getattr(insight, "ticker", "") == ticker:
                        sentiment = getattr(insight, "sentiment", "")
                        break
                articles.append({
                    "title": getattr(article, "title", ""),
                    "url": getattr(article, "article_url", ""),
                    "published": getattr(article, "published_utc", ""),
                    "source": getattr(getattr(article, "publisher", None), "name", ""),
                    "sentiment": sentiment,
                })
            if articles:
                post_to_server("news", {"ticker": ticker, "articles": articles})
                _sent_news_tickers.add(ticker)
                logging.info(f"[news] Sent {len(articles)} articles for {ticker}")
        except Exception as e:
            logging.warning(f"[news] Failed to fetch news for {ticker}: {e}")


DEFAULT_VOLUME_LOOKBACK_DAYS = 10
# Trading day in UTC: 09:00 to 24:00 (pre-market + regular + after-hours)
TRADING_START_HOUR_UTC = 9
TRADING_END_HOUR_UTC = 24
TRADING_MINUTES = (TRADING_END_HOUR_UTC - TRADING_START_HOUR_UTC) * 60  # 900 minutes

# Track tickers we've already computed volume analysis for this session
_sent_volume_tickers = set()


def _split_bars_by_day(bars):
    """
    Split minute bars into per-day lists.
    A trading day spans 09:00 UTC to 24:00 UTC.
    bars: list of rows [ticker, datetime_utc, open, high, low, close, volume, vwap, timestamp, ...]
    Returns dict: date_str -> list of bars for that day.
    """
    days = {}
    for bar in bars:
        ts_ms = bar[8]
        dt = datetime.utcfromtimestamp(ts_ms / 1000)
        hour = dt.hour
        if hour < TRADING_START_HOUR_UTC:
            continue
        date_str = dt.strftime("%Y-%m-%d")
        if date_str not in days:
            days[date_str] = []
        days[date_str].append(bar)
    return days


def _bars_to_accumulated_volume(bars, date_str):
    """
    Given bars for a single day, interpolate to minute granularity (09:00-24:00 UTC)
    and return accumulated volume array of length TRADING_MINUTES.
    Each index i corresponds to minute i from 09:00 UTC.
    """
    # Build a dict: minute_offset -> volume
    minute_volumes = {}
    for bar in bars:
        ts_ms = bar[8]
        dt = datetime.utcfromtimestamp(ts_ms / 1000)
        minute_offset = (dt.hour - TRADING_START_HOUR_UTC) * 60 + dt.minute
        if 0 <= minute_offset < TRADING_MINUTES:
            minute_volumes[minute_offset] = (
                minute_volumes.get(minute_offset, 0) + bar[6]
            )

    # Fill accumulated volume array
    accumulated = [0.0] * TRADING_MINUTES
    cumulative = 0.0
    for i in range(TRADING_MINUTES):
        cumulative += minute_volumes.get(i, 0)
        accumulated[i] = cumulative
    return accumulated


def compute_volume_analysis(tickers, volume_lookback_days=DEFAULT_VOLUME_LOOKBACK_DAYS):
    """
    Stage 1: Download last K days of minute bars for breakout tickers.
    Stage 2: Split by day.
    Stage 3: Convert to accumulated volume per day.
    Stage 4: Average accumulated volume across K-1 historical days.
    Stage 5: Compare today's accumulated volume to the average.

    Returns dict: ticker -> volume_pct (float, e.g. 145.0 means 145% of avg)
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start_date = (
        datetime.now(timezone.utc) - timedelta(days=volume_lookback_days + 5)
    ).strftime(
        "%Y-%m-%d"
    )  # extra days buffer for weekends/holidays

    # Stage 1: Download
    new_tickers = [t for t in tickers if t not in _sent_volume_tickers]
    if not new_tickers:
        return {}

    all_bars = download_minute_daily_bars(new_tickers, start_date, today)

    # Current minute offset from 09:00 UTC
    now = datetime.now(timezone.utc)
    current_minute_offset = (now.hour - TRADING_START_HOUR_UTC) * 60 + now.minute
    if current_minute_offset < 0 or current_minute_offset >= TRADING_MINUTES:
        logging.warning(
            f"[volume] Current time {now.strftime('%H:%M')} UTC is outside trading window"
        )
        return {}

    results = {}
    for ticker in new_tickers:
        bars = all_bars.get(ticker, [])
        if not bars:
            continue

        # Stage 2: Split by day
        daily_bars = _split_bars_by_day(bars)

        # Stage 3: Accumulated volume per day
        daily_accumulated = {}
        for date_str, day_bars in daily_bars.items():
            daily_accumulated[date_str] = _bars_to_accumulated_volume(
                day_bars, date_str
            )

        # Separate today vs historical days
        today_acc = daily_accumulated.pop(today, None)
        if today_acc is None:
            continue

        # Keep only the most recent K-1 historical days
        hist_dates = sorted(daily_accumulated.keys(), reverse=True)[
            : volume_lookback_days - 1
        ]
        if not hist_dates:
            continue

        # Stage 4: Average accumulated volume across historical days
        avg_accumulated = [0.0] * TRADING_MINUTES
        for date_str in hist_dates:
            acc = daily_accumulated[date_str]
            for i in range(TRADING_MINUTES):
                avg_accumulated[i] += acc[i]
        for i in range(TRADING_MINUTES):
            avg_accumulated[i] /= len(hist_dates)

        # Stage 5: Compare at current minute
        avg_vol = avg_accumulated[current_minute_offset]
        today_vol = today_acc[current_minute_offset]
        if avg_vol > 0:
            pct = (today_vol / avg_vol) * 100
        else:
            pct = 0.0

        results[ticker] = round(pct, 1)
        _sent_volume_tickers.add(ticker)
        logging.info(
            f"[volume] {ticker}: today={today_vol:.0f} avg={avg_vol:.0f} -> {pct:.1f}% "
            f"(at minute {current_minute_offset}, {len(hist_dates)} hist days)"
        )

    return results


def start_server():
    """Start the Node.js dashboard server and open the browser."""
    server_js = os.path.join(PROJECT_ROOT_DIR, "node_server", "server.js")
    proc = subprocess.Popen(
        ["node", server_js],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    time.sleep(1)
    if proc.poll() is not None:
        stderr = proc.stderr.read().decode()
        raise RuntimeError(f"Node server failed to start: {stderr}")
    logging.info(f"Node dashboard started at {SERVER_URL}")
    webbrowser.open(SERVER_URL)
    return proc


def post_to_server(endpoint: str, data: dict):
    """Post JSON data to the Node server. Silently logs on failure."""
    try:
        requests.post(f"{SERVER_URL}/{endpoint}", json=data, timeout=2)
    except Exception as e:
        logging.warning(f"[post_to_server] Failed to post to /{endpoint}: {e}")


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
    random_inject=False,
    random_inject_prob=0.5,
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

    server_proc = start_server()

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
            # Get snapshot time from AAPL's minute bar timestamp
            snapshot_time = ""
            aapl_snap = snapshots.get("AAPL")
            # print(aapl_snap)
            if aapl_snap:
                snapshot_time = aapl_snap.get("updated_human_utc", "")
            logging.info(
                f"[scan #{scan_count}] Stored {len(snapshots)} tickers at {key_time_utc} | snapshot_time={snapshot_time}"
            )
            post_to_server(
                "scan",
                {
                    "time": key_time_utc,
                    "ticker_count": len(snapshots),
                    "snapshot_time": snapshot_time,
                },
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
            # Inject a random fake breakout if none detected
            if random_inject and not breakouts and random.random() < random_inject_prob:
                ticker = random.choice(ALL_TICKERS)
                fake_breakout = {
                    "ticker": ticker,
                    "lookback_min": random.choice(lookback_minutes),
                    "ratio": round(
                        random.uniform(breakout_threshold, breakout_threshold + 0.1), 4
                    ),
                    "current_close": 0.0,
                    "past_close": 0.0,
                    "current_time": key_time_utc,
                    "past_time": key_time_utc,
                }
                breakouts.append(fake_breakout)
                logging.info(f"[random_inject] Injected fake breakout for {ticker}")

            # Deduplicate: keep only the highest-ratio breakout per ticker
            best = {}
            for b in breakouts:
                t = b["ticker"]
                if t not in best or b["ratio"] > best[t]["ratio"]:
                    best[t] = b
            breakouts = list(best.values())

            log_breakouts(breakouts)
            if breakouts:
                post_to_server(
                    "breakouts", {"time": key_time_utc, "breakouts": breakouts}
                )
                # Fetch and send today's minute candles for breakout tickers
                breakout_tickers = list({b["ticker"] for b in breakouts})
                fetch_and_post_candles(client, breakout_tickers)

                # Fetch and send recent news for breakout tickers
                fetch_and_post_news(client, breakout_tickers)

                # Volume analysis: compare today's volume to historical average
                volume_pcts = compute_volume_analysis(breakout_tickers)
                if volume_pcts:
                    # Add volume_pct to each breakout and post to server
                    for b in breakouts:
                        b["volume_pct"] = volume_pcts.get(b["ticker"])
                    post_to_server(
                        "breakouts", {"time": key_time_utc, "breakouts": breakouts}
                    )
                    for ticker, pct in volume_pcts.items():
                        logging.info(f"[volume] {ticker}: {pct}% of avg volume")

            # Sleep remaining time
            elapsed = time.time() - t0
            sleep_time = max(0, scanning_period_secs - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        logging.info("Scanner stopped by user")
    finally:
        db.close()
        server_proc.terminate()
        logging.info(f"Database closed, server stopped. Total scans: {scan_count}")


if __name__ == "__main__":
    run_realtime()
