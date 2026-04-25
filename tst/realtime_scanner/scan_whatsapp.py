"""
Minimal WhatsApp-alerting breakout scanner.

Reuses the snapshot + breakout detection logic from tst_scan_realtime.py but:
- Skips the Node.js dashboard, candles, volume analysis, and news fetch.
- Posts each detected breakout directly to the local WhatsApp bridge
  (which forwards to a group chat).

Run:
    cd /Users/dotansbot/scanner_minute
    source .venv/bin/activate
    python -m tst.realtime_scanner.scan_whatsapp
"""
import os
import sys
import time
import json
import logging
import argparse
import urllib.request
from datetime import datetime, timezone

from rocksdict import Rdict

from ScannerMinute.src import snapshot_utils, polygon_utils, logging_utils
from ScannerMinute.definitions import PROJECT_ROOT_DIR

# Reuse detection helpers from the existing scanner
from tst.realtime_scanner.tst_scan_realtime import (
    store_snapshot,
    scan_breakouts,
    process_items,
    DEFAULT_LOOKBACK_MINUTES,
    DEFAULT_BREAKOUT_THRESHOLD,
    DEFAULT_MIN_PRICE,
    DEFAULT_MAX_PRICE,
)

# ---- WhatsApp delivery config ----
WA_BRIDGE_URL = os.environ.get("WA_BRIDGE_URL", "http://127.0.0.1:3000/send")
WA_CHAT_ID = os.environ.get("WA_CHAT_ID", "120363426345321664@g.us")
# Optional: comma-separated list of extra chat IDs to also receive alerts.
_extra = os.environ.get("WA_CHAT_IDS", "")
WA_CHAT_IDS = [c.strip() for c in (WA_CHAT_ID + "," + _extra).split(",") if c.strip()]
# Dedupe while preserving order.
WA_CHAT_IDS = list(dict.fromkeys(WA_CHAT_IDS))

DEFAULT_ROCKSDICT_PATH = f"{PROJECT_ROOT_DIR}/data/rocksdict_snapshots_wa"

# Ticker details cache (ticker -> {"name","mcap","desc1"}) so we hit Polygon
# at most once per ticker per process.
_details_cache: dict = {}


def _first_sentence(text: str) -> str:
    if not text:
        return ""
    text = text.strip().replace("\n", " ")
    # Stop at the first period+space, but keep at least 20 chars.
    for i in range(20, len(text)):
        if text[i] == "." and (i + 1 == len(text) or text[i + 1] == " "):
            return text[: i + 1]
    return text[:180] + ("…" if len(text) > 180 else "")


def _fmt_mcap(mcap) -> str:
    if not mcap:
        return "n/a"
    mcap = float(mcap)
    for unit, scale in (("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if mcap >= scale:
            return f"${mcap/scale:.2f}{unit}"
    return f"${mcap:.0f}"


def get_ticker_details(client, ticker: str) -> dict:
    cached = _details_cache.get(ticker)
    if cached is not None:
        return cached
    info = {"name": "", "mcap": None, "desc1": ""}
    try:
        d = client.get_ticker_details(ticker)
        info["name"] = getattr(d, "name", "") or ""
        info["mcap"] = getattr(d, "market_cap", None)
        info["desc1"] = _first_sentence(getattr(d, "description", "") or "")
    except Exception as e:
        logging.warning(f"[details] {ticker}: {e}")
    _details_cache[ticker] = info
    return info


def get_latest_news_today(client, ticker: str):
    """Return the most recent news article published today (UTC) or None."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        for article in client.list_ticker_news(
            ticker=ticker, limit=5, sort="published_utc", order="desc"
        ):
            pub = getattr(article, "published_utc", "") or ""
            if not pub.startswith(today):
                return None  # newest is already older than today
            title = getattr(article, "title", "") or ""
            sentiment = ""
            for ins in getattr(article, "insights", None) or []:
                if getattr(ins, "ticker", "") == ticker:
                    sentiment = getattr(ins, "sentiment", "") or ""
                    break
            return {"title": title, "sentiment": sentiment, "published": pub}
    except Exception as e:
        logging.warning(f"[news] {ticker}: {e}")
    return None


def send_whatsapp(message: str) -> None:
    """POST a plain-text message to the local WhatsApp bridge for each configured chat."""
    for chat_id in WA_CHAT_IDS:
        body = json.dumps({"chatId": chat_id, "message": message}).encode()
        req = urllib.request.Request(
            WA_BRIDGE_URL,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                resp.read()
        except Exception as e:
            logging.warning(f"[whatsapp] send failed ({chat_id}): {e}")


def format_alert(b: dict, details=None, news=None) -> str:
    ratio = b["ratio"]
    pct = (ratio - 1.0) * 100.0
    tv = f"https://www.tradingview.com/chart/?symbol={b['ticker']}&interval=1"
    lines = [
        f"🚀 {b['ticker']}  +{pct:.2f}% / {b['lookback_min']}m",
        f"${b['past_close']:.2f} → ${b['current_close']:.2f}",
    ]
    if details:
        name = details.get("name") or ""
        mcap = _fmt_mcap(details.get("mcap"))
        head = f"{name} · mcap {mcap}" if name else f"mcap {mcap}"
        lines.append(head)
        desc = details.get("desc1") or ""
        if desc:
            lines.append(desc)
    if news:
        title = (news.get("title") or "").strip()
        if title:
            sentiment = news.get("sentiment") or ""
            tag = f"[{sentiment}] " if sentiment else ""
            lines.append(f"📰 {tag}{title}")
    lines.append(tv)
    return "\n".join(lines)


def run(
    tickers=None,
    scanning_period_secs=10,
    num_scans=None,
    lookback_minutes=None,
    breakout_threshold=DEFAULT_BREAKOUT_THRESHOLD,
    rocksdict_path=DEFAULT_ROCKSDICT_PATH,
    min_price=DEFAULT_MIN_PRICE,
    max_price=DEFAULT_MAX_PRICE,
    dedupe_secs=300,
    announce_startup=True,
):
    if lookback_minutes is None:
        lookback_minutes = DEFAULT_LOOKBACK_MINUTES

    logging_utils.setup_logging(
        log_level="INFO",
        log_folder=f"{os.path.dirname(os.path.abspath(__file__))}/logs_wa/",
        include_time=True,
    )

    client = polygon_utils.get_polygon_client()
    os.makedirs(rocksdict_path, exist_ok=True)
    db = Rdict(rocksdict_path)

    logging.info(
        f"WA scanner start: period={scanning_period_secs}s, "
        f"lookbacks={lookback_minutes}min, threshold={breakout_threshold}, "
        f"price=[{min_price},{max_price}], chat={WA_CHAT_ID}"
    )
    if announce_startup:
        send_whatsapp(
            f"📡 Breakout scanner online · {scanning_period_secs}s tick · "
            f"lookbacks {lookback_minutes}m · ≥{(breakout_threshold-1)*100:.1f}% · "
            f"price ${min_price:g}–${max_price:g}"
        )

    # ticker -> last alert unix time, for dedupe within `dedupe_secs`
    last_alert: dict[str, float] = {}
    scan_count = 0

    try:
        while num_scans is None or scan_count < num_scans:
            t0 = time.time()
            key_time_utc = datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
            try:
                items = client.get_snapshot_all("stocks", tickers=tickers)
            except Exception as e:
                logging.warning(f"[snapshot] fetch failed: {e}")
                time.sleep(scanning_period_secs)
                continue

            snapshots = process_items(items)
            store_snapshot(db, key_time_utc, snapshots)
            scan_count += 1
            logging.info(
                f"[scan #{scan_count}] stored {len(snapshots)} tickers at {key_time_utc}"
            )

            breakouts = scan_breakouts(
                db,
                key_time_utc,
                snapshots,
                lookback_minutes,
                breakout_threshold,
                min_price,
                max_price,
            )

            # Keep only the highest-ratio breakout per ticker
            best: dict[str, dict] = {}
            for b in breakouts:
                t = b["ticker"]
                if t not in best or b["ratio"] > best[t]["ratio"]:
                    best[t] = b
            breakouts = sorted(best.values(), key=lambda x: x["ratio"], reverse=True)

            now = time.time()
            fresh = []
            for b in breakouts:
                # Enforce minimum current breakout price
                if b["current_close"] < min_price:
                    continue
                last = last_alert.get(b["ticker"], 0)
                if now - last >= dedupe_secs:
                    fresh.append(b)
                    last_alert[b["ticker"]] = now

            if fresh:
                logging.info(f"=== {len(fresh)} NEW BREAKOUTS → WhatsApp ===")
                for b in fresh:
                    details = get_ticker_details(client, b["ticker"])
                    news = get_latest_news_today(client, b["ticker"])
                    msg = format_alert(b, details, news)
                    logging.info(msg.replace("\n", " | "))
                    send_whatsapp(msg)

            elapsed = time.time() - t0
            sleep_time = max(0, scanning_period_secs - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        logging.info("Scanner stopped by user")
    finally:
        db.close()
        logging.info(f"Database closed. Total scans: {scan_count}")


def parse_args():
    p = argparse.ArgumentParser(description="Realtime scanner → WhatsApp alerts")
    p.add_argument("--tickers", nargs="*", default=None)
    p.add_argument("--scanning_period_secs", type=int, default=10)
    p.add_argument("--num_scans", type=int, default=None)
    p.add_argument(
        "--lookback_minutes", type=int, nargs="*", default=DEFAULT_LOOKBACK_MINUTES
    )
    p.add_argument("--breakout_threshold", type=float, default=DEFAULT_BREAKOUT_THRESHOLD)
    p.add_argument("--min_price", type=float, default=DEFAULT_MIN_PRICE)
    p.add_argument("--max_price", type=float, default=DEFAULT_MAX_PRICE)
    p.add_argument("--dedupe_secs", type=int, default=300,
                   help="min seconds between alerts for the same ticker")
    p.add_argument("--no_startup_msg", action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    a = parse_args()
    run(
        tickers=a.tickers,
        scanning_period_secs=a.scanning_period_secs,
        num_scans=a.num_scans,
        lookback_minutes=a.lookback_minutes,
        breakout_threshold=a.breakout_threshold,
        min_price=a.min_price,
        max_price=a.max_price,
        dedupe_secs=a.dedupe_secs,
        announce_startup=not a.no_startup_msg,
    )
