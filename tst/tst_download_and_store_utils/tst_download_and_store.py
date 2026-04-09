import logging
from ScannerMinute.src import logging_utils
from ScannerMinute.src.download_and_store_utils import download_and_store
from ScannerMinute.src.rocksdict_utils import get_first_and_last_time, read_bars


DB_PATH = "./data/rocksdict_test/"


def main():
    logging_utils.setup_logging(log_level="INFO", include_time=True, log_folder="./logs/")

    tickers = ["AAPL", "MSFT", "GOOGL"]
    date_start = "2026-03-16"
    date_end = "2026-03-20"
    timespan = "minute"
    num_threads = 3

    print("=" * 80)
    print(f"Download and store: {tickers}")
    print(f"Range: {date_start} to {date_end}, timespan: {timespan}, threads: {num_threads}")
    print(f"DB path: {DB_PATH}")
    print("=" * 80)

    download_and_store(
        tickers=tickers,
        date_start=date_start,
        date_end=date_end,
        timespan=timespan,
        db_path=DB_PATH,
        num_threads=num_threads,
    )

    # Verify data was stored
    print("\n" + "=" * 80)
    print("Verification: checking stored data")
    print("=" * 80)
    for ticker in tickers:
        first, last = get_first_and_last_time(DB_PATH, timespan, ticker)
        print(f"  {ticker}: first={first}, last={last}")

    # Read back some bars
    print("\n" + "=" * 80)
    print("Sample bars (first 3 per ticker)")
    print("=" * 80)
    bars = read_bars(DB_PATH, timespan, tickers, f"{date_start}T00:00:00", f"{date_end}T23:59:59")
    print(f"Total bars read back: {len(bars)}")
    for ticker in tickers:
        ticker_bars = [b for b in bars if b[0] == ticker]
        print(f"\n  {ticker}: {len(ticker_bars)} bars")
        for i, bar in enumerate(ticker_bars[:3]):
            print(f"    raw[{i}]: {bar}")
        for bar in ticker_bars[:3]:
            print(f"    {bar[1]}  O={bar[2]}  H={bar[3]}  L={bar[4]}  C={bar[5]}  V={bar[6]}")


if __name__ == "__main__":
    main()
