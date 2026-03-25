from ScannerMinute.src.polygon_utils import generate_daily_tasks, get_polygon_client


def print_tasks(tasks):
    for task in tasks:
        ticker, ts, sd, ed, idx, est_samples = task
        print(f"  Task {idx:3d}: {ticker:<6s}  {ts:<8s}  {sd}  {ed}  #={est_samples}")


def test_list_of_tickers_single_timespan(client):
    print("=" * 80)
    print("Test 1: List of tickers, single timespan (minute)")
    print("=" * 80)
    tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
    tasks = generate_daily_tasks(tickers, "minute", "2026-03-16", "2026-03-22", client=client)
    print(f"Tickers: {tickers}")
    print(f"Total tasks: {len(tasks)}\n")
    print_tasks(tasks)
    print()


def test_single_ticker_as_string(client):
    print("=" * 80)
    print("Test 2: Single ticker as string")
    print("=" * 80)
    tasks = generate_daily_tasks("AAPL", "minute", "2026-03-16", "2026-03-22", client=client)
    print(f"Ticker: 'AAPL' (string)")
    print(f"Total tasks: {len(tasks)}\n")
    print_tasks(tasks)
    print()


def test_single_ticker_list_of_timespans(client):
    print("=" * 80)
    print("Test 3: Single ticker, list of timespans (minute + second)")
    print("=" * 80)
    tasks = generate_daily_tasks("AAPL", ["minute", "second"], "2026-03-16", "2026-03-22", client=client)
    print(f"Ticker: 'AAPL', timespans: ['minute', 'second']")
    print(f"Total tasks: {len(tasks)}\n")
    print_tasks(tasks)
    print()


def test_multiple_tickers_multiple_timespans(client):
    print("=" * 80)
    print("Test 4: Multiple tickers, multiple timespans")
    print("=" * 80)
    tickers = ["AAPL", "MSFT"]
    timespans = ["minute", "second"]
    tasks = generate_daily_tasks(tickers, timespans, "2026-03-16", "2026-03-22", client=client)
    print(f"Tickers: {tickers}, timespans: {timespans}")
    print(f"Total tasks: {len(tasks)}\n")
    print_tasks(tasks)
    print()


def test_weekend_only_range(client):
    print("=" * 80)
    print("Test 5: Weekend-only range (expect 0 tasks)")
    print("=" * 80)
    tasks = generate_daily_tasks("AAPL", "minute", "2026-03-21", "2026-03-22", client=client)
    print(f"Range: 2026-03-21 (Sat) to 2026-03-22 (Sun)")
    print(f"Total tasks: {len(tasks)}\n")
    print_tasks(tasks)
    print()


def main():
    client = get_polygon_client()
    test_list_of_tickers_single_timespan(client)
    test_single_ticker_as_string(client)
    test_single_ticker_list_of_timespans(client)
    test_multiple_tickers_multiple_timespans(client)
    test_weekend_only_range(client)


if __name__ == "__main__":
    main()
