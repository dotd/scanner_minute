from ScannerMinute.src.download_and_store_utils import merge_consecutive_tasks


def print_tasks(label, tasks):
    print(f"\n  {label}: {len(tasks)} tasks")
    for task in tasks:
        ticker, ts, sd, ed, idx, est = task
        print(f"    Task {idx:3d}: {ticker:<6s}  {ts:<8s}  {sd} -> {ed}  #={est}")


def test_minute_merging():
    """Minute tasks: 960 samples/day, ~52 days fit in 50000."""
    print("=" * 80)
    print("Test 1: Minute tasks — many consecutive days should merge")
    print("=" * 80)

    # Simulate 10 consecutive trading days for 1 ticker
    daily_tasks = []
    days = [f"2026-03-{d:02d}" for d in range(2, 14) if d not in (7, 8)]  # skip weekends
    for i, day in enumerate(days):
        daily_tasks.append(("AAPL", "minute", day, day, i, 960))

    print_tasks("Before merge", daily_tasks)
    merged = merge_consecutive_tasks(daily_tasks)
    print_tasks("After merge", merged)


def test_second_no_merging():
    """Second tasks: 57600 samples/day, cannot merge even 2 days."""
    print("\n" + "=" * 80)
    print("Test 2: Second tasks — should NOT merge (57600 > 50000/2)")
    print("=" * 80)

    daily_tasks = [
        ("AAPL", "second", "2026-03-02", "2026-03-02", 0, 57600),
        ("AAPL", "second", "2026-03-03", "2026-03-03", 1, 57600),
        ("AAPL", "second", "2026-03-04", "2026-03-04", 2, 57600),
    ]

    print_tasks("Before merge", daily_tasks)
    merged = merge_consecutive_tasks(daily_tasks)
    print_tasks("After merge", merged)


def test_multiple_tickers():
    """Each ticker's tasks are merged independently."""
    print("\n" + "=" * 80)
    print("Test 3: Multiple tickers — each merged independently")
    print("=" * 80)

    daily_tasks = []
    idx = 0
    for ticker in ["AAPL", "MSFT"]:
        for day in ["2026-03-02", "2026-03-03", "2026-03-04", "2026-03-05", "2026-03-06"]:
            daily_tasks.append((ticker, "minute", day, day, idx, 960))
            idx += 1

    print_tasks("Before merge", daily_tasks)
    merged = merge_consecutive_tasks(daily_tasks)
    print_tasks("After merge", merged)


def test_mixed_timespans():
    """Minute and second tasks for same ticker merge separately."""
    print("\n" + "=" * 80)
    print("Test 4: Mixed timespans — minute merges, second stays separate")
    print("=" * 80)

    daily_tasks = []
    idx = 0
    for ts, est in [("minute", 960), ("second", 57600)]:
        for day in ["2026-03-02", "2026-03-03", "2026-03-04"]:
            daily_tasks.append(("AAPL", ts, day, day, idx, est))
            idx += 1

    print_tasks("Before merge", daily_tasks)
    merged = merge_consecutive_tasks(daily_tasks)
    print_tasks("After merge", merged)


def test_large_range_splits():
    """60 minute-days should split into 2 chunks (52 + 8)."""
    print("\n" + "=" * 80)
    print("Test 5: 60 days minute — should split into chunks of ~52")
    print("=" * 80)

    daily_tasks = []
    for i in range(60):
        day = f"2026-{(i // 28) + 1:02d}-{(i % 28) + 1:02d}"
        daily_tasks.append(("AAPL", "minute", day, day, i, 960))

    print_tasks("Before merge", daily_tasks)
    merged = merge_consecutive_tasks(daily_tasks)
    print_tasks("After merge", merged)


def test_empty_tasks():
    """Empty input returns empty output."""
    print("\n" + "=" * 80)
    print("Test 6: Empty tasks — should return []")
    print("=" * 80)

    merged = merge_consecutive_tasks([])
    print_tasks("After merge", merged)


def main():
    test_minute_merging()
    test_second_no_merging()
    test_multiple_tickers()
    test_mixed_timespans()
    test_large_range_splits()
    test_empty_tasks()


if __name__ == "__main__":
    main()
