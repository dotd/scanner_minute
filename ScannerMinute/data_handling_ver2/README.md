# data_handling_ver2 module

## Introduction
Minute-level OHLCV data pipeline for all active tickers, stored in RocksDB at `./data/ver2/`.

## Scripts in this module

### `download_data.py` — `download_data(num_threads=30)`

Downloads 1 year of minute-bar data for all active tickers from Polygon.io and stores it in RocksDB.

- Fetches the full ticker list from the market snapshot
- Date range: today minus 365 days to today
- Uses multithreaded download (default 30 threads)

### `verify_data.py` — `verify_data(k=100, db_path=DB_PATH, seed=42)`

Spot-checks data integrity by comparing DB contents against fresh API downloads.

- Samples `k` random (ticker, day) pairs
- Re-downloads minute bars from Polygon and compares them to the stored data
- Uses IoU (Intersection over Union) on timestamps to measure match quality (100% = perfect match)
- Logs per-pair results with cumulative stats and a summary table of any mismatches

### `examine_data.py`

#### `collect_stats(db_path=DB_PATH, timespan="minute")` → `ExamineStats`

Scans the DB directly (no Polygon API call needed) and returns an `ExamineStats` dataclass containing:

- `ticker_stats` — per-ticker counts and time ranges (discovered from DB keys)
- `start_date_counts` — histogram of how many tickers start on each date
- `end_date_counts` — histogram of how many tickers end on each date
- `date_range_counts` — histogram of (start_date, end_date) tuples showing how many tickers share the same date range

#### `examine_data(db_path=DB_PATH, timespan="minute")`

Generates a human-readable text report from stats, saved to `reports/ver2/report_<timestamp>.txt`.

#### `run_pipeline_get_stats(db_path=DB_PATH, timespan="minute")`

Entry point that runs the full examine pipeline: calls `collect_stats` then `examine_data`. Returns `(stats, report_path)`.

