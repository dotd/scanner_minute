# 1. data_handling_ver2 module

Download, verify, and analyze minute-level OHLCV stock data from Polygon.io, stored in RocksDB.

## 1.1. Introduction
Minute-level OHLCV data pipeline for all active tickers, stored in RocksDB at `./data/ver2/`.

## 1.2. Scripts in this module

### 1.2.1. `ScannerMinute/data_handling_ver2/download_data.py` — `download_data(num_threads=30)`

Downloads 1 year of minute-bar data for all active tickers from Polygon.io and stores it in RocksDB.

- Fetches the full ticker list from the market snapshot
- Date range: today minus 365 days to today
- Uses multithreaded download (default 30 threads)

### 1.2.2. `ScannerMinute/data_handling_ver2/verify_data.py` — `verify_data(k=100, db_path=DB_PATH, seed=42)`

Spot-checks data integrity by comparing DB contents against fresh API downloads.

- Samples `k` random (ticker, day) pairs
- Re-downloads minute bars from Polygon and compares them to the stored data
- Uses IoU (Intersection over Union) on timestamps to measure match quality (100% = perfect match)
- Logs per-pair results with cumulative stats and a summary table of any mismatches

### 1.2.3. `ScannerMinute/data_handling_ver2/examine_data.py`

#### 1.2.3.1. `collect_stats(db_path=DB_PATH, timespan="minute")` → `ExamineStats`

Scans the DB directly (no Polygon API call needed) and returns an `ExamineStats` dataclass containing:

- `ticker_stats` — per-ticker counts and time ranges (discovered from DB keys)
- `start_date_counts` — histogram of how many tickers start on each date
- `end_date_counts` — histogram of how many tickers end on each date
- `date_range_counts` — histogram of (start_date, end_date) tuples showing how many tickers share the same date range

#### 1.2.3.2. `examine_data(db_path=DB_PATH, timespan="minute")`

Generates a human-readable text report from stats, saved to `reports/ver2/report_<timestamp>.txt`.

#### 1.2.3.3. `run_pipeline_get_stats(db_path=DB_PATH, timespan="minute")`

Entry point that runs the full examine pipeline: calls `collect_stats` then `examine_data`. Returns `(stats, report_path)`.

## 1.3. Tests

Tests are located in `tst/data_handling_ver2/`.

### 1.3.1. `tst/data_handling_ver2/tst_download_and_examine.py`

End-to-end test that downloads AAPL and NVDA in two phases (2 years ago → 1 year ago, then 1 year ago → today), collects stats after each phase, and verifies data integrity by comparing DB contents against a fresh API download with per-column breakdown.

### 1.3.2. `tst/data_handling_ver2/tst_download_AAPL_and_show.py`

Downloads AAPL minute data for the last 4 days and displays it as a candlestick chart in the browser via the Node.js chart server.
