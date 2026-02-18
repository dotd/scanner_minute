import logging
import duckdb
import pandas as pd
from datetime import datetime
from pathlib import Path
from ScannerMinute.src import polygon_utils
from ScannerMinute.definitions import PROJECT_ROOT_DIR

DATA_DIR = f"{PROJECT_ROOT_DIR}/data/polygon"
"""
Calculation how many smaples are for ticker in a month:
16*60*25 = 24000 samples per month per ticker
"""


def get_conn(read_only=False):
    return duckdb.connect(read_only=read_only)


# ─── WRITE ────────────────────────────────────────────────────────────────────

"""
def bars_to_df(ticker: str, bars: list) -> pd.DataFrame:
    records = []
    for bar in bars:
        datetime_value = datetime.utcfromtimestamp(bar.timestamp / 1000)
        records.append(
            {
                "ticker": ticker,
                "year": datetime_value.strftime("%Y"),
                "month": datetime_value.strftime("%m"),
                "day": datetime_value.strftime("%d"),
                "time": datetime_value.strftime("%H%M%S"),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "vwap": bar.vwap,
                "timestamp": bar.timestamp,
                "transactions": bar.transactions,
                "otc": bar.otc,
            }
        )
    return pd.DataFrame(records)
"""


def save_bars(ticker: str, bars: list, conn_input=None, data_dir: str = DATA_DIR):
    """Save bars for a ticker to partitioned parquet files."""
    df = pd.DataFrame(bars, columns=polygon_utils.COLUMNS)
    if df.empty:
        return

    conn = get_conn() if conn_input is None else conn_input
    conn.execute(
        f"""
        COPY (
            SELECT * FROM df ORDER BY timestamp
        )
        TO '{data_dir}'
        (FORMAT PARQUET, PARTITION_BY (ticker), OVERWRITE_OR_IGNORE TRUE, ROW_GROUP_SIZE 100000)
    """
    )
    conn.commit() if conn_input is None else conn.close()
    logging.info(f"Saved {len(df)} bars for {ticker}")


'''
def save_bars_df(df: pd.DataFrame, conn_input=None, data_dir: str = DATA_DIR):
    """Save a dataframe of bars (multiple tickers) to partitioned parquet files."""
    conn = get_conn()
    conn.execute(
        f"""
        COPY (
            SELECT * FROM df ORDER BY ticker, timestamp
        )
        TO '{data_dir}'
        (FORMAT PARQUET, PARTITION_BY (ticker, year, month), OVERWRITE_OR_IGNORE TRUE, ROW_GROUP_SIZE 100000)
    """
    )
    conn.close()
'''

# ─── READ ─────────────────────────────────────────────────────────────────────


def query_bars(
    tickers: list[str],
    date_from: str,  # "YYYYMMDD"
    date_to: str,  # "YYYYMMDD"
    data_dir: str = DATA_DIR,
) -> pd.DataFrame:
    """
    Retrieve bars for given tickers and date range.

    Example:
        df = query_bars(['AAPL', 'MSFT'], '20240101_000000', '20240630_235959')
    """
    # Build ticker filter path globs for partition pruning
    ticker_globs = ", ".join([f"'{data_dir}/ticker={t}/**.parquet'" for t in tickers])

    conn = get_conn()
    df = conn.execute(
        f"""
        SELECT *
        FROM read_parquet([{ticker_globs}], hive_partitioning=true)
        WHERE datetime_utc BETWEEN '{date_from}' AND '{date_to}'
        ORDER BY ticker, timestamp
    """
    ).df()
    conn.close()
    return df


def query_bars_time_range(
    tickers: list[str],
    date_from: str,  # "YYYYMMDD"
    date_to: str,  # "YYYYMMDD"
    time_from: str = "000000",  # "HHMMSS"
    time_to: str = "235959",  # "HHMMSS"
    data_dir: str = DATA_DIR,
) -> pd.DataFrame:
    """
    Retrieve bars filtered by both date and intraday time window.
    Useful for e.g. only market hours: time_from='093000', time_to='160000'

    Example:
        df = query_bars_time_range(['AAPL'], '20240101', '20240630', '093000', '160000')
    """
    ticker_globs = ", ".join([f"'{data_dir}/ticker={t}/**.parquet'" for t in tickers])

    conn = get_conn()
    df = conn.execute(
        f"""
        SELECT *
        FROM read_parquet([{ticker_globs}], hive_partitioning=true)
        WHERE date BETWEEN '{date_from}' AND '{date_to}'
          AND time BETWEEN '{time_from}' AND '{time_to}'
        ORDER BY ticker, timestamp
    """
    ).df()
    conn.close()
    return df


def query_single_ticker(
    ticker: str,
    date_from: str,
    date_to: str,
    data_dir: str = DATA_DIR,
) -> pd.DataFrame:
    return query_bars([ticker], date_from, date_to, data_dir)


# ─── UTILS ────────────────────────────────────────────────────────────────────


def list_tickers(data_dir: str = DATA_DIR) -> list[str]:
    """List all tickers available in the data directory."""
    base = Path(data_dir)
    if not base.exists():
        return []
    return [p.name.replace("ticker=", "") for p in base.iterdir() if p.is_dir()]


def get_date_range(ticker: str, data_dir: str = DATA_DIR) -> dict:
    """Get the min/max date available for a ticker."""
    conn = get_conn()
    result = conn.execute(
        f"""
        SELECT MIN(date) as min_date, MAX(date) as max_date
        FROM read_parquet('{data_dir}/ticker={ticker}/**.parquet', hive_partitioning=true)
    """
    ).fetchone()
    conn.close()
    return {"min_date": result[0], "max_date": result[1]}


def get_stats(data_dir: str = DATA_DIR) -> pd.DataFrame:
    """Get row counts and date ranges per ticker."""
    conn = get_conn()
    df = conn.execute(
        f"""
        SELECT 
            ticker,
            COUNT(*) as bar_count,
            MIN(date) as first_date,
            MAX(date) as last_date
        FROM read_parquet('{data_dir}/ticker=*/**.parquet', hive_partitioning=true)
        GROUP BY ticker
        ORDER BY ticker
    """
    ).df()
    conn.close()
    return df
