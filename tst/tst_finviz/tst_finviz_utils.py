import logging
from ScannerMinute.src.finviz_utils import download_finviz_cached, INTERESTING_FIELDS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

if __name__ == "__main__":
    table, filename = download_finviz_cached(
        size_filter=None,
        max_pages=150,  # 10 pages = ~200 stocks, quick test
        fields=INTERESTING_FIELDS,
        initial_delay=1.0,
    )
    print(f"\nFile: {filename}")
    print(f"Shape: {table.shape}")
    print(f"Columns: {list(table.columns)}")
    print(f"\nFirst 10 rows:\n{table.head(10).to_string()}")
