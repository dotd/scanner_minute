from ScannerMinute.src import polygon_utils
from ScannerMinute.src import duckdb_utils
from ScannerMinute.definitions import PROJECT_ROOT_DIR


def main():
    client = polygon_utils.get_polygon_client()
    data = polygon_utils.get_ticker_data_from_polygon(
        client, "AAPL", "minute", "2025-01-01", "2025-02-01"
    )
    # print(data)
    duckdb_utils.save_bars("AAPL", data)
    df = duckdb_utils.query_bars(["AAPL"], "2025-01-01", "2025-02-01")
    print(df)


if __name__ == "__main__":
    main()
