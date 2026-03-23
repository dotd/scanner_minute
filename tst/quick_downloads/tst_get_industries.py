import logging
import time

from ScannerMinute.src import logging_utils
from ScannerMinute.src.ticker_utils import ALL_TICKERS
from ScannerMinute.src.quick_download_utils import fetch_ticker_industries


def tst_get_industries():
    logging_utils.setup_logging(
        log_level="INFO", include_time=True, log_folder="./logs/"
    )

    # Test 1: Small ticker list (first call downloads, second call loads from cache)

    logging.info(f"=== Test 1: Fetch industries for {len(ALL_TICKERS)} tickers ===")
    t0 = time.time()
    industries = fetch_ticker_industries(ALL_TICKERS)
    logging.info(f"First call: {len(industries)} industries in {time.time() - t0:.1f}s")
    for ticker, industry in sorted(industries.items()):
        logging.info(f"  {ticker}: {industry}")

    # Test 2: Same list again (should load from cache)
    logging.info(f"\n=== Test 2: Same list again (should use cache) ===")
    t0 = time.time()
    industries2 = fetch_ticker_industries(ALL_TICKERS)
    logging.info(
        f"Cache call: {len(industries2)} industries in {time.time() - t0:.1f}s"
    )
    assert industries == industries2, "Cache mismatch!"
    logging.info("Cache matches original.")

    # Test 3: Different ticker count (should download separately)
    medium_tickers = ALL_TICKERS[:20]
    logging.info(f"\n=== Test 3: Different list ({len(medium_tickers)} tickers) ===")
    t0 = time.time()
    industries3 = fetch_ticker_industries(medium_tickers)
    logging.info(f"Got {len(industries3)} industries in {time.time() - t0:.1f}s")

    logging.info("\nAll tests passed.")


if __name__ == "__main__":
    tst_get_industries()
