"""
Generate a full sector/industry report and pickle from Finviz data.

Outputs to reports/finviz_sectors_industries/:
  - report_YYYYMMDD_HHMMSS.txt  — human-readable report
  - map_YYYYMMDD_HHMMSS.pkl     — pickled SectorIndustryMap object
"""
import logging
import os
import pickle
from datetime import datetime, timezone

from ScannerMinute.src.logging_utils import setup_logging
from ScannerMinute.src.finviz_utils import download_finviz_cached, get_sector_industry_mappings
from ScannerMinute.definitions import PROJECT_ROOT_DIR


REPORT_DIR = os.path.join(PROJECT_ROOT_DIR, "reports", "finviz_sectors_industries")


def generate_report(mappings):
    lines = []
    now = datetime.now(timezone.utc)
    lines.append(f"Sector & Industry Report — {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    lines.append("=" * 80)
    lines.append("")

    # Sectors
    lines.append("SECTORS")
    lines.append("-" * 80)
    for sector in mappings.sectors:
        lines.append(sector)
    lines.append("")

    # Industries
    lines.append("INDUSTRIES")
    lines.append("-" * 80)
    for industry in mappings.industries:
        lines.append(industry)
    lines.append("")

    # Ticker → Sector / Industry table
    lines.append("TICKER MAP")
    lines.append("-" * 80)
    lines.append(f"{'Ticker':<10} {'Sector':<30} {'Industry'}")
    lines.append(f"{'-'*10} {'-'*30} {'-'*40}")
    for ticker in sorted(mappings.ticker_to_sector.keys()):
        sector = mappings.ticker_to_sector[ticker]
        industry = mappings.ticker_to_industry[ticker]
        lines.append(f"{ticker:<10} {sector:<30} {industry}")
    lines.append("")

    # Tickers by sector
    lines.append("TICKERS BY SECTOR")
    lines.append("-" * 80)
    for sector in mappings.sectors:
        tickers = mappings.tickers_by_sector[sector]
        lines.append(f"{sector} ({len(tickers)})")
        for i in range(0, len(tickers), 10):
            lines.append(" ".join(tickers[i:i + 10]))
        lines.append("")

    # Tickers by industry
    lines.append("TICKERS BY INDUSTRY")
    lines.append("-" * 80)
    for industry in mappings.industries:
        tickers = mappings.tickers_by_industry[industry]
        lines.append(f"{industry} ({len(tickers)})")
        for i in range(0, len(tickers), 10):
            lines.append(" ".join(tickers[i:i + 10]))
        lines.append("")

    return "\n".join(lines)


def run():
    setup_logging(log_level="INFO", include_time=True)
    os.makedirs(REPORT_DIR, exist_ok=True)

    # Download (or load from cache)
    df, _ = download_finviz_cached()
    logging.info(f"Finviz data: {len(df)} tickers")

    mappings = get_sector_industry_mappings(df)
    logging.info(f"Sectors: {len(mappings.sectors)}, Industries: {len(mappings.industries)}")

    now = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # Save pickle
    pkl_path = os.path.join(REPORT_DIR, f"map_{now}.pkl")
    with open(pkl_path, "wb") as f:
        pickle.dump(mappings, f)
    logging.info(f"Map saved to {pkl_path}")

    # Save report
    report = generate_report(mappings)
    report_path = os.path.join(REPORT_DIR, f"report_{now}.txt")
    with open(report_path, "w") as f:
        f.write(report)
    logging.info(f"Report saved to {report_path}")

    print(report)


if __name__ == "__main__":
    run()
