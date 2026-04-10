import os
from ScannerMinute.src.snapshot_utils import download_snapshots
from ScannerMinute.definitions import PROJECT_ROOT_DIR

DATA_DIR = os.path.join(PROJECT_ROOT_DIR, "data", "snapshots")

if __name__ == "__main__":
    download_snapshots(
        period=10,
        snapshots_downloaded=os.path.join(DATA_DIR, "downloaded"),
        snapshot_consolidated=os.path.join(DATA_DIR, "consolidated"),
        snapshots_logs=os.path.join(DATA_DIR, "logs"),
        period_consolidation=30,
        consolidate=True,
    )
