"""Download the source dataset into data/raw/.

Source: UCI "Online Retail" dataset (transactions of a UK online giftware
retailer, Dec 2010 - Dec 2011). The copy used here is the mirror bundled with
Databricks' "Spark: The Definitive Guide" repository, because the UCI host
is not always reachable from CI.

Usage:
    python scripts/download_data.py
"""

from __future__ import annotations

import sys
import urllib.request
from pathlib import Path

URL = (
    "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/"
    "master/data/retail-data/all/online-retail-dataset.csv"
)
DEST = Path(__file__).resolve().parents[1] / "data" / "raw" / "online_retail.csv"


def main() -> int:
    DEST.parent.mkdir(parents=True, exist_ok=True)
    if DEST.exists():
        print(f"Already present: {DEST} ({DEST.stat().st_size / 1e6:.1f} MB)")
        return 0
    print(f"Downloading {URL}\n  -> {DEST}")
    urllib.request.urlretrieve(URL, DEST)
    print(f"Done: {DEST.stat().st_size / 1e6:.1f} MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
