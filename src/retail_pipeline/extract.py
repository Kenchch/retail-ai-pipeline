"""Stage 1 - Extract.

Reads the raw transaction file, standardises column names and types, and hands
a dataframe on to the quality stage. Nothing is cleaned or dropped here: the
extract layer is deliberately faithful to the source so that the quality report
downstream describes the source, not our own edits.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .config import Config, get_logger

log = get_logger(__name__)


def extract(cfg: Config, source: Path | None = None) -> pd.DataFrame:
    path = source or cfg.paths["raw"]
    if not Path(path).exists():
        raise FileNotFoundError(
            f"Raw file not found: {path}\nRun `python scripts/download_data.py` first."
        )

    log.info("Reading raw transactions from %s", path)
    df = pd.read_csv(
        path,
        dtype={"InvoiceNo": "string", "StockCode": "string", "Description": "string"},
        encoding="utf-8",
        on_bad_lines="warn",
    )
    df = df.rename(columns=cfg.extract["column_map"])

    df["invoice_ts"] = pd.to_datetime(
        df["invoice_ts"], format=cfg.extract["date_format"], errors="coerce"
    )
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df["stock_code"] = df["stock_code"].str.strip().str.upper()
    df["description"] = df["description"].str.strip()

    log.info(
        "Extracted %s rows | %s invoices | %s stock codes | %s -> %s",
        f"{len(df):,}",
        f"{df['invoice_no'].nunique():,}",
        f"{df['stock_code'].nunique():,}",
        df["invoice_ts"].min().date(),
        df["invoice_ts"].max().date(),
    )
    return df
