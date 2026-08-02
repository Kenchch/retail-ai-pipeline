"""Stage 4 - Load.

Writes two copies of every table:

  * Parquet in data/processed/  - the analytics / feature layer. Columnar and
    compressed, which is what a Spark, Databricks or Synapse job would read.
  * SQLite in data/warehouse/   - a local stand-in for the serving database, so
    the schema can be queried with plain SQL without any infrastructure.

Swapping SQLite for Azure SQL or Postgres is a one-line change to the
connection, because everything goes through pandas' `to_sql`.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from .config import Config, get_logger

log = get_logger(__name__)


def to_parquet(tables: dict[str, pd.DataFrame], cfg: Config) -> None:
    out_dir: Path = cfg.paths["processed"]
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        path = out_dir / f"{name}.parquet"
        df.to_parquet(path, index=False, compression="snappy")
        log.info("Parquet %-22s %8s rows  %6.1f MB", name, f"{len(df):,}",
                 path.stat().st_size / 1e6)


def to_warehouse(tables: dict[str, pd.DataFrame], cfg: Config) -> None:
    db: Path = cfg.paths["warehouse"]
    db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db) as conn:
        for name, df in tables.items():
            df.to_sql(name, conn, if_exists="replace", index=False)
        # Indexes on the join keys the BI layer actually filters on.
        cur = conn.cursor()
        for stmt in [
            "CREATE INDEX IF NOT EXISTS ix_fact_stock ON fact_sales(stock_code)",
            "CREATE INDEX IF NOT EXISTS ix_fact_cust  ON fact_sales(customer_id)",
            "CREATE INDEX IF NOT EXISTS ix_fact_date  ON fact_sales(date_key)",
            "CREATE INDEX IF NOT EXISTS ix_fact_inv   ON fact_sales(invoice_no)",
        ]:
            try:
                cur.execute(stmt)
            except sqlite3.OperationalError:
                pass  # table absent in a partial run
        conn.commit()
    log.info("SQLite warehouse written to %s (%.1f MB)", db, db.stat().st_size / 1e6)


def load(tables: dict[str, pd.DataFrame], cfg: Config) -> None:
    to_parquet(tables, cfg)
    to_warehouse(tables, cfg)
