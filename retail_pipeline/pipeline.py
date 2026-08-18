"""Retail ETL pipeline: extract -> data quality -> star schema -> load.

Run it with `python -m retail_pipeline.pipeline`.

The stages above are separate functions so the Airflow DAG in dags/ can schedule
them as separate tasks - per-stage retries, and a failure that points at the
stage that broke rather than at "the pipeline".

`run()` below drives the full refresh, which is these four plus recommend and
adoption from the sibling modules - hence the 1/5..5/5 progress markers, which
count data quality and the star-schema build as one step each.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pipeline")


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    """Config with paths resolved, and licensed headcount derived from the roster
    so a total can never drift out of step with the per-team numbers."""
    cfg_path = Path(path) if path else ROOT / "config.yaml"
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    root = cfg_path.resolve().parent
    cfg["paths"] = {k: (root / v).resolve() for k, v in cfg["paths"].items()}
    cfg["adoption"]["licensed_users"] = sum(cfg["adoption"]["roster"].values())
    return cfg


# --------------------------------------------------------------------------- #
# 1. Extract
# --------------------------------------------------------------------------- #


def extract(cfg: dict, source: Path | None = None) -> pd.DataFrame:
    """Read the transaction feed and standardise names and types. Nothing is
    dropped here, so the quality report downstream describes the source rather
    than describing our own edits."""
    path = Path(source or cfg["paths"]["raw"])
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found - run `python scripts/get_data.py` first."
        )

    df = pd.read_csv(
        path,
        dtype={"InvoiceNo": "string", "StockCode": "string", "Description": "string"},
    ).rename(columns=cfg["extract"]["column_map"])

    # A renamed or dropped source column is the most common way a pipeline like
    # this breaks. Without this it surfaces later as a KeyError naming an
    # internal column the reader has never seen.
    missing = sorted(set(cfg["extract"]["column_map"].values()) - set(df.columns))
    if missing:
        raise ValueError(
            f"{path} is missing expected column(s): {', '.join(missing)}. "
            f"Found: {', '.join(map(str, df.columns))}. "
            "Update `extract.column_map` in config.yaml."
        )
    if df.empty:
        raise ValueError(f"{path} contains no rows.")

    df["invoice_ts"] = pd.to_datetime(
        df["invoice_ts"], format=cfg["extract"]["date_format"], errors="coerce"
    )
    if df["invoice_ts"].isna().all():
        raise ValueError(
            f"No timestamp matched '{cfg['extract']['date_format']}' - the source "
            "date format has changed. Fix `extract.date_format` in config.yaml."
        )
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype(
        "Int64"
    )
    df["stock_code"] = df["stock_code"].str.strip().str.upper()
    df["description"] = df["description"].str.strip()

    log.info(
        "Extracted %s rows | %s invoices | %s -> %s",
        f"{len(df):,}",
        f"{df['invoice_no'].nunique():,}",
        df["invoice_ts"].min().date(),
        df["invoice_ts"].max().date(),
    )
    return df


# --------------------------------------------------------------------------- #
# 2. Data quality
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Check:
    name: str
    dimension: str
    blocking: bool  # blocking rows are quarantined; the rest are flagged
    why: str
    fn: Callable[[pd.DataFrame, dict], pd.Series]  # True = row FAILS


CHECKS: list[Check] = [
    Check(
        "duplicate_line_items",
        "uniqueness",
        True,
        "Same invoice/product/qty/price/timestamp twice - double-counts revenue",
        lambda d, c: d.duplicated(
            subset=["invoice_no", "stock_code", "quantity", "unit_price", "invoice_ts"]
        ),
    ),
    Check(
        "missing_invoice_key",
        "completeness",
        True,
        "Invoice, product or timestamp is null - the row cannot be modelled",
        lambda d, c: (
            d["invoice_no"].isna() | d["stock_code"].isna() | d["invoice_ts"].isna()
        ),
    ),
    Check(
        "cancelled_invoice",
        "validity",
        True,
        "'C'-prefixed invoices are cancellations, not sales",
        lambda d, c: d["invoice_no"].fillna("").str.upper().str.startswith("C"),
    ),
    Check(
        "non_positive_quantity",
        "validity",
        True,
        "Returns and stock adjustments",
        lambda d, c: d["quantity"].isna() | (d["quantity"] <= 0),
    ),
    Check(
        "non_positive_price",
        "validity",
        True,
        "Zero-price giveaways and manual corrections",
        lambda d, c: (
            d["unit_price"].isna() | (d["unit_price"] < c["quality"]["min_unit_price"])
        ),
    ),
    Check(
        "price_outlier",
        "validity",
        True,
        "Above the configured cap - almost always an adjustment line",
        lambda d, c: d["unit_price"] > c["quality"]["max_unit_price"],
    ),
    Check(
        "non_product_stock_code",
        "consistency",
        True,
        "POST, BANK CHARGES, M - real rows, but not sellable products",
        lambda d, c: (
            d["stock_code"]
            .fillna("")
            .str.upper()
            .isin({x.upper() for x in c["quality"]["non_product_codes"]})
        ),
    ),
    Check(
        "missing_description",
        "completeness",
        False,
        "Degrades the recommender, not the sales facts",
        lambda d, c: (
            d["description"].isna() | (d["description"].fillna("").str.len() == 0)
        ),
    ),
    Check(
        "missing_customer_id",
        "completeness",
        False,
        "Guest checkout - fine for basket analysis, not for customer analytics",
        lambda d, c: d["customer_id"].isna(),
    ),
]


def check_quality(
    df: pd.DataFrame, cfg: dict
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return (clean rows, quarantined rows with reasons, per-rule results).

    Two decisions worth stating, because both are business calls rather than
    technical ones:

    * **Blocking vs flagging.** A quarter of rows have no customer id. Dropping
      them would throw away a quarter of the basket evidence to satisfy a rule
      only customer analytics cares about, so that rule reports instead of
      rejecting. Cancellations are silently wrong in a sales table, so they block.
    * **Quarantine, not delete.** Rejected rows are kept with the rules they
      broke, so a rule that turns out to be too aggressive can be relaxed and
      the rows re-admitted.
    """
    flags = pd.DataFrame(
        {c.name: c.fn(df, cfg).fillna(True).astype(bool) for c in CHECKS},
        index=df.index,
    )
    results = pd.DataFrame(
        [
            {
                "check": c.name,
                "dimension": c.dimension,
                "blocking": c.blocking,
                "failed_rows": int(flags[c.name].sum()),
                "failed_pct": round(100 * flags[c.name].mean(), 3) if len(df) else 0.0,
                "why": c.why,
            }
            for c in CHECKS
        ]
    )
    for r in results.itertuples():
        log.info(
            "  %-24s %9s rows (%5.2f%%) %s",
            r.check,
            f"{r.failed_rows:,}",
            r.failed_pct,
            "[blocking]" if r.blocking else "",
        )

    blocking = [c.name for c in CHECKS if c.blocking]
    failed = flags[blocking].any(axis=1)

    quarantine = df[failed].copy()
    names = np.array(blocking)
    quarantine["reasons"] = [
        ",".join(names[row]) for row in flags.loc[failed, blocking].to_numpy(dtype=bool)
    ]
    clean = df[~failed].copy()

    rate = float(failed.mean()) if len(df) else 0.0
    log.info(
        "Quarantined %s of %s rows (%.2f%%)",
        f"{int(failed.sum()):,}",
        f"{len(df):,}",
        100 * rate,
    )
    if rate > cfg["quality"]["max_quarantine_rate"]:
        # Write the report BEFORE raising. The gate's message says "investigate
        # the source extract", and this report is the artefact an investigator
        # opens to do that - per-rule counts and percentages. Raising first left
        # the previous run's green numbers in place, so the one run that needed
        # the report was the only run that did not produce it, and nothing in
        # the file said the run had failed at all.
        write_quality_report(
            results,
            cfg,
            len(df),
            len(clean),
            len(quarantine),
            gate_failed=True,
            rate=rate,
            ceiling=cfg["quality"]["max_quarantine_rate"],
        )
        # Fail before loading, so a broken upstream extract leaves last night's
        # published data intact rather than replacing it with something thinner.
        raise ValueError(
            f"Quarantine rate {rate:.1%} exceeds the {cfg['quality']['max_quarantine_rate']:.0%} "
            "ceiling - refusing to load. Investigate the source extract."
        )
    return clean, quarantine, results


def write_quality_report(
    results: pd.DataFrame,
    cfg: dict,
    n_in: int,
    n_out: int,
    n_q: int,
    *,
    gate_failed: bool = False,
    rate: float | None = None,
    ceiling: float | None = None,
) -> None:
    pct = 100 * n_q / n_in if n_in else 0.0
    lines = ["# Data quality report", ""]
    if gate_failed:
        # State the outcome at the top. A reader who sees only the table cannot
        # tell a published run from a rejected one.
        lines += [
            (
                f"> **GATE FAILED - NOTHING WAS PUBLISHED.** Quarantine rate {rate:.2%} "
                f"exceeds the {ceiling:.0%} ceiling. The figures below describe the "
                "*rejected* extract; the warehouse still holds the previous run's data."
            ),
            "",
        ]
    lines += [
        f"- Rows read: **{n_in:,}**",
        f"- Quarantined (failed a blocking rule): **{n_q:,}** ({pct:.2f}%)",
        f"- {'Would have loaded' if gate_failed else 'Loaded'}: **{n_out:,}**",
        "",
        "| Check | Dimension | Blocking | Failed | % | What it means |",
        "|---|---|---|---|---|---|",
    ]
    lines += [
        f"| `{r.check}` | {r.dimension} | {'yes' if r.blocking else 'no'} "
        f"| {r.failed_rows:,} | {r.failed_pct:.2f}% | {r.why} |"
        for r in results.itertuples()
    ]
    lines += [
        "",
        "Rows can fail more than one check, so the column does not sum to the total.",
    ]
    (cfg["paths"]["reports"] / "data_quality_report.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


# --------------------------------------------------------------------------- #
# 3. Transform - a star schema
# --------------------------------------------------------------------------- #


def transform(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """fact_sales joined to dim_product / dim_customer / dim_date.

    The fact table carries measures and keys only, so a product renamed once is
    renamed everywhere. BI slices by product/customer/time; the recommender
    reads only invoice + product; both read the same conformed dimensions.
    """
    d = df.copy()
    d["date_key"] = pd.to_datetime(d["invoice_ts"].dt.date)
    d["revenue"] = (d["quantity"] * d["unit_price"]).round(4)

    fact = d[
        [
            "invoice_no",
            "stock_code",
            "customer_id",
            "date_key",
            "invoice_ts",
            "quantity",
            "unit_price",
            "revenue",
            "country",
        ]
    ].reset_index(drop=True)

    # Descriptions vary between rows for the same code; the most used one wins.
    desc = (
        d.dropna(subset=["description"])
        .groupby(["stock_code", "description"])
        .size()
        .reset_index(name="n")
        .sort_values(["stock_code", "n"], ascending=[True, False])
        .drop_duplicates("stock_code")[["stock_code", "description"]]
    )
    dim_product = (
        d.groupby("stock_code")
        .agg(
            n_invoices=("invoice_no", "nunique"),
            units_sold=("quantity", "sum"),
            revenue=("revenue", "sum"),
            avg_unit_price=("unit_price", "mean"),
        )
        .reset_index()
        .merge(desc, on="stock_code", how="left")
    )
    dim_product["description"] = dim_product["description"].fillna("UNKNOWN")

    known = d.dropna(subset=["customer_id"])
    dim_customer = (
        known.groupby("customer_id")
        .agg(
            country=(
                "country",
                lambda s: s.mode().iat[0] if not s.mode().empty else "Unknown",
            ),
            n_invoices=("invoice_no", "nunique"),
            total_revenue=("revenue", "sum"),
            first_order=("invoice_ts", "min"),
            last_order=("invoice_ts", "max"),
        )
        .reset_index()
    )

    dim_date = _date_dimension(d)

    log.info(
        "fact_sales %s rows | dim_product %s | dim_customer %s | dim_date %s",
        f"{len(fact):,}",
        f"{len(dim_product):,}",
        f"{len(dim_customer):,}",
        f"{len(dim_date):,}",
    )
    return {
        "fact_sales": fact,
        "dim_product": dim_product,
        "dim_customer": dim_customer,
        "dim_date": dim_date,
    }


def _date_dimension(d: pd.DataFrame) -> pd.DataFrame:
    """A CONTINUOUS calendar, not just the days that traded.

    This retailer is shut on Saturdays. Building the dimension from the dates
    present in the data would omit 53 of them, and a BI user grouping by week
    would silently get six-day weeks. `has_sales` keeps "closed" distinguishable
    from "missing".
    """
    observed = pd.to_datetime(pd.Series(d["invoice_ts"].dt.date.unique())).dropna()
    if observed.empty:
        raise ValueError("No valid timestamps - cannot build a date dimension.")
    dim = pd.DataFrame(
        {"date_key": pd.date_range(observed.min(), observed.max(), freq="D")}
    )
    dim["has_sales"] = dim["date_key"].isin(observed)
    dim["year"] = dim["date_key"].dt.year
    dim["month"] = dim["date_key"].dt.month
    dim["day_of_week"] = dim["date_key"].dt.day_name()
    dim["is_weekend"] = dim["date_key"].dt.dayofweek >= 5
    return dim


# --------------------------------------------------------------------------- #
# 4. Load
# --------------------------------------------------------------------------- #

# Columns whose contents the published numbers actually depend on. Hashing the
# whole file over-reports for usage_events.csv: generate_events() draws
# stock_code from dim_product.parquet when it exists and from synthetic SKUxxxxx
# codes when it does not, so a first run on a clean clone produces a different
# byte stream while every adoption metric is identical - no metric reads
# stock_code. A digest that changes when nothing measurable changed is a false
# alarm, and a false alarm in a provenance check is worse than none, because it
# trains the reader to ignore it.
_FINGERPRINT_COLS = {
    "usage_events": ["event_ts", "user_id", "team", "event_type", "feedback_score"],
}


def _input_fingerprint(cfg: dict) -> dict:
    """sha256 + row count of each raw input, for run-to-run traceability.

    Scoped to the metric-bearing columns where they differ from the file, so
    the digest answers "could this input have produced different numbers?"
    rather than "is this byte-identical to some other run?".
    """
    import hashlib

    out = {}
    for key in ("raw", "usage_events"):
        p = Path(cfg["paths"][key])
        if not p.exists():
            continue

        cols = _FINGERPRINT_COLS.get(key)
        if cols:
            # dtype=str + keep_default_na=False: hash the text as written, so
            # the digest cannot shift on a pandas dtype or NA-formatting change.
            df = pd.read_csv(p, usecols=cols, dtype=str, keep_default_na=False)
            # Sort before hashing so the digest describes the *content* of the
            # telemetry, not the order it happens to sit in. Row order is not a
            # property any metric depends on, so letting it move the digest
            # would be another false alarm of the kind this check exists to
            # avoid. kind="stable" so the sort itself is deterministic.
            # lineterminator="\n" is load-bearing, not tidiness. to_csv defaults
            # it to os.linesep - for a returned string as much as for a written
            # file - so the same telemetry hashes to one value on Windows and a
            # different one on Linux, purely from CRLF vs LF in the payload.
            # That makes the digest answer "which OS ran this?" on top of "could
            # this input have produced different numbers?", and the first
            # question drowns out the second: a Windows dev and a Linux CI run
            # disagree forever on byte-identical data. Pinning the terminator is
            # what makes the digest portable, which is the only way it can serve
            # as a provenance record at all.
            payload = (
                df[cols]
                .sort_values(cols, kind="stable")
                .to_csv(index=False, lineterminator="\n")
                .encode()
            )
            rows = len(df)
        else:
            payload = p.read_bytes()
            rows = payload.count(b"\n") - 1

        out[p.name] = {
            "sha256": hashlib.sha256(payload).hexdigest()[:16],
            "rows": rows,
            **({"columns_hashed": cols} if cols else {}),
        }
    return out


_INDEXES = (
    "CREATE INDEX IF NOT EXISTS ix_fact_stock ON fact_sales(stock_code)",
    "CREATE INDEX IF NOT EXISTS ix_fact_date ON fact_sales(date_key)",
)


def load(tables: dict[str, pd.DataFrame], cfg: dict) -> None:
    """Publish to Parquet and SQLite, staging both so a mid-load failure cannot
    leave the two layers describing different runs.

    The guarantee, stated precisely because the previous version claimed one it
    did not have: SQLite is swapped in a single transaction, and no Parquet file
    is moved into the published directory until that transaction has committed.
    A crash before the commit leaves BOTH layers entirely on the previous run;
    a crash during the final renames can leave a mixed Parquet set, which is the
    one residual window and is milliseconds wide.

    What did not work: wrapping `to_sql(..., if_exists="replace")` calls in
    `with conn:`. pandas' SQLiteDatabase.run_transaction commits after *every*
    to_sql, and the DROP/CREATE that "replace" issues runs outside sqlite3's
    implicit transaction anyway - so the outer block bought nothing. A killed
    worker left fact_sales from tonight beside dim_product from last night, with
    fact keys silently resolving against the wrong dimension rows and no error
    anywhere. Verified against pandas 3.0.2's own source.
    """
    out = cfg["paths"]["processed"]
    out.mkdir(parents=True, exist_ok=True)
    cfg["paths"]["warehouse"].parent.mkdir(parents=True, exist_ok=True)

    # 1. Parquet to sidecars - written now, published only after SQLite commits.
    retired: list[str] = []
    staged: list[tuple[Path, Path]] = []
    for name, df in tables.items():
        tmp = out / f"{name}.parquet.tmp"
        df.to_parquet(tmp, index=False, compression="snappy")
        staged.append((tmp, out / f"{name}.parquet"))

    # timeout: the DAG runs measure_adoption as a branch parallel to the
    # merchandising chain, so two tasks can call load() against this file at
    # once. Writing the star schema holds the write lock for ~4 s, and the
    # sqlite3 default timeout is 5 s - a margin thin enough that a slower disk
    # or a larger extract turns into "database is locked". WAL lets the readers
    # through and the explicit timeout gives the writers room to queue.
    try:
        with closing(sqlite3.connect(cfg["paths"]["warehouse"], timeout=60.0)) as conn:
            conn.execute("PRAGMA journal_mode=WAL")

            # 2. Load into __new tables, under sqlite3's default transaction
            #    handling. Do NOT switch to isolation_level=None before this:
            #    that autocommits every INSERT individually and took a full run
            #    from 12 s to 80 s (measured).
            for name, df in tables.items():
                df.to_sql(f"{name}__new", conn, if_exists="replace", index=False)

            # 3. Swap. isolation_level=None only now, because sqlite3 does not
            #    open a transaction for DDL, so without manual control the
            #    renames below would autocommit table by table - which is the
            #    torn state being fixed. SQLite DDL is transactional, so with an
            #    explicit BEGIN this is genuinely all-or-nothing.
            conn.isolation_level = None
            conn.execute("BEGIN IMMEDIATE")
            try:
                for name in tables:
                    conn.execute(f'DROP TABLE IF EXISTS "{name}"')
                    conn.execute(f'ALTER TABLE "{name}__new" RENAME TO "{name}"')

                # 4. Indexes go INSIDE the transaction, with the swap they
                #    belong to. Built after the COMMIT, a failure here left the
                #    warehouse holding tonight's tables while the `except`
                #    below deleted the staged Parquet, so SQLite was the new run
                #    and Parquet was entirely the old one - and load() raised,
                #    so the caller believed nothing had been published.
                #    Reproduced by pointing an index at a column that does not
                #    exist: sqlite=7 rows, parquet=522,566.
                #
                #    Indexes are dropped along with the table they were on, so
                #    they are rebuilt on every swap rather than only first load.
                for stmt in _INDEXES:
                    try:
                        conn.execute(stmt)
                    except sqlite3.OperationalError as exc:
                        # Only "the table isn't here yet" is expected - the
                        # adoption branch legitimately runs before fact_sales
                        # exists. A lock or disk error is not, and swallowing it
                        # would hide a failed write behind a successful run.
                        if "no such table" not in str(exc):
                            raise
                # 5. Retire what this pipeline published before and is not
                #    publishing now. Without it the warehouse accumulated tables
                #    no code writes any more - dq_results and
                #    adoption_top_products sat there for weeks, still returning
                #    rows to anyone who queried them, frozen at whatever the
                #    last version that wrote them produced. A stale table that
                #    answers is worse than a missing one that errors.
                #
                #    Scoped to a recorded manifest rather than "everything not
                #    in `tables`", so a table someone else put in this database
                #    is never dropped by us.
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS _published (name TEXT PRIMARY KEY)"
                )
                previous = {r[0] for r in conn.execute("SELECT name FROM _published")}
                retired = sorted(previous - set(tables))
                for gone in retired:
                    conn.execute(f'DROP TABLE IF EXISTS "{gone}"')
                    log.info("Retired %s - no longer published", gone)
                conn.execute("DELETE FROM _published")
                conn.executemany(
                    "INSERT INTO _published(name) VALUES (?)",
                    [(n,) for n in sorted(tables)],
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
    except BaseException:
        for tmp, _ in staged:  # publish nothing on the way out
            tmp.unlink(missing_ok=True)
        # pandas commits each `<name>__new` before the swap transaction opens,
        # so a rollback leaves them in the database - a partial run's rows,
        # visible to anything that lists tables. Observed after a failed load:
        # fact_sales__new sitting alongside the real table.
        try:
            with closing(
                sqlite3.connect(cfg["paths"]["warehouse"], timeout=60.0)
            ) as c2:
                for name in tables:
                    c2.execute(f'DROP TABLE IF EXISTS "{name}__new"')
                c2.commit()
        except sqlite3.Error:
            pass  # the original failure is the one worth raising
        raise

    # 6. SQLite is committed; publish Parquet, and remove the Parquet of any
    #    table retired above. Both layers retire together or the analytics
    #    directory keeps serving a file the warehouse no longer has.
    for tmp, final in staged:
        os.replace(tmp, final)
    for gone in retired:
        (out / f"{gone}.parquet").unlink(missing_ok=True)
    log.info("Loaded %s tables to Parquet and SQLite", len(tables))


# --------------------------------------------------------------------------- #


def write_run_metrics(
    cfg: dict,
    *,
    n_raw: int,
    n_quarantined: int,
    n_clean: int,
    n_products: int,
    recs: pd.DataFrame,
    adoption_headline: pd.DataFrame,
    runtime_seconds: float,
) -> dict:
    """Assemble and write reports/run_metrics.json.

    Factored out of run() because run() is not the code path production uses.
    The Airflow DAG wires the stage functions up directly and never calls run(),
    so for every scheduled run this file was simply never rewritten - it stayed
    frozen at whatever a developer's last local run produced while the two
    markdown reports beside it refreshed nightly. A provenance record that
    silently describes a different run's inputs is worse than none, which is the
    exact failure mode the digest below exists to prevent.
    """
    metrics = {
        "rows_source": n_raw,
        "rows_quarantined": n_quarantined,
        "quarantine_rate_pct": round(100 * n_quarantined / n_raw, 2) if n_raw else 0.0,
        "rows_loaded": n_clean,
        "products": n_products,
        "recommendations": len(recs),
        "catalogue_coverage_pct": round(
            100 * recs["stock_code"].nunique() / n_products, 1
        )
        if n_products
        else 0.0,
        "adoption": {r.metric: r.value for r in adoption_headline.itertuples()},
        # Fingerprint the telemetry input. generate_events() in scripts/get_data.py
        # is seeded and deterministic, but the pipeline reads whatever
        # usage_events.csv happens to be on disk -- so a file left over from an
        # older revision produces a self-consistent report that no one can
        # reproduce, and any doc quoting it silently goes stale. Recording the
        # digest makes "which input produced this report" answerable from the
        # report itself.
        "inputs": _input_fingerprint(cfg),
        "runtime_seconds": round(runtime_seconds, 1),
    }
    (cfg["paths"]["reports"] / "run_metrics.json").write_text(
        json.dumps(metrics, indent=2, default=str), encoding="utf-8"
    )
    return metrics


def run(config_path: str | None = None) -> dict:
    from retail_pipeline.adoption import measure_adoption
    from retail_pipeline.recommend import recommend

    started = time.perf_counter()
    cfg = load_config(config_path)
    for key in ("processed", "reports"):
        cfg["paths"][key].mkdir(parents=True, exist_ok=True)

    log.info("--- 1/5 extract")
    raw = extract(cfg)
    log.info("--- 2/5 data quality")
    clean, quarantine, results = check_quality(raw, cfg)
    write_quality_report(results, cfg, len(raw), len(clean), len(quarantine))
    log.info("--- 3/5 transform")
    tables = transform(clean)
    log.info("--- 4/5 recommend")
    recs = recommend(tables, cfg)
    log.info("--- 5/5 adoption")
    adoption = measure_adoption(cfg)

    load({**tables, "quarantine": quarantine, "recommendations": recs, **adoption}, cfg)

    metrics = write_run_metrics(
        cfg,
        n_raw=len(raw),
        n_quarantined=len(quarantine),
        n_clean=len(clean),
        n_products=len(tables["dim_product"]),
        recs=recs,
        adoption_headline=adoption["adoption_headline"],
        runtime_seconds=time.perf_counter() - started,
    )
    log.info("Done in %.1fs", metrics["runtime_seconds"])
    return metrics


if __name__ == "__main__":
    run()
