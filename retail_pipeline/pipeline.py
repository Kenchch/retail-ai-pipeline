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
import shutil
import sqlite3
import time
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
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
    df: pd.DataFrame,
    cfg: dict,
    *,
    reports_dest: Path | None = None,
    run_id: str | None = None,
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
        #
        # It goes to `reports_dest` when the caller supplies one, which is how
        # the failure report reaches reports/failed_runs/<run_id>/ instead of
        # replacing the report describing the data the warehouse still holds.
        # A GATE FAILED report published over the last good one told every
        # reader of reports/ that the current warehouse contents were rejected,
        # which is the opposite of what happened.
        write_quality_report(
            results,
            cfg,
            len(df),
            len(clean),
            len(quarantine),
            gate_failed=True,
            rate=rate,
            ceiling=cfg["quality"]["max_quarantine_rate"],
            dest=reports_dest,
            run_id=run_id,
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
    dest: Path | None = None,
    run_id: str | None = None,
) -> None:
    pct = 100 * n_q / n_in if n_in else 0.0
    lines = ["# Data quality report", ""]
    if run_id:
        # Every report names the version it belongs to. reports/CURRENT is the
        # authority for which version is published; the copies at the top of
        # reports/ are a convenience for GitHub readers, and this line is what
        # lets anyone tell whether the copy they are looking at is the current
        # one.
        lines += [f"`run_id: {run_id}`", ""]
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
    (dest or cfg["paths"]["reports"]).mkdir(parents=True, exist_ok=True)
    ((dest or cfg["paths"]["reports"]) / "data_quality_report.md").write_text(
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


# Tables earlier versions of this pipeline published and no longer do. The
# manifest-based retirement below only reaches tables recorded in a previous
# run, so on a warehouse built before the manifest existed these would survive
# every upgrade - which is exactly the state that prompted it: dq_results and
# adoption_top_products sat there for weeks, answering queries with frozen rows.
#
# An explicit allowlist, not "drop anything unrecognised". A table someone else
# put in this database is not ours to remove, and a one-time migration that
# guesses is worse than one that leaves something behind.
LEGACY_RETIRED_TABLES = ("dq_results", "adoption_top_products")

_INDEXES = (
    "CREATE INDEX IF NOT EXISTS ix_fact_stock ON fact_sales(stock_code)",
    "CREATE INDEX IF NOT EXISTS ix_fact_date ON fact_sales(date_key)",
)


# The three files that make up one report version. Named once so the promote,
# the reader snapshot and the tests cannot disagree about what "complete" means.
REPORT_NAMES = (
    "data_quality_report.md",
    "adoption_report.md",
    "run_metrics.json",
)

# Written into a version directory by the step that publishes the data. Its
# presence is the only thing the finaliser consults, which is what lets the
# finaliser be re-run safely and lets it work without asking Airflow about
# another task's state.
PUBLISHED_MARKER = ".published"


def reports_dir(cfg: dict, run_id: str) -> Path:
    """This run's version directory: reports/runs/<run_id>/.

    Reports used to be written straight to reports/ by the tasks that compute
    them, all of which run before `publish`. A run whose publish then failed
    left last night's warehouse beside tonight's reports - reports describing
    data nobody can query.

    Staging them and moving them across afterwards was not enough either: three
    separate os.replace() calls can half-succeed, and on Windows they routinely
    do, because replacing a file another process holds open raises
    PermissionError. That leaves reports/ with one new report and two old ones
    and nothing recording it. So a version is a DIRECTORY, built complete and
    then pointed at, and the only thing that changes about the published set is
    one line in one file. See publish_version().
    """
    d = cfg["paths"]["reports"] / "runs" / run_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def _version_root(cfg: dict) -> Path:
    return cfg["paths"]["reports"] / "runs"


def _current_file(cfg: dict) -> Path:
    return cfg["paths"]["reports"] / "CURRENT"


def current_run_id(cfg: dict) -> str | None:
    """The run_id reports/CURRENT names, or None if nothing is published."""
    f = _current_file(cfg)
    if not f.exists():
        return None
    return f.read_text(encoding="utf-8").strip() or None


def published_reports(cfg: dict) -> Path | None:
    """The version directory a reader should read. Always complete.

    Everything that consumes these reports programmatically goes through here,
    so it can never observe a half-swapped set: CURRENT names the old version
    or the new one, never a mixture.
    """
    run_id = current_run_id(cfg)
    if run_id is None:
        return None
    d = _version_root(cfg) / run_id
    return d if d.is_dir() else None


def warehouse_run_id(cfg: dict) -> str | None:
    """The run_id the warehouse itself says it holds, or None.

    Written inside load()'s swap transaction, so it is true the instant the
    data is - no window. This is the authority for "did this run publish";
    the marker file below is a fallback for a load() called without a run_id.
    """
    db = cfg["paths"]["warehouse"]
    if not db.exists():
        return None
    try:
        with closing(sqlite3.connect(db, timeout=60.0)) as conn:
            row = conn.execute(
                "SELECT run_id FROM _publication WHERE id = 1"
            ).fetchone()
    except sqlite3.Error:
        return None  # no manifest table: a warehouse from before this existed
    return row[0] if row else None


def mark_published(cfg: dict, run_id: str) -> None:
    """Record that the data for this version reached the warehouse.

    Called immediately after load() commits. The finaliser reads this rather
    than asking Airflow whether the publish task succeeded, which keeps it a
    plain function - testable, and correct when Airflow retries it.
    """
    (reports_dir(cfg, run_id) / PUBLISHED_MARKER).write_text(
        run_id + "\n", encoding="utf-8"
    )


def _snapshot_for_readers(cfg: dict, version: Path) -> None:
    """Copy the published version up to reports/ for people reading on GitHub.

    These top-level copies are a CONVENIENCE, not the contract: the repository
    commits them so nobody has to clone and run the pipeline to see what it
    produces. Nothing in this codebase reads them. CURRENT is the authority,
    and each report names its own run_id, so a copy that failed half way is
    self-identifying rather than quietly wrong.

    Deliberately best-effort: a file somebody has open must not turn a
    successful publish into a failed run.
    """
    for name in REPORT_NAMES:
        src = version / name
        if not src.exists():
            continue
        try:
            shutil.copyfile(src, cfg["paths"]["reports"] / name)
        except OSError as exc:  # pragma: no cover - needs a locked file
            log.warning(
                "Could not refresh the reports/%s copy (%s). reports/CURRENT "
                "still names the published version.",
                name,
                exc,
            )


def publish_version(cfg: dict, run_id: str) -> Path:
    """Make this version the published one, by writing ONE file.

    reports/CURRENT is replaced with os.replace of a temp file - a single
    atomic filesystem operation on POSIX and on Windows. Before the call the
    previous version is published in full; after it, this one is. There is no
    state in between, which is the entire reason a version is a directory
    rather than three files promoted one at a time.
    """
    version = reports_dir(cfg, run_id)
    missing = [n for n in REPORT_NAMES if not (version / n).exists()]
    if missing:
        # Pointing at a version before it is complete is precisely the failure
        # the pointer exists to prevent.
        raise FileNotFoundError(
            "{} is missing {} - refusing to publish an incomplete report "
            "version.".format(version, ", ".join(missing))
        )
    tmp = _current_file(cfg).with_suffix(".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(run_id + "\n", encoding="utf-8")
    os.replace(tmp, _current_file(cfg))
    _snapshot_for_readers(cfg, version)
    log.info("Published report version %s", run_id)
    return version


def keep_failed_reports(cfg: dict, run_id: str) -> Path | None:
    """Park a version that never became current, under reports/failed_runs/.

    The gate's own message says "investigate the source extract", and this is
    what an investigator opens - so it has to survive. It must also never
    become the published set, which is why it moves out of runs/ rather than
    staying somewhere CURRENT could later name.
    """
    version = _version_root(cfg) / run_id
    if not version.is_dir():
        return None
    dest = cfg["paths"]["reports"] / "failed_runs" / run_id
    if dest.exists():
        shutil.rmtree(dest, ignore_errors=True)
    dest.parent.mkdir(parents=True, exist_ok=True)
    os.replace(version, dest)
    log.warning("Reports for the failed run are in %s", dest)
    return dest


def finalize_reports(cfg: dict, run_id: str) -> str:
    """Decide what becomes of this run's version. Safe to run twice.

    Returns "published", "failed" or "noop".

    One function, run once every task has reached a terminal state, rather than
    an archive task on `one_failed`. `one_failed` fires as soon as ANY upstream
    fails, without waiting for the others, so a gate failure could archive the
    version while the adoption branch - which has no upstream and runs in
    parallel - was still computing. Adoption then wrote its report into a
    directory that had already been moved, and nobody ever saw it.

    Idempotent because Airflow retries tasks: if CURRENT already names this run
    there is nothing to publish, and if the version has already been archived
    there is nothing to move.
    """
    version = _version_root(cfg) / run_id
    if current_run_id(cfg) == run_id:
        return "noop"  # already published - this is a retry
    if not version.is_dir():
        return "noop"  # already archived - this is a retry
    # The warehouse first, because its answer commits atomically with the data.
    # The marker file is the fallback, for a load() called without a run_id.
    if warehouse_run_id(cfg) == run_id or (version / PUBLISHED_MARKER).exists():
        publish_version(cfg, run_id)
        return "published"
    keep_failed_reports(cfg, run_id)
    return "failed"


def prune_report_versions(cfg: dict, keep: int = 5) -> list[str]:
    """Keep the newest `keep` versions, plus whatever CURRENT names.

    One directory per run is unbounded otherwise. CURRENT is protected
    explicitly rather than by assuming it is the newest - a run that fails
    after a successful one leaves a newer directory that is not published.
    """
    root = _version_root(cfg)
    if not root.is_dir():
        return []
    protected = {current_run_id(cfg)}
    versions = sorted(
        (d for d in root.iterdir() if d.is_dir()),
        key=lambda d: d.stat().st_mtime,
        reverse=True,
    )
    removed = []
    for d in versions[keep:]:
        if d.name in protected:
            continue
        shutil.rmtree(d, ignore_errors=True)
        removed.append(d.name)
    return removed


def load(tables: dict[str, pd.DataFrame], cfg: dict, run_id: str | None = None) -> None:
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
                had_manifest = conn.execute(
                    "SELECT count(*) FROM sqlite_master "
                    "WHERE type='table' AND name='_published'"
                ).fetchone()[0]
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS _published (name TEXT PRIMARY KEY)"
                )
                previous = {r[0] for r in conn.execute("SELECT name FROM _published")}
                if not had_manifest:
                    # First run against a warehouse that predates the manifest.
                    # Seed it with the legacy names so they go out through the
                    # normal retirement path below rather than a separate one.
                    previous |= set(LEGACY_RETIRED_TABLES)
                present = {
                    r[0]
                    for r in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                # Two sets, because the two layers can disagree about what is
                # still there. `retired` is what this pipeline once published
                # and no longer does - the authority for BOTH layers. The
                # SQLite DROP is separately restricted to what SQLite actually
                # has, since a table can be absent there (a rebuilt or deleted
                # warehouse.db) while its Parquet file is still sitting in the
                # analytics directory. Intersecting before deciding what to
                # retire made SQLite's state decide the Parquet layer's, so
                # dq_results.parquet survived every upgrade of a warehouse
                # whose database had been rebuilt - and a Power BI folder
                # source reads that directory, not the database.
                retired = sorted(previous - set(tables))
                for gone in retired:
                    if gone in present:
                        conn.execute(f'DROP TABLE IF EXISTS "{gone}"')
                    log.info("Retired %s - no longer published", gone)
                conn.execute("DELETE FROM _published")
                conn.executemany(
                    "INSERT INTO _published(name) VALUES (?)",
                    [(n,) for n in sorted(tables)],
                )
                # Which run this data is, recorded INSIDE the swap transaction.
                #
                # The report finaliser has to answer "did this run's data reach
                # the warehouse", and it used to answer it from a file written
                # just after the commit - so a crash in between left the
                # warehouse ahead of reports/CURRENT with nothing recording it.
                # Stamped here, the answer commits atomically with the data it
                # describes, and a warehouse/report mismatch becomes something
                # you can detect rather than something you discover.
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS _publication "
                    "(id INTEGER PRIMARY KEY CHECK (id = 1), "
                    "run_id TEXT, published_at TEXT)"
                )
                conn.execute("DELETE FROM _publication")
                conn.execute(
                    "INSERT INTO _publication(id, run_id, published_at) "
                    "VALUES (1, ?, ?)",
                    (run_id, datetime.now(timezone.utc).isoformat(timespec="seconds")),
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
        # Unconditional: `retired` is what we no longer publish, not what
        # SQLite happened to still hold.
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
    compute_seconds: float,
    dest: Path | None = None,
    run_id: str | None = None,
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
        # First, so the version this file describes is the first thing read.
        "run_id": run_id,
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
        # Renamed from runtime_seconds, because what it measures changed. This
        # file is written before the publish now - it has to be, or the data
        # can move without a complete version describing it - so it can only
        # ever cover the compute. The publish is a few seconds more; the log
        # line at the end of run() reports the total.
        "compute_seconds": round(compute_seconds, 1),
    }
    (dest or cfg["paths"]["reports"]).mkdir(parents=True, exist_ok=True)
    ((dest or cfg["paths"]["reports"]) / "run_metrics.json").write_text(
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

    # Same order as the DAG, deliberately. The version is built COMPLETE
    # first, and only then is the data allowed to move; see the comment at
    # load() below.
    run_id = f"local_{datetime.now(timezone.utc):%Y%m%dT%H%M%S%f}"
    version = reports_dir(cfg, run_id)
    try:
        log.info("--- 1/5 extract")
        raw = extract(cfg)
        log.info("--- 2/5 data quality")
        clean, quarantine, results = check_quality(
            raw, cfg, reports_dest=version, run_id=run_id
        )
        write_quality_report(
            results,
            cfg,
            len(raw),
            len(clean),
            len(quarantine),
            dest=version,
            run_id=run_id,
        )
        log.info("--- 3/5 transform")
        tables = transform(clean)
        log.info("--- 4/5 recommend")
        recs = recommend(tables, cfg)
        log.info("--- 5/5 adoption")
        adoption = measure_adoption(cfg, reports_dest=version, run_id=run_id)

        # The last file of the version, and still BEFORE load(). This used to
        # run after the load had committed, so a failure writing it - a full
        # disk, a locked file - left the warehouse holding tonight's data,
        # CURRENT still naming last night's reports, and tonight's incomplete
        # version in failed_runs. The data had moved and every report
        # describing it had been thrown away.
        #
        # Nothing here needs the load to have happened: the counts, the frames
        # and the input digests all exist already. So the rule is simply that
        # the version is complete before the data moves, which is the same
        # order the DAG runs its tasks in.
        metrics = write_run_metrics(
            cfg,
            n_raw=len(raw),
            n_quarantined=len(quarantine),
            n_clean=len(clean),
            n_products=len(tables["dim_product"]),
            recs=recs,
            adoption_headline=adoption["adoption_headline"],
            compute_seconds=time.perf_counter() - started,
            dest=version,
            run_id=run_id,
        )

        load(
            {**tables, "quarantine": quarantine, "recommendations": recs, **adoption},
            cfg,
            run_id=run_id,
        )
        # Belt and braces. load() has already stamped the run_id inside its
        # own transaction, which is what finalize_reports actually reads; this
        # file is the fallback for a load() called without one.
        mark_published(cfg, run_id)
    except BaseException:
        # Whatever was computed before the failure is still worth reading, so
        # the version is parked under reports/failed_runs/ - it never becomes
        # something CURRENT can name. finalize_reports rather than
        # keep_failed_reports, so both entry points take the same decision in
        # the same place.
        finalize_reports(cfg, run_id)
        shutil.rmtree(cfg["paths"]["staging"] / run_id, ignore_errors=True)
        raise

    finalize_reports(cfg, run_id)
    prune_report_versions(cfg)
    shutil.rmtree(cfg["paths"]["staging"] / run_id, ignore_errors=True)
    # Both ids, every run. The two layers are versioned separately and there is
    # no cross-layer transaction, so the one thing an operator needs is to be
    # able to see at a glance whether they agree.
    log.info(
        "Done in %.1fs | warehouse run %s | reports run %s",
        time.perf_counter() - started,
        warehouse_run_id(cfg),
        current_run_id(cfg),
    )
    return metrics


if __name__ == "__main__":
    run()
