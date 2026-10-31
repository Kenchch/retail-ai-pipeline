"""Run every stage in pipeline order and log how the data changes shape.

    python scripts/trace_pipeline.py

The trace is a project directory of its own, data/trace/<run_id>/: a copy of
config.yaml with every output pointed there, and a copy of the dbt project. The
stages find it through RETAIL_CONFIG, the way tests/test_dag_run.py points the
DAG at a fixture, so data/runs/, published/, reports/ and dbt/ are never touched
and data/raw/ is only read. At the end the trace's reports are compared with the
committed ones, and its tables with the published data version when there is
one, so the log doubles as a reproducibility check. The dbt step needs
requirements-dbt.txt and is skipped without it. About 2 minutes.

    trace.log          full log: per step, input and output shape, columns,
                       dtypes, nulls, ranges, sample rows, what changed
    dbt_build.log      dbt's own output for the dbt step
    data_lineage.md    one table, one row per step
"""

import contextlib
import datetime as dt
import hashlib
import io
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import textwrap
import time
from importlib.metadata import version as package_version
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bi import build_star_schema
from retail_pipeline import pipeline, publish
from retail_pipeline.adoption import (
    eligible_events,
    load_events,
    measure_adoption,
    resolve_as_of,
)
from retail_pipeline.evaluate import evaluate
from retail_pipeline.recommend import co_purchase_rules, content_fallback, recommend
from scripts import run_dbt
from scripts.check_committed_reports import _committed, _normalise

TOTAL = 11
ANSI = re.compile(r"\x1b\[[0-9;]*m")  # dbt colours its output even when piped

log = logging.getLogger("trace")
lineage = []


# ---------------------------------------------------------------- formatting


def _mark(record):
    record.mark = "" if record.name == "trace" else f"[{record.name}] "
    return True


def setup(out):
    if hasattr(sys.stdout, "reconfigure"):  # a cp1252 console cannot print the log
        sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
    out.mkdir(parents=True)
    # UTC with the date, like the run id and the pipeline's own log lines.
    fmt = logging.Formatter("%(asctime)s  %(mark)s%(message)s", "%Y-%m-%dT%H:%M:%SZ")
    fmt.converter = time.gmtime
    handlers = (
        logging.FileHandler(out / "trace.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    )
    for h in handlers:
        h.setFormatter(fmt)
        h.addFilter(_mark)
    # The stages log through children of "retail_pipeline". Sending it to the
    # same handlers puts what each stage reported into the trace, marked and in
    # order; propagate=False keeps a configured root from printing it twice.
    for name in ("trace", "retail_pipeline"):
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        for h in handlers:
            logger.addHandler(h)


def header(n, total, name, module, what):
    log.info("")
    log.info("=" * 78)
    log.info(f"[{n}/{total}] {name}   ({module})   {what}")
    log.info("=" * 78)


def frame(label, df, sample=3, max_cols=10):
    """Shape, memory, per-column dtype / nulls / range, and a few rows."""
    mem = df.memory_usage(deep=True).sum() / 2**20
    log.info(
        f"  {label}: {len(df):,} rows x {df.shape[1]} columns, {mem:,.1f} MB in memory"
    )
    for c in df.columns:
        col = df[c]
        info = f"{str(col.dtype):<22} nulls {int(col.isna().sum()):>8,}"
        if pd.api.types.is_bool_dtype(col):
            info += f"  True {int(col.fillna(False).sum()):,}"
        elif pd.api.types.is_numeric_dtype(col) or pd.api.types.is_datetime64_any_dtype(
            col
        ):
            if col.notna().any():
                lo, hi = col.min(), col.max()
                if pd.api.types.is_integer_dtype(col):  # ids and years in full
                    lo, hi = int(lo), int(hi)
                elif pd.api.types.is_numeric_dtype(col):
                    lo, hi = f"{lo:,.4g}", f"{hi:,.4g}"
                info += f"  range [{lo}, {hi}]"
        else:
            info += f"  {col.nunique():,} distinct values"
        log.info(f"      {str(c):<24} {info}")
    shown = list(df.columns[:max_cols])
    extra = (
        f"  ({df.shape[1] - len(shown)} columns not shown)"
        if df.shape[1] > max_cols
        else ""
    )
    log.info(f"      first {sample} rows{extra}:")
    log.info(
        textwrap.indent(df[shown].head(sample).to_string(max_colwidth=22), "      | ")
    )


def table(df):
    """A small frame in full: rule results, metrics."""
    log.info(textwrap.indent(df.to_string(index=False, max_colwidth=40), "      | "))


def change(text):
    log.info(f"  -> what changed: {text}")


def record(step, module, inp, out, rows, what):
    lineage.append(
        {
            "step": step,
            "module": module,
            "input": inp,
            "output": out,
            "rows": rows,
            "change": what,
        }
    )


def timed(fn, *args, **kw):
    t0 = time.perf_counter()
    result = fn(*args, **kw)
    log.info(f"  took {time.perf_counter() - t0:,.1f} s")
    return result


def git_tag():
    """Short commit, -dirty when the tree has changes: the code this trace ran."""

    def git(*args):
        return subprocess.run(
            ["git", *args], cwd=ROOT, capture_output=True, text=True, check=False
        ).stdout.strip()

    return (git("rev-parse", "--short", "HEAD") or "nogit") + (
        "-dirty" if git("status", "--porcelain") else ""
    )


def make_project(out):
    """The real config with its outputs under `out` and its inputs left where
    they are, plus a copy of the dbt project: the directory tests/test_dag_run.py
    builds around a fixture feed, built around the real extract instead."""
    real = pipeline.load_config(ROOT / "config.yaml")
    cfg = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    for key in ("raw", "usage_events"):
        cfg["paths"][key] = real["paths"][key].as_posix()
    path = out / "config.yaml"
    path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
    shutil.copytree(
        ROOT / "dbt",
        out / "dbt",
        ignore=shutil.ignore_patterns(
            "target", "logs", "runs", "dbt_packages", "*.duckdb*", "CURRENT.json"
        ),
    )
    os.environ["RETAIL_CONFIG"] = str(path)
    cfg = pipeline.load_config()
    for key in ("processed", "reports"):  # as run() does
        cfg["paths"][key].mkdir(parents=True, exist_ok=True)
    return cfg


# ---------------------------------------------------------------- steps


def step_config(cfg):
    header(
        0,
        TOTAL,
        "config",
        "config.yaml",
        "parameters every stage shares; output paths point into the trace directory",
    )
    for k, v in cfg["paths"].items():
        log.info(f"  paths.{k:<14} {v}")
    for k, v in cfg.items():
        if k != "paths":
            log.info(f"  {k}: {v}")


def step_extract(cfg):
    header(
        1,
        TOTAL,
        "extract",
        "pipeline.py",
        "read the CSV, standardise column names and types, drop no rows",
    )
    path = cfg["paths"]["raw"]
    src = pd.read_csv(path, dtype=str)
    frame(f"raw {path.name} (every column read as a string)", src)
    raw = timed(pipeline.extract, cfg)
    frame("extract() output", raw)
    code = src["StockCode"]
    renamed = code.notna() & (code.str.strip().str.upper() != code)
    log.info(
        "  timestamps that failed to parse (-> NaT): "
        f"{int(raw.invoice_ts.isna().sum()):,} rows; "
        f"customer_id empty: {int(raw.customer_id.isna().sum()):,} rows; "
        f"stock_code changed by trimming and upper-casing: {int(renamed.sum()):,} rows"
    )
    change(
        f"all {len(src):,} rows kept as they are. Columns renamed by column_map; "
        "invoice_ts string -> datetime64; quantity / unit_price -> numeric; "
        "customer_id -> Int64 (nullable integer); invoice_no / stock_code / "
        "description become the string dtype"
    )
    record(
        "extract",
        "pipeline.py",
        f"{path.name}, {len(src):,} rows x {src.shape[1]} columns",
        f"raw, {len(raw):,} rows x {raw.shape[1]} columns",
        "x1 (no rows dropped)",
        "columns renamed; strings -> datetime / numeric / Int64; stock_code normalised",
    )
    return raw


def step_quality(cfg, raw, version, run_id):
    header(
        2,
        TOTAL,
        "data_quality_gate",
        "pipeline.py + credits.py",
        f"{len(pipeline.CHECKS)} rules; blocking ones quarantine rows rather than "
        "drop them; credit-note matches are only flagged",
    )
    clean, quarantine, results = timed(
        pipeline.check_quality, raw, cfg, reports_dest=version, run_id=run_id
    )
    pipeline.write_quality_report(
        results,
        cfg,
        len(raw),
        len(clean),
        len(quarantine),
        dest=version,
        run_id=run_id,
    )
    log.info("  result per rule (a row can break several, so these do not add up):")
    table(results.drop(columns="why"))
    frame("clean (passed every blocking rule)", clean, max_cols=13)
    frame("quarantine (one extra column: reasons)", quarantine)
    multi = int(quarantine.reasons.str.contains(",").sum())
    log.info(
        "  quarantine reasons (top 6 combinations): "
        f"{quarantine.reasons.value_counts().head(6).to_dict()}; "
        f"{multi:,} rows break more than one rule"
    )
    rate, ceiling = len(quarantine) / len(raw), cfg["quality"]["max_quarantine_rate"]
    log.info(
        f"  gate: quarantine rate {rate:.2%} vs ceiling {ceiling:.0%} -> "
        f"{'passed' if rate <= ceiling else 'FAILED'}"
    )
    total = len(clean) + len(quarantine)
    log.info(
        f"  check: clean {len(clean):,} + quarantine {len(quarantine):,} = {total:,} "
        f"vs raw {len(raw):,} -> {'adds up' if total == len(raw) else 'DOES NOT ADD UP'}"
    )
    flagged = clean[clean.reversed_by_credit]
    log.info(
        f"  credit matching: {len(flagged):,} sales lines reversed by "
        f"{flagged.matched_credit_invoice.nunique():,} credit notes, worth GBP "
        f"{(flagged.quantity * flagged.unit_price).sum():,.2f}; still in clean, "
        "only flagged"
    )
    log.info(
        f"  non-blocking rule: the {int(clean.customer_id.isna().sum()):,} clean rows "
        "with no customer_id (guest checkouts) go on to the later steps all the same"
    )
    change(
        f"{len(raw):,} rows split into clean {len(clean):,} + quarantine "
        f"{len(quarantine):,}. clean gains two columns (reversed_by_credit, "
        "matched_credit_invoice): sales reversed by a credit note are flagged, not "
        "dropped; quarantine gains one, reasons, naming the blocking rules a row broke"
    )
    record(
        "data_quality_gate",
        "pipeline.py + credits.py",
        f"raw, {len(raw):,} rows",
        f"clean {len(clean):,} rows, quarantine {len(quarantine):,} rows",
        f"{rate:.2%} quarantined",
        f"{len(pipeline.CHECKS)} rules; quarantined, not dropped; credit matches flagged",
    )
    return clean, quarantine


def step_transform(clean):
    header(
        3,
        TOTAL,
        "transform",
        "pipeline.py",
        "star schema: 1 fact table + 3 dimension tables",
    )
    tables = timed(pipeline.transform, clean)
    for name, df in tables.items():
        frame(name, df, max_cols=11)
    fact, dim_date = tables["fact_sales"], tables["dim_date"]
    closed = dim_date.loc[~dim_date.has_sales, "day_of_week"].value_counts().to_dict()
    log.info(
        f"  check: fact_sales {len(fact):,} rows vs clean {len(clean):,} rows -> "
        f"{'matches' if len(fact) == len(clean) else 'DOES NOT MATCH'}; revenue total "
        f"GBP {fact.revenue.sum():,.2f}; guest rows "
        f"{int(fact.customer_id.isna().sum()):,}"
    )
    log.info(
        f"  dim_date is an unbroken calendar of {len(dim_date):,} days; the "
        f"{int((~dim_date.has_sales).sum())} days with no sales, by weekday: {closed}"
    )
    change(
        "one transaction table split into 1 fact + 3 dimensions. fact_sales is still "
        "one row per sales line, gains date_key and revenue = quantity x unit_price, "
        "and loses description; dim_product is grouped by stock_code and keeps its "
        "most common description; dim_customer holds only customers with an ID; "
        "dim_date is an unbroken calendar, with a row for days without sales too"
    )
    record(
        "transform",
        "pipeline.py",
        f"clean, {len(clean):,} rows",
        ", ".join(f"{n} {len(df):,}" for n, df in tables.items()),
        "1 -> 4 tables",
        "date_key and revenue added; description moved to a dimension; "
        "dimensions grouped by key; unbroken calendar",
    )
    return tables


def step_recommend(cfg, tables):
    header(
        4,
        TOTAL,
        "build_recommendations",
        "recommend.py",
        "co-purchase rules (ranked by lift), description TF-IDF for cold-start "
        "products",
    )
    fact, products, c = tables["fact_sales"], tables["dim_product"], cfg["recommend"]
    sizes = fact.groupby("invoice_no")["stock_code"].nunique()
    cap = c["max_basket_size"]
    log.info(
        f"  baskets (one per invoice): {len(sizes):,}; 1 item "
        f"{int((sizes == 1).sum()):,}, 2..{cap} items "
        f"{int(sizes.between(2, cap).sum()):,} (only these are paired), more than "
        f"{cap} items {int((sizes > cap).sum()):,}; the denominator is every basket"
    )
    top = timed(co_purchase_rules, fact, cfg)
    frame(
        f"co_purchase_rules(): at least {c['min_support_count']} baskets, "
        f"confidence >= {c['min_confidence']}, lift >= {c['min_lift']}, "
        f"top {c['top_n']} per product",
        top,
    )
    cold = timed(content_fallback, products, set(top.stock_code), cfg)
    usable = int((products.description != "UNKNOWN").sum())
    frame(
        f"content_fallback(): of the {usable:,} products with a description, "
        "those without co-purchase rules",
        cold,
    )
    log.info(
        "  recommend() runs both steps above again itself, so the "
        "[retail_pipeline.recommend] lines below repeat:"
    )
    recs = timed(recommend, tables, cfg)
    frame("recommend(): both merged, with the product names on each side", recs)
    by_method = recs.groupby("method").stock_code.nunique().to_dict()
    covered = recs.stock_code.nunique()
    log.info(
        f"  products covered per method {by_method}; catalogue coverage "
        f"{covered / len(products):.1%} ({covered:,} / {len(products):,})"
    )
    change(
        f"fact {len(fact):,} rows -> {len(sizes):,} baskets (invoice -> set of "
        f"products) -> pair counts -> {top.stock_code.nunique():,} products with "
        "co-purchase rules; the other "
        f"{cold.stock_code.nunique() if len(cold) else 0:,} filled in by cosine "
        f"neighbours on description; merged into {len(recs):,} rows, one per "
        "(product, recommended product, rank)"
    )
    record(
        "build_recommendations",
        "recommend.py",
        f"fact_sales {len(fact):,} rows + dim_product {len(products):,} rows",
        f"recommendations {len(recs):,} rows",
        f"{len(sizes):,} baskets -> {len(recs):,} recommendations",
        f"co-purchase rules + TF-IDF fallback; coverage {covered / len(products):.1%}",
    )
    return recs


def step_adoption(cfg, version, run_id):
    header(
        5,
        TOTAL,
        "measure_adoption",
        "adoption.py",
        "usage telemetry -> reach / activation / action rate / CSAT",
    )
    events = load_events(cfg)
    frame(cfg["paths"]["usage_events"].name, events)
    as_of = resolve_as_of(events, cfg)
    eligible = eligible_events(events, as_of)
    log.info(
        f"  as_of {as_of} (the end of the config's analysis_as_of day); events "
        f"before it {len(eligible):,} / {len(events):,}; types "
        f"{eligible.event_type.value_counts().to_dict()}"
    )
    tables = timed(measure_adoption, cfg, reports_dest=version, run_id=run_id)
    for name, df in tables.items():
        frame(name, df, sample=min(len(df), 12), max_cols=12)
    weekly, teams = tables["adoption_weekly"], tables["adoption_by_team"]
    change(
        f"{len(events):,} events (one row per action) -> cut at as_of -> three "
        f"summary tables: headline {len(tables['adoption_headline'])} rows (metric, "
        f"target, status), weekly {len(weekly)} rows (an unbroken run of weeks, with "
        f"a row for weeks nobody used it), by_team {len(teams)} rows (from the "
        "roster, with a row for teams that have no events)"
    )
    record(
        "measure_adoption",
        "adoption.py",
        f"usage_events {len(events):,} rows",
        ", ".join(f"{n} {len(df)}" for n, df in tables.items()),
        f"{len(events):,} -> {sum(len(df) for df in tables.values())}",
        "events -> metrics; unbroken weeks; teams listed from the roster",
    )
    return tables


def step_run_metrics(
    cfg, raw, clean, quarantine, tables, recs, adoption, version, run_id, started
):
    header(
        6,
        TOTAL,
        "write_run_metrics",
        "pipeline.py + doc_figures.py",
        "the report version's last file, written before anything is published",
    )
    metrics = pipeline.write_run_metrics(
        cfg,
        n_raw=len(raw),
        n_quarantined=len(quarantine),
        n_clean=len(clean),
        n_products=len(tables["dim_product"]),
        recs=recs,
        adoption_headline=adoption["adoption_headline"],
        # Includes the trace's own logging; the comparison excuses it.
        compute_seconds=time.perf_counter() - started,
        dest=version,
        run_id=run_id,
    )
    log.info("  run_metrics.json:")
    log.info(textwrap.indent(json.dumps(metrics, indent=2, default=str), "      | "))
    figures = json.loads((version / "doc_figures.json").read_text(encoding="utf-8"))
    log.info(f"  doc_figures.json: {figures}")
    change(
        "no new tables. The earlier steps' row counts, recommendation coverage, "
        "adoption rates and the fingerprints of the two input files go into "
        "run_metrics.json; only now is the report version complete, and only after "
        "this may the data be published"
    )
    record(
        "write_run_metrics",
        "pipeline.py + doc_figures.py",
        "the results of the first 5 steps",
        "run_metrics.json, doc_figures.json",
        "-",
        "counts + input fingerprints; data published only once the reports are whole",
    )


def step_publish(cfg, tables, quarantine, recs, adoption, run_id):
    header(
        7,
        TOTAL,
        "publish",
        "pipeline.py + publish.py",
        "the only step that writes the warehouse: one version directory, "
        "Parquet + SQLite, then the pointer moves",
    )
    payload = {**tables, "quarantine": quarantine, "recommendations": recs, **adoption}
    timed(pipeline.load, payload, cfg, run_id=run_id)
    pipeline.mark_published(cfg, run_id)
    data = publish.published_data_dir(cfg)
    db = data / publish.WAREHOUSE_FILE
    with contextlib.closing(sqlite3.connect(db)) as conn:
        names = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        ]
        counts = {
            n: conn.execute(
                f"SELECT count(*) FROM {pipeline.quote_ident(n)}"
            ).fetchone()[0]
            for n in names
        }
        stamp = conn.execute("SELECT run_id, published_at FROM _publication").fetchone()
    log.info(
        f"  data version {data.relative_to(cfg['paths']['data_runs'].parent)} "
        "(data/CURRENT points to it):"
    )
    for name in sorted(payload):
        p = data / f"{name}.parquet"
        rows = pq.ParquetFile(p).metadata.num_rows
        ok = rows == counts.get(name) == len(payload[name])
        log.info(
            f"      {name:<20} parquet {rows:>8,} rows {p.stat().st_size / 2**20:>6,.1f} MB"
            f" | sqlite {counts.get(name, 0):>8,} rows | {'match' if ok else 'DO NOT MATCH'}"
        )
    log.info(
        f"  retail.db {db.stat().st_size / 2**20:,.1f} MB; plus the metadata tables "
        f"{[n for n in names if n not in payload]}; _publication = {stamp}"
    )
    change(
        f"{len(payload)} tables, each written once as Parquet and once into SQLite in "
        "the same version directory; SQLite swaps the tables in one transaction, then "
        "records the publication list (_published) and the run_id stamp "
        "(_publication); once those check out, the one pointer data/CURRENT moves"
    )
    record(
        "publish",
        "pipeline.py + publish.py",
        f"{len(payload)} tables (in memory)",
        f"data/runs/{data.name}/: {len(payload)} parquet files + retail.db",
        "unchanged",
        "Parquet + SQLite in one version; one transaction; the pointer moves",
    )


def step_finalize(cfg, run_id):
    header(
        8,
        TOTAL,
        "finalize_reports",
        "publish.py",
        "the report version becomes current, bound to the data version",
    )
    outcome = publish.finalize_reports(cfg, run_id)
    publish.prune_report_versions(cfg)
    log.info(
        f"  finalize_reports: {outcome} | warehouse run {publish.warehouse_run_id(cfg)} "
        f"| reports run {publish.current_run_id(cfg)}"
    )
    log.info(f"  published/CURRENT.json: {publish.published_manifest(cfg)}")
    for p in sorted(publish.published_reports(cfg).iterdir()):
        log.info(f"      {p.name:<26} {p.stat().st_size:>8,} bytes")
    change(
        "the data is not touched. reports/CURRENT points to this report version (and "
        "a copy goes to the top level of reports/); published/CURRENT.json records the "
        "data version and the report version together, and downstream (dbt, Power BI) "
        "reads only that"
    )
    record(
        "finalize_reports",
        "publish.py",
        f"reports/runs/{run_id}/",
        "published/CURRENT.json",
        "-",
        "one file binds the data version to the report version",
    )


def step_dbt(cfg, run_id, out):
    header(
        9,
        TOTAL,
        "dbt_build",
        "scripts/run_dbt.py + dbt/",
        "build the DuckDB marts from the published version and run the data tests",
    )
    try:
        run_dbt._dbt_executable()
    except FileNotFoundError:
        log.info(
            "  dbt is not installed (pip install -r requirements-dbt.txt): skipped"
        )
        record("dbt_build", "scripts/run_dbt.py", "-", "-", "-", "skipped: no dbt")
        return

    real_run = subprocess.run
    lines = []

    def spy(cmd, check=False, **kw):
        res = real_run(
            cmd,
            **kw,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        text = ANSI.sub("", res.stdout + res.stderr)
        (out / "dbt_build.log").write_text(text, encoding="utf-8")
        lines.extend(text.splitlines())
        if check and res.returncode:
            for line in lines[-15:]:
                log.info(f"      | {line}")
            res.check_returncode()
        return res

    run_dbt.subprocess.run = spy
    try:
        timed(run_dbt.main, expected_run_id=run_id)
    finally:
        run_dbt.subprocess.run = real_run
    done = next((ln for ln in reversed(lines) if "Done." in ln), "")
    models = sum("OK created" in ln for ln in lines)
    tests = sum(" PASS " in ln for ln in lines)
    log.info(f"  dbt: {models} models built, {tests} tests passed; {done.strip()}")
    log.info(f"  full output in {out / 'dbt_build.log'}")

    import duckdb

    pointer = json.loads((cfg["paths"]["dbt"] / "CURRENT.json").read_text("utf-8"))
    log.info(f"  dbt/CURRENT.json: {pointer}")
    marts = {}
    with contextlib.closing(
        duckdb.connect(str(cfg["paths"]["dbt"] / pointer["database"]), read_only=True)
    ) as con:
        objects = con.execute(
            "SELECT table_schema, table_name, table_type FROM information_schema.tables "
            "ORDER BY table_name"
        ).fetchall()
        for schema, name, kind in objects:
            df = con.execute(f'SELECT * FROM "{schema}"."{name}"').df()
            log.info(
                f"      {name:<24} {kind:<10} {len(df):>8,} rows x {df.shape[1]} columns"
            )
            if name.startswith("mart_"):
                marts[name] = df
    for name, df in marts.items():
        frame(name, df, sample=2, max_cols=12)
    change(
        "dbt finds this publication's Parquet through published/CURRENT.json: "
        "staging views rename and select columns -> intermediate computes the "
        "measures -> 4 mart tables (by day / product / customer, and "
        "recommendations), written into a new DuckDB file; dbt/CURRENT.json moves "
        "only once every test passes"
    )
    record(
        "dbt_build",
        "scripts/run_dbt.py + dbt/",
        "the published Parquet + reports",
        ", ".join(f"{n} {len(df):,}" for n, df in marts.items()),
        "aggregated",
        f"{models} models, {tests} tests",
    )


def step_bi(out, tables):
    header(
        10,
        TOTAL,
        "bi semantic model",
        "bi/build_star_schema.py",
        "warehouse star schema -> Power BI semantic model: surrogate keys, "
        "unknown members, whole-year calendar",
    )
    model = out / "bi" / "model"
    real_out, build_star_schema.OUT = build_star_schema.OUT, model
    printed = io.StringIO()
    try:
        with contextlib.redirect_stdout(printed):
            timed(build_star_schema.main)
    finally:
        build_star_schema.OUT = real_out
    log.info("  build_star_schema's own output:")
    log.info(textwrap.indent(printed.getvalue().strip(), "      | ", lambda _: True))
    built = {p.stem: pd.read_csv(p) for p in sorted(model.glob("*.csv"))}
    for name, df in built.items():
        frame(f"{name}.csv", df, sample=2, max_cols=12)
    log.info(
        f"  against the warehouse: fact_sales {len(tables['fact_sales']):,} -> "
        f"{len(built['fact_sales']):,} rows; dim_customer "
        f"{len(tables['dim_customer']):,} -> {len(built['dim_customer']):,} (-1 Guest "
        f"added); dim_product {len(tables['dim_product']):,} -> "
        f"{len(built['dim_product']):,} (-1 Non-product added, which takes every "
        "quarantined code that is not a product); dim_date "
        f"{len(tables['dim_date']):,} -> {len(built['dim_date']):,} days (filled out "
        "to whole years)"
    )
    change(
        "natural keys become integer surrogate keys; a missing foreign key points to "
        "the -1 unknown member instead of a null; the calendar is filled out to whole "
        "years, with sort columns; the dimensions lose their aggregate columns; "
        "fact_quarantine and a rule bridge table are added; written as CSV for "
        "Power BI"
    )
    record(
        "bi semantic model",
        "bi/build_star_schema.py",
        "the published Parquet",
        f"{len(built)} CSVs",
        f"fact_sales {len(built['fact_sales']):,} rows",
        "surrogate keys; -1 unknown members; whole-year calendar; dimensions without "
        "aggregates",
    )


def step_evaluate(cfg, clean, out):
    header(
        11,
        TOTAL,
        "evaluate",
        "evaluate.py",
        "offline evaluation: split by time, measure hit-rate@5",
    )
    result = timed(evaluate, clean, cfg)
    # The two fields evaluate.main() adds, so the file compares with the committed one
    result["source_sha256"] = hashlib.sha256(
        cfg["paths"]["raw"].read_bytes()
    ).hexdigest()
    result["environment"] = {
        "pandas": pd.__version__,
        "scikit-learn": package_version("scikit-learn"),
    }
    (out / "evaluation.json").write_text(
        json.dumps(result, indent=2, allow_nan=False), encoding="utf-8"
    )
    log.info(
        f"  training {result['training_rows']:,} rows (to "
        f"{result['training_last_timestamp']}), test {result['test_rows']:,} rows "
        f"(from {result['test_first_timestamp']}); the "
        f"{result['cross_boundary_invoices_excluded']} invoices that straddle the "
        "cutoff are in neither"
    )
    models = pd.DataFrame.from_dict(result["models"], orient="index")
    table(models.rename_axis("model").reset_index())
    hybrid = result["models"]["hybrid"]
    change(
        f"clean split at {result['cutoff']}; the training part goes through "
        "transform + recommend again, the test part becomes baskets by invoice; each "
        f"product in a basket is one query ({hybrid['queries']:,} in all), a hit when "
        f"the top {result['k']} recommendations include another product from the "
        "same basket; compared with a most-popular baseline"
    )
    record(
        "evaluate",
        "evaluate.py",
        f"clean, {len(clean):,} rows",
        "evaluation.json",
        f"training {result['training_rows']:,} / test {result['test_rows']:,}",
        f"hit-rate@{result['k']} {hybrid['hit_rate_at_k']:.1%} vs most-popular "
        f"{result['models']['most_popular']['hit_rate_at_k']:.1%}",
    )


def step_not_run():
    log.info("")
    log.info("=" * 78)
    log.info("Scripts not run (they do not change the data)")
    log.info("=" * 78)
    for script, why in (
        (
            "check_cancellations.py",
            "reconciles the credit evidence; step 2 logged the match counts",
        ),
        (
            "check_revenue_bridge.py",
            "compares the positive-sale selection with the R version's",
        ),
        (
            "verify_doc_figures.py",
            "recomputes doc_figures.json; step 6 already wrote one",
        ),
        ("build_workshop.py", "refreshes the workshop slides"),
    ):
        log.info(f"  scripts/{script}: {why}")


# ---------------------------------------------------------------- checks


def _first_difference(a, b):
    for i, (x, y) in enumerate(zip(a, b)):
        if x != y:
            return i, x, y
    i = min(len(a), len(b))
    return i, (a[i:] or ["<end>"])[0], (b[i:] or ["<end>"])[0]


def compare_with_committed(cfg, out):
    log.info("")
    log.info("=" * 78)
    log.info(
        "Reproducibility check: the trace's outputs vs the committed reports and the "
        "published data"
    )
    log.info("=" * 78)
    version = publish.published_reports(cfg)
    pairs = [(version / n, n) for n in (*publish.REPORT_NAMES, "doc_figures.json")]
    pairs.append((out / "evaluation.json", "evaluation.json"))
    for new, name in pairs:
        committed = _committed(ROOT / "reports" / name)
        if committed is None:
            log.info(f"  reports/{name}: not in HEAD")
            continue
        a = _normalise(new.read_text(encoding="utf-8")).splitlines()
        b = _normalise(committed).splitlines()
        if a == b:
            log.info(
                f"  reports/{name}: matches (apart from run_id and compute_seconds)"
            )
        else:
            i, x, y = _first_difference(a, b)
            log.info(
                f"  reports/{name}: DIFFERS at line {i + 1}: trace {x!r} vs committed {y!r}"
            )

    real = publish.published_data_dir(pipeline.load_config(ROOT / "config.yaml"))
    if real is None:
        log.info(
            "  no published data version under data/ yet, so the tables are not "
            "compared (run this again after python -m retail_pipeline.pipeline)"
        )
        return
    log.info(f"  published data version: {real.name}")
    for new in sorted(publish.published_data_dir(cfg).glob("*.parquet")):
        old = real / new.name
        if not old.exists():
            log.info(f"  {new.name}: not in the published version")
            continue
        a, b = pd.read_parquet(new), pd.read_parquet(old)
        if a.equals(b):
            log.info(f"  {new.name}: identical ({len(a):,} rows)")
            continue
        try:
            pd.testing.assert_frame_equal(a, b, check_exact=False, rtol=1e-9)
            log.info(f"  {new.name}: matches within 1e-9 ({len(a):,} rows)")
        except AssertionError as e:
            log.info(f"  {new.name}: DIFFERS: {str(e).splitlines()[0]}")


def write_lineage(out):
    lines = [
        "# Data lineage (trace run)",
        "",
        "| Step | Module | Input | Output | Rows | What happened to the structure |",
        "|---|---|---|---|---|---|",
    ]
    lines += [
        f"| {r['step']} | `{r['module']}` | {r['input']} | {r['output']} | "
        f"{r['rows']} | {r['change']} |"
        for r in lineage
    ]
    (out / "data_lineage.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"trace_{stamp}_{git_tag()}"
    out = ROOT / "data" / "trace" / run_id
    setup(out)
    log.info(f"trace run {run_id}; everything is written under {out}")

    t0 = time.perf_counter()
    try:
        _run(out, run_id, t0)
    except BaseException:
        # Into trace.log as well as onto the screen: the last header above this
        # line is the step that failed.
        log.exception(f"trace failed after {time.perf_counter() - t0:,.0f} s")
        raise


def _run(out, run_id, t0):
    cfg = make_project(out)
    step_config(cfg)
    version = publish.reports_dir(cfg, run_id)
    raw = step_extract(cfg)
    clean, quarantine = step_quality(cfg, raw, version, run_id)
    tables = step_transform(clean)
    recs = step_recommend(cfg, tables)
    adoption = step_adoption(cfg, version, run_id)
    step_run_metrics(
        cfg, raw, clean, quarantine, tables, recs, adoption, version, run_id, t0
    )
    step_publish(cfg, tables, quarantine, recs, adoption, run_id)
    step_finalize(cfg, run_id)
    step_dbt(cfg, run_id, out)
    step_bi(out, tables)
    step_evaluate(cfg, clean, out)
    step_not_run()
    write_lineage(out)
    compare_with_committed(cfg, out)
    log.info("")
    log.info(f"done in {time.perf_counter() - t0:,.0f} s; log {out / 'trace.log'}")


if __name__ == "__main__":
    main()
