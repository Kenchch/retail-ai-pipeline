"""Airflow DAG - how the pipeline is scheduled in production.

Each stage is its own task rather than one big one, because that is what gives
per-stage retries, per-stage runtime in the UI, and a failure that points at the
stage that broke. The quality task is a hard gate: it raises when too many rows
are quarantined, so a bad upstream extract stops the run *before* anything
reaches the warehouse, leaving last night's data intact.

Dataframes move between tasks through the Parquet layer on shared storage, not
through XCom - XCom is a metadata channel and the wrong place for half a million
rows. In a cloud deployment `data/` would be a blob container or ADLS path.

The Azure Data Factory equivalent is the same activities chained on success with
the quality activity's failure path wired to an alert; the Python is unchanged.

To run: put this repo on the worker's PYTHONPATH and drop this file in
$AIRFLOW_HOME/dags/.
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from retail_pipeline.adoption import measure_adoption            # noqa: E402
from retail_pipeline.pipeline import (                           # noqa: E402
    check_quality, extract, load, load_config, transform, write_quality_report,
    write_run_metrics,
)
from retail_pipeline.recommend import recommend                  # noqa: E402


STAGING_RETENTION_DAYS = 7


def _staging(cfg, name: str, run_id: str) -> Path:
    """Hand-off files live OUTSIDE the published layer, and inside a per-run
    directory.

    data/processed is the analytics layer, and anything that enumerates it -
    a Spark job, a Power BI folder source - treats every parquet in it as a
    published table. Writing the pre-quality extract there would publish rows
    the quality gate exists to reject, and would leave them there if the gate
    stops the run.

    The per-run subdirectory is what makes recovery possible. These files used
    to be shared across runs and deleted on `all_done`, so a failed gate took
    `raw.parquet` down with it - and the operator's normal remedy (fix the
    source, clear the failed task, re-run) then died with a bare
    FileNotFoundError pointing at the staging path. Recovering meant knowing to
    clear `extract` as well, i.e. re-reading the whole feed, which is precisely
    what staging exists to avoid.
    """
    d = cfg["paths"]["staging"] / run_id
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{name}.parquet"


def _read_staged(cfg, name: str, run_id: str, produced_by: str) -> pd.DataFrame:
    """Read a hand-off file, naming the task that should have produced it."""
    p = _staging(cfg, name, run_id)
    if not p.exists():
        raise FileNotFoundError(
            f"{p} is missing. It is produced by the `{produced_by}` task; clear and "
            f"re-run that task as well (staging is per-run, so a previous run's copy "
            f"is deliberately not reused)."
        )
    return pd.read_parquet(p)


def task_extract(**context):
    cfg = load_config()
    cfg["paths"]["processed"].mkdir(parents=True, exist_ok=True)
    extract(cfg).to_parquet(_staging(cfg, "raw", context["run_id"]), index=False)


def task_quality(**context):
    cfg = load_config()
    run_id = context["run_id"]
    raw = _read_staged(cfg, "raw", run_id, "extract")
    clean, quarantine, results = check_quality(raw, cfg)   # raises -> DAG stops here
    write_quality_report(results, cfg, len(raw), len(clean), len(quarantine))
    clean.to_parquet(_staging(cfg, "clean", run_id), index=False)
    quarantine.to_parquet(_staging(cfg, "quarantine", run_id), index=False)


def task_transform(**context):
    """Builds the star schema into staging. Publishing is `publish`'s job."""
    cfg = load_config()
    run_id = context["run_id"]
    for name, df in transform(_read_staged(cfg, "clean", run_id, "data_quality_gate")).items():
        df.to_parquet(_staging(cfg, name, run_id), index=False)


def task_recommend(**context):
    cfg = load_config()
    run_id = context["run_id"]
    # Reads this run's staged star schema, not the published one. Reading the
    # published copy made tonight's recommendations depend on tonight's facts
    # having already been published, which is what forced the split publish
    # this DAG used to do.
    tables = {n: _read_staged(cfg, n, run_id, "transform")
              for n in ("fact_sales", "dim_product")}
    recommend(tables, cfg).to_parquet(_staging(cfg, "recommendations", run_id), index=False)


def task_adoption(**context):
    """Adoption computes on its own root branch, because it reads the usage
    telemetry and nothing the extract produces - so a slow or failing extract
    should not stop it being calculated. It still publishes with everything
    else, in `publish`.

    That is a deliberate trade. Independent branches and an atomic publish
    cannot both be had without versioning the warehouse: either a telemetry
    failure can hold back the sales refresh, or the two can disagree about
    which run they came from. This picks the first, because the README promises
    the second cannot happen."""
    cfg = load_config()
    run_id = context["run_id"]
    for name, df in measure_adoption(cfg).items():
        df.to_parquet(_staging(cfg, name, run_id), index=False)


def task_publish(**context):
    """The only task that writes to the warehouse.

    Splitting the publish across transform, recommend and adoption meant a
    failure between them left the warehouse holding tonight's fact_sales beside
    last night's recommendations, with nothing recording that it had happened -
    and the recommendations referencing stock codes that tonight's dim_product
    no longer contained. Both the README ("a broken extract leaves last night's
    data intact") and pipeline.run(), which has always published in a single
    call, described behaviour this DAG did not have.

    Every table is now written in one load(), which is itself one SQLite
    transaction, so the warehouse only ever holds one run's output."""
    cfg = load_config()
    run_id = context["run_id"]
    names = ("fact_sales", "dim_product", "dim_customer", "dim_date", "quarantine",
             "recommendations", "adoption_headline", "adoption_weekly", "adoption_by_team")
    produced = {"quarantine": "data_quality_gate", "recommendations": "build_recommendations"}
    tables = {
        n: _read_staged(cfg, n, run_id,
                        produced.get(n, "measure_adoption" if n.startswith("adoption") else "transform"))
        for n in names
    }
    load(tables, cfg)


def task_run_metrics(**_):
    """Write reports/run_metrics.json - the machine-readable provenance record.

    This task exists because `run()` does not run in production. The DAG wires
    the stage functions up directly, and `run()` was the only place that called
    `_input_fingerprint()` and wrote this file, so every scheduled run refreshed
    the two markdown reports while leaving run_metrics.json frozen at some
    developer's last local run - actively asserting the wrong inputs for every
    night thereafter.

    `all_success` deliberately: a metrics file describing a run that did not
    publish would recreate the same problem in a new form.
    """
    cfg = load_config()
    recs = pd.read_parquet(cfg["paths"]["processed"] / "recommendations.parquet")
    dim_product = pd.read_parquet(cfg["paths"]["processed"] / "dim_product.parquet")
    fact = pd.read_parquet(cfg["paths"]["processed"] / "fact_sales.parquet",
                           columns=["invoice_no"])
    quarantine = pd.read_parquet(cfg["paths"]["processed"] / "quarantine.parquet",
                                 columns=["invoice_no"])
    headline = pd.read_parquet(cfg["paths"]["processed"] / "adoption_headline.parquet")
    n_clean, n_q = len(fact), len(quarantine)
    write_run_metrics(
        cfg, n_raw=n_clean + n_q, n_quarantined=n_q, n_clean=n_clean,
        n_products=len(dim_product), recs=recs, adoption_headline=headline,
        runtime_seconds=0.0,   # per-task durations live in the Airflow UI
    )


def task_clear_staging(**context):
    """Remove THIS run's hand-off files, on `all_success` only.

    `all_done` here was a trap: when the gate failed, t3/t4 became
    upstream_failed (a done state), this task fired, and it deleted the very
    `raw.parquet` the operator needed to re-run the gate against. Hygiene is
    handled instead by task_prune_staging below, which runs on `all_done` and
    only touches directories old enough that no run could still be recovering
    from them.
    """
    cfg = load_config()
    d = cfg["paths"]["staging"] / context["run_id"]
    if d.exists():
        for f in d.glob("*.parquet"):
            f.unlink()
        d.rmdir()


def task_prune_staging(**_):
    """Age-based cleanup, on `all_done`, so a failed run still leaves its own
    hand-off files behind for long enough to be re-run from."""
    cfg = load_config()
    root = cfg["paths"]["staging"]
    if not root.exists():
        return
    cutoff = time.time() - STAGING_RETENTION_DAYS * 86400
    for d in root.iterdir():
        if d.is_dir() and d.stat().st_mtime < cutoff:
            for f in d.glob("*.parquet"):
                f.unlink()
            d.rmdir()


with DAG(
    dag_id="retail_ai_pipeline",
    description="Ingest retail transactions, gate on data quality, publish the sales "
                "star schema, product recommendations and adoption reporting",
    default_args={"owner": "data-platform", "retries": 2,
                  "retry_delay": timedelta(minutes=5), "email_on_failure": True},
    start_date=datetime(2026, 1, 1),
    schedule="0 4 * * *",          # nightly, after end-of-day close
    catchup=False,
    max_active_runs=1,
    tags=["retail", "etl", "data-quality", "recommendations"],
) as dag:
    t1 = PythonOperator(task_id="extract", python_callable=task_extract)
    t2 = PythonOperator(task_id="data_quality_gate", python_callable=task_quality)
    t3 = PythonOperator(task_id="transform", python_callable=task_transform)
    t4 = PythonOperator(task_id="build_recommendations", python_callable=task_recommend)
    t5 = PythonOperator(task_id="measure_adoption", python_callable=task_adoption)
    t6 = PythonOperator(task_id="publish", python_callable=task_publish)
    t7 = PythonOperator(task_id="write_run_metrics", python_callable=task_run_metrics,
                        trigger_rule="all_success")
    t8 = PythonOperator(task_id="clear_staging", python_callable=task_clear_staging,
                        trigger_rule="all_success")
    t9 = PythonOperator(task_id="prune_staging", python_callable=task_prune_staging,
                        trigger_rule="all_done")

    # Everything upstream of `publish` computes into per-run staging and touches
    # nothing a reader can see. `publish` is the only writer, so the warehouse
    # holds one run's output or the previous run's, never a mixture.
    #
    # Adoption stays a separate branch because it computes from telemetry and
    # does not need the extract; it joins at publish rather than publishing
    # itself. clear_staging is all_success - deleting this run's hand-off files
    # after a failure is what made the gate unrecoverable. prune_staging keeps
    # all_done but only removes directories older than STAGING_RETENTION_DAYS.
    t1 >> t2 >> t3 >> t4
    [t4, t5] >> t6 >> t7 >> t8 >> t9
