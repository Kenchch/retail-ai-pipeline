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

import logging
import shutil
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from retail_pipeline.adoption import measure_adoption
from retail_pipeline.pipeline import (
    check_quality,
    current_run_id,
    extract,
    finalize_reports,
    load,
    load_config,
    mark_published,
    prune_report_versions,
    reports_dir,
    transform,
    warehouse_run_id,
    write_quality_report,
    write_run_metrics,
)
from retail_pipeline.recommend import recommend

log = logging.getLogger("pipeline")

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
    version = reports_dir(cfg, run_id)
    # raises -> the DAG stops here, leaving its GATE FAILED report in this
    # run's version directory for finalize_reports to park under
    # reports/failed_runs/. It is never pointed at by reports/CURRENT.
    clean, quarantine, results = check_quality(
        raw, cfg, reports_dest=version, run_id=run_id
    )
    # Into this run's staging, not reports/. `publish` promotes it; a run that
    # never publishes must not overwrite the report describing the data the
    # warehouse actually holds.
    write_quality_report(
        results,
        cfg,
        len(raw),
        len(clean),
        len(quarantine),
        dest=version,
        run_id=run_id,
    )
    clean.to_parquet(_staging(cfg, "clean", run_id), index=False)
    quarantine.to_parquet(_staging(cfg, "quarantine", run_id), index=False)


def task_transform(**context):
    """Builds the star schema into staging. Publishing is `publish`'s job."""
    cfg = load_config()
    run_id = context["run_id"]
    for name, df in transform(
        _read_staged(cfg, "clean", run_id, "data_quality_gate")
    ).items():
        df.to_parquet(_staging(cfg, name, run_id), index=False)


def task_recommend(**context):
    cfg = load_config()
    run_id = context["run_id"]
    # Reads this run's staged star schema, not the published one. Reading the
    # published copy made tonight's recommendations depend on tonight's facts
    # having already been published, which is what forced the split publish
    # this DAG used to do.
    tables = {
        n: _read_staged(cfg, n, run_id, "transform")
        for n in ("fact_sales", "dim_product")
    }
    recommend(tables, cfg).to_parquet(
        _staging(cfg, "recommendations", run_id), index=False
    )


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
    for name, df in measure_adoption(
        cfg, reports_dest=reports_dir(cfg, run_id), run_id=run_id
    ).items():
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
    names = (
        "fact_sales",
        "dim_product",
        "dim_customer",
        "dim_date",
        "quarantine",
        "recommendations",
        "adoption_headline",
        "adoption_weekly",
        "adoption_by_team",
    )
    produced = {
        "quarantine": "data_quality_gate",
        "recommendations": "build_recommendations",
    }
    tables = {
        n: _read_staged(
            cfg,
            n,
            run_id,
            produced.get(
                n, "measure_adoption" if n.startswith("adoption") else "transform"
            ),
        )
        for n in names
    }
    load(tables, cfg, run_id=run_id)
    # The warehouse now holds this run. That fact is recorded IN the version
    # directory, so finalize_reports can decide what to do with it without
    # asking Airflow about this task's state - which is what makes the
    # finaliser a plain, testable, retry-safe function.
    mark_published(cfg, run_id)


def task_run_metrics(**context):
    """Write run_metrics.json into this run's version directory.

    This task exists because `run()` does not run in production. The DAG wires
    the stage functions up directly, and `run()` was the only place that called
    `_input_fingerprint()` and wrote this file, so every scheduled run refreshed
    the two markdown reports while leaving run_metrics.json frozen at some
    developer's last local run - actively asserting the wrong inputs for every
    night thereafter.

    It reads THIS RUN'S STAGED TABLES, and it runs BEFORE publish. Reading the
    published Parquet after the publish meant the file was assembled from a
    different batch than the two reports beside it: on the first ever run there
    was nothing published to read, and on any run whose publish failed it
    described the previous night's data under this night's date. A version has
    to be complete before it can be pointed at, so every file in it is built
    from the same staged tables.
    """
    cfg = load_config()
    run_id = context["run_id"]
    recs = _read_staged(cfg, "recommendations", run_id, "build_recommendations")
    dim_product = _read_staged(cfg, "dim_product", run_id, "transform")
    fact = _read_staged(cfg, "fact_sales", run_id, "transform")
    quarantine = _read_staged(cfg, "quarantine", run_id, "data_quality_gate")
    headline = _read_staged(cfg, "adoption_headline", run_id, "measure_adoption")
    n_clean, n_q = len(fact), len(quarantine)
    write_run_metrics(
        cfg,
        n_raw=n_clean + n_q,
        n_quarantined=n_q,
        n_clean=n_clean,
        n_products=len(dim_product),
        recs=recs,
        adoption_headline=headline,
        compute_seconds=0.0,  # per-task durations live in the Airflow UI
        dest=reports_dir(cfg, run_id),
        run_id=run_id,
    )


def task_finalize_reports(**context):
    """Publish this run's report version, or archive it. Exactly once, at the end.

    `all_done` on every task that writes into the version directory plus
    `publish`, so it cannot run until all of them have reached a terminal
    state. The previous archiver used `one_failed`, which fires as soon as ANY
    upstream fails without waiting for the others: a gate failure archived the
    version while `measure_adoption` - a root task with no upstream, running in
    parallel - was still computing, and adoption then wrote its report into a
    directory that had already been moved.

    finalize_reports() is idempotent, which matters because this task retries
    like any other.
    """
    cfg = load_config()
    run_id = context["run_id"]
    outcome = finalize_reports(cfg, run_id)
    if outcome == "published":
        prune_report_versions(cfg)
    # The two layers are versioned separately and there is no cross-layer
    # transaction, so every run says which run each of them is on. An operator
    # comparing the two lines can see a mismatch instead of discovering it.
    log.info(
        "finalize_reports: %s | warehouse run %s | reports run %s",
        outcome,
        warehouse_run_id(cfg),
        current_run_id(cfg),
    )
    return outcome


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
    # rmtree, not glob("*.parquet") + rmdir: the run directory also holds a
    # reports/ subdirectory now, and rmdir on a non-empty directory raises.
    shutil.rmtree(d, ignore_errors=True)


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
            shutil.rmtree(d, ignore_errors=True)


with DAG(
    dag_id="retail_ai_pipeline",
    description="Ingest retail transactions, gate on data quality, publish the sales "
    "star schema, product recommendations and adoption reporting",
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": True,
    },
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="0 4 * * *",  # nightly, after end-of-day close
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
    t7 = PythonOperator(
        task_id="write_run_metrics",
        python_callable=task_run_metrics,
        trigger_rule="all_success",
    )
    t10 = PythonOperator(
        task_id="finalize_reports",
        python_callable=task_finalize_reports,
        trigger_rule="all_done",
    )
    t8 = PythonOperator(
        task_id="clear_staging",
        python_callable=task_clear_staging,
        trigger_rule="all_success",
    )
    t9 = PythonOperator(
        task_id="prune_staging",
        python_callable=task_prune_staging,
        trigger_rule="all_done",
    )

    # Everything upstream of `publish` computes into per-run staging and touches
    # nothing a reader can see. `publish` is the only writer, so the warehouse
    # holds one run's output or the previous run's, never a mixture.
    #
    # Adoption stays a separate branch because it computes from telemetry and
    # does not need the extract; it joins at publish rather than publishing
    # itself. clear_staging is all_success - deleting this run's hand-off files
    # after a failure is what made the gate unrecoverable. prune_staging keeps
    # all_done but only removes directories older than STAGING_RETENTION_DAYS.
    # write_run_metrics moved BEFORE publish: every file in a version is built
    # from the same staged tables, so the version is complete before anything
    # can point at it.
    t1 >> t2 >> t3 >> t4
    [t4, t5] >> t7 >> t6 >> t8 >> t9

    # The finaliser waits for every task that writes into the version
    # directory, and for the publish. all_done, so it runs whether they
    # succeeded or not, and only once they are all finished - which is the
    # guarantee `one_failed` could not give.
    [t2, t5, t7, t6] >> t10
