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
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from retail_pipeline.adoption import measure_adoption            # noqa: E402
from retail_pipeline.pipeline import (                           # noqa: E402
    check_quality, extract, load, load_config, transform, write_quality_report,
)
from retail_pipeline.recommend import recommend                  # noqa: E402


def _staging(cfg, name: str) -> Path:
    return cfg["paths"]["processed"] / f"_staging_{name}.parquet"


def task_extract(**_):
    cfg = load_config()
    cfg["paths"]["processed"].mkdir(parents=True, exist_ok=True)
    extract(cfg).to_parquet(_staging(cfg, "raw"), index=False)


def task_quality(**_):
    cfg = load_config()
    raw = pd.read_parquet(_staging(cfg, "raw"))
    clean, quarantine, results = check_quality(raw, cfg)   # raises -> DAG stops here
    write_quality_report(results, cfg, len(raw), len(clean), len(quarantine))
    clean.to_parquet(_staging(cfg, "clean"), index=False)
    quarantine.to_parquet(_staging(cfg, "quarantine"), index=False)


def task_transform_load(**_):
    cfg = load_config()
    tables = transform(pd.read_parquet(_staging(cfg, "clean")))
    load({**tables, "quarantine": pd.read_parquet(_staging(cfg, "quarantine"))}, cfg)


def task_recommend(**_):
    cfg = load_config()
    tables = {n: pd.read_parquet(cfg["paths"]["processed"] / f"{n}.parquet")
              for n in ("fact_sales", "dim_product")}
    load({"recommendations": recommend(tables, cfg)}, cfg)


def task_adoption(**_):
    """Separate task with `all_done`: a missing telemetry extract must not hold
    back the merchandising data the morning planogram review depends on."""
    cfg = load_config()
    load(measure_adoption(cfg), cfg)


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
    t3 = PythonOperator(task_id="transform_and_load", python_callable=task_transform_load)
    t4 = PythonOperator(task_id="build_recommendations", python_callable=task_recommend)
    t5 = PythonOperator(task_id="measure_adoption", python_callable=task_adoption,
                        trigger_rule="all_done")

    t1 >> t2 >> t3 >> t4 >> t5
