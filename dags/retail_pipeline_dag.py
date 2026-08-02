"""Airflow DAG for the retail pipeline.

The pipeline runs standalone via `python run_pipeline.py`; this DAG is how it
would be scheduled in production. Each stage is a separate task rather than one
big task, because that is what gives per-stage retries, per-stage runtime in the
Airflow UI, and a failure that points at the stage that broke.

The data-quality task is a hard gate: `split_quarantine` raises when the share
of quarantined rows exceeds `quality.max_quarantine_rate`, so a bad upstream
extract stops the DAG *before* anything reaches the warehouse, instead of
quietly publishing thin data.

The equivalent in Azure Data Factory is a pipeline with the same five
activities chained on success, with the quality activity's failure path wired
to an alert - the Python modules would be unchanged.

To run it: put this repository on the Airflow worker's PYTHONPATH (or install
it as a package) and drop this file in $AIRFLOW_HOME/dags/.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from retail_pipeline.config import Config  # noqa: E402
from retail_pipeline.extract import extract  # noqa: E402
from retail_pipeline.load import load  # noqa: E402
from retail_pipeline.quality import run_checks, split_quarantine, write_report  # noqa: E402
from retail_pipeline.recommend import recommend  # noqa: E402
from retail_pipeline.transform import transform  # noqa: E402

CONFIG_PATH = str(REPO_ROOT / "config.yaml")

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}


# --------------------------------------------------------------------------- #
# Task callables.
#
# Dataframes are passed between tasks through the Parquet layer on shared
# storage, not through XCom - XCom is a metadata channel and is the wrong place
# for half a million rows. In a cloud deployment `data/` would be a blob
# container / ADLS path.
# --------------------------------------------------------------------------- #

def _staging_path(cfg: Config, name: str) -> Path:
    return cfg.paths["processed"] / f"_staging_{name}.parquet"


def task_extract(**_) -> None:
    cfg = Config.load(CONFIG_PATH)
    cfg.ensure_dirs()
    extract(cfg).to_parquet(_staging_path(cfg, "raw"), index=False)


def task_quality(**_) -> None:
    import pandas as pd

    cfg = Config.load(CONFIG_PATH)
    raw = pd.read_parquet(_staging_path(cfg, "raw"))
    results, flags = run_checks(raw, cfg)
    clean, quarantine = split_quarantine(raw, flags, cfg)  # raises -> DAG stops here
    write_report(results, cfg, len(raw), len(clean), len(quarantine))
    clean.to_parquet(_staging_path(cfg, "clean"), index=False)
    quarantine.to_parquet(_staging_path(cfg, "quarantine"), index=False)


def task_transform_load(**_) -> None:
    import pandas as pd

    cfg = Config.load(CONFIG_PATH)
    clean = pd.read_parquet(_staging_path(cfg, "clean"))
    tables = transform(clean)
    tables["quarantine"] = pd.read_parquet(_staging_path(cfg, "quarantine"))
    load(tables, cfg)


def task_recommend(**_) -> None:
    import pandas as pd

    cfg = Config.load(CONFIG_PATH)
    tables = {
        "fact_sales": pd.read_parquet(cfg.paths["processed"] / "fact_sales.parquet"),
        "dim_product": pd.read_parquet(cfg.paths["processed"] / "dim_product.parquet"),
    }
    load({"recommendations": recommend(tables, cfg)}, cfg)


with DAG(
    dag_id="retail_ai_pipeline",
    description="Ingest retail transactions, enforce data quality, publish sales star schema and product recommendations",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 4 * * *",   # nightly, after the source system's end-of-day close
    catchup=False,
    max_active_runs=1,
    tags=["retail", "etl", "data-quality", "recommendations"],
) as dag:

    extract_op = PythonOperator(task_id="extract", python_callable=task_extract)
    quality_op = PythonOperator(task_id="data_quality_gate", python_callable=task_quality)
    load_op = PythonOperator(task_id="transform_and_load", python_callable=task_transform_load)
    recommend_op = PythonOperator(task_id="build_recommendations", python_callable=task_recommend)

    extract_op >> quality_op >> load_op >> recommend_op
