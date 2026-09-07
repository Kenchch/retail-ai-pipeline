"""Execute the DAG, rather than only importing it.

`test_dag.py` asserts the wiring: which tasks exist, how they are joined, what
trigger rules they carry. It never runs them, so every claim the DAG makes
about what it *produces* -- staged hand-offs, an atomic publish, a version that
`reports/CURRENT` points at -- rested on the stage functions being tested
separately and the DAG being assumed to call them correctly.

This runs the whole thing on a fixture through `dag.test()`, which executes
tasks in-process against a temporary metadata database, and then checks the
artefacts on disk.

Nothing is stubbed, including dbt: it builds the mart and runs its data tests
against the run this DAG just published, which is the contract the pipeline
exists to satisfy.
"""

from __future__ import annotations

import csv
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytest.importorskip("airflow")

ROOT = Path(__file__).resolve().parents[1]

RETAIL_HEADER = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
]

# Four products that co-occur, so the recommender has pairs to find rather than
# a catalogue of singletons.
PRODUCTS = [
    ("85123A", "WHITE HANGING HEART T-LIGHT HOLDER", 2.55),
    ("71053", "WHITE METAL LANTERN", 3.39),
    ("84406B", "CREAM CUPID HEARTS COAT HANGER", 2.75),
    ("22423", "REGENCY CAKESTAND 3 TIER", 12.75),
]


def _write_transactions(path: Path, baskets: int = 60) -> int:
    """A small feed with real structure: repeat customers, multi-line baskets,
    and one cancelled pair so the quality rules have something to act on."""
    rows = []
    start = datetime(2011, 1, 4, 9, 0)
    for i in range(baskets):
        invoice = f"5{40000 + i}"
        customer = 17000 + (i % 12)
        when = (start + timedelta(days=i % 28, minutes=i)).strftime("%m/%d/%Y %H:%M")
        for code, desc, price in PRODUCTS[: 2 + (i % 3)]:
            rows.append([invoice, code, desc, 1 + (i % 6), when, price, customer, "United Kingdom"])

    # One cancellation, and one line with a non-positive quantity: the gate has
    # to quarantine both rather than load them.
    rows.append(["C560001", "85123A", PRODUCTS[0][1], -3, "01/20/2011 10:00", 2.55, 17001, "United Kingdom"])
    rows.append(["560002", "71053", PRODUCTS[1][1], 0, "01/20/2011 10:05", 3.39, 17002, "United Kingdom"])

    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(RETAIL_HEADER)
        writer.writerows(rows)
    return len(rows)


def _write_usage_events(path: Path, roster: dict[str, int]) -> None:
    """Telemetry for the adoption branch. Teams come from the roster, because the
    report iterates the roster and a team that never appears must still show up
    with a zero."""
    rows = []
    start = datetime(2026, 6, 1, 9, 0, tzinfo=timezone.utc)
    for team_index, (team, seats) in enumerate(roster.items()):
        for seat in range(min(seats, 6)):
            user = f"{team[:3].lower()}{seat:02d}"
            when = start + timedelta(days=team_index * 3 + seat)
            rows.append([when.strftime("%Y-%m-%d %H:%M:%S"), user, team, "open_report", ""])
            if seat % 2 == 0:
                rows.append([when.strftime("%Y-%m-%d %H:%M:%S"), user, team, "feedback", 4])

    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["event_ts", "user_id", "team", "event_type", "feedback_score"])
        writer.writerows(rows)


@pytest.fixture()
def project(tmp_path):
    """A complete project directory: the real config with its paths pointed at a
    temporary root, and a fixture feed under it."""
    import yaml

    cfg = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    # The fixture is small and deliberately contains bad rows, so the configured
    # ceiling -- tuned for the 3.57% the real extract produces -- would trip.
    cfg["quality"]["max_quarantine_rate"] = 1.0
    # Measure the adoption windows from the fixture's own events rather than the
    # date pinned for the real extract.
    cfg["adoption"].pop("analysis_as_of", None)

    (tmp_path / "data" / "raw").mkdir(parents=True)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")

    rows = _write_transactions(tmp_path / "data" / "raw" / "online_retail.csv")
    _write_usage_events(tmp_path / "data" / "raw" / "usage_events.csv", cfg["adoption"]["roster"])

    # dbt reads the published pointer through its own project; the DAG's dbt task
    # is stubbed below, so only the directory needs to exist.
    shutil.copytree(ROOT / "dbt", tmp_path / "dbt", dirs_exist_ok=True)
    return tmp_path, config_path, rows


def test_the_dag_runs_and_publishes(project, monkeypatch, airflow_metadata_db, serialize_dag):
    tmp_path, config_path, source_rows = project
    monkeypatch.setenv("RETAIL_CONFIG", str(config_path))
    monkeypatch.setenv("AIRFLOW__CORE__DAGS_FOLDER", str(ROOT / "dags"))

    from dags import retail_pipeline_dag as dag_module

    # Retries exist for a flaky network, not for a deterministic fixture: the
    # default two attempts five minutes apart turn the failing-gate test into a
    # ten-minute wait for a failure that cannot become a success.
    for task in dag_module.dag.tasks:
        task.retries = 0
        task.retry_delay = timedelta(seconds=0)
    serialize_dag(dag_module.dag)
    dag_module.dag.test()

    published = tmp_path / "published" / "CURRENT.json"
    assert published.exists(), "the run did not publish a pointer"

    import json

    pointer = json.loads(published.read_text(encoding="utf-8"))
    run_id = pointer["run_id"]

    warehouse = tmp_path / "data" / "runs" / run_id
    assert warehouse.is_dir(), f"no published run directory for {run_id}"
    assert (warehouse / "retail.db").exists(), "the published run has no warehouse"

    reports = tmp_path / "reports" / "runs" / run_id
    assert (reports / "run_metrics.json").exists()
    assert (reports / "data_quality_report.md").exists()

    metrics = json.loads((reports / "run_metrics.json").read_text(encoding="utf-8"))
    assert metrics["rows_source"] == source_rows
    assert metrics["rows_loaded"] + metrics["rows_quarantined"] == metrics["rows_source"], (
        "the run's own metrics do not account for every source row"
    )
    assert metrics["rows_quarantined"] >= 2, "the cancelled and zero-quantity rows should be held back"
    assert metrics["recommendations"] > 0, "no co-purchase pairs were produced"

    # dbt is not stubbed: the task builds the mart and runs its 47 data tests
    # against this run, and raises if any fail, so a DAG that completes is a
    # DAG whose published output satisfied the contract.
    built = list(tmp_path.rglob("retail_*.duckdb"))
    assert built, "dbt_build produced no warehouse"

    # clear_staging is all_success, so a clean run leaves nothing behind for it.
    staging = tmp_path / "data" / "staging" / run_id
    assert not staging.exists(), "this run's staging hand-off was not cleared"


def test_a_failing_gate_publishes_nothing(project, monkeypatch, airflow_metadata_db, serialize_dag):
    """The gate is the DAG's one hard stop. If it fails, `publish` must not run
    and `published/CURRENT.json` must not appear -- the guarantee the whole
    staging design exists to provide."""
    tmp_path, config_path, _ = project

    import yaml

    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    cfg["quality"]["max_quarantine_rate"] = 0.0  # any quarantined row now trips it
    config_path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")

    monkeypatch.setenv("RETAIL_CONFIG", str(config_path))
    monkeypatch.setenv("AIRFLOW__CORE__DAGS_FOLDER", str(ROOT / "dags"))

    from dags import retail_pipeline_dag as dag_module

    # Retries exist for a flaky network, not for a deterministic fixture: the
    # default two attempts five minutes apart turn the failing-gate test into a
    # ten-minute wait for a failure that cannot become a success.
    for task in dag_module.dag.tasks:
        task.retries = 0
        task.retry_delay = timedelta(seconds=0)
    serialize_dag(dag_module.dag)
    dag_module.dag.test()

    assert not (tmp_path / "published" / "CURRENT.json").exists(), (
        "a failed quality gate still published a pointer"
    )
