"""Test-session setup.

AIRFLOW_HOME has to be set before anything imports airflow, because Airflow
reads its configuration -- including where the metadata database lives -- at
import time. A fixture cannot do it: by the time one runs, the module under
test has already imported airflow and the connection string is fixed.

Pointing it at a temporary directory keeps the suite from writing an
airflow.cfg and a sqlite database into the working tree, and keeps one test
run from inheriting another's task history.

`project` is here rather than in test_dag_run.py because the dbt wrapper
tests run dbt against the same fixture, and test_dag_run.py is skipped
wherever Airflow is not installed.
"""

from __future__ import annotations

import csv
import os
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

_AIRFLOW_HOME = Path(tempfile.gettempdir()) / "retail-ai-pipeline-airflow-home"
os.environ.setdefault("AIRFLOW_HOME", str(_AIRFLOW_HOME))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault(
    "AIRFLOW__CORE__DAGS_FOLDER", str(Path(__file__).resolve().parents[1] / "dags")
)


@pytest.fixture(scope="session")
def airflow_metadata_db():
    """Create the metadata tables once for the session.

    `dag.test()` executes tasks against the real metadata database rather than
    faking it, so without this every run fails on `no such table: task_instance`.
    """
    pytest.importorskip("airflow")
    from airflow.utils import db

    db.resetdb(skip_init=False)
    return _AIRFLOW_HOME


@pytest.fixture()
def serialize_dag():
    """Write a DAG into the metadata database so `dag.test()` can create a run.

    Airflow 3 creates DagRuns from the *serialized* DAG, not from the object in
    memory, so a DAG that only exists in an imported module fails with "the dag
    is not serialized". The scheduler normally does this; a test has to do it
    itself.
    """
    pytest.importorskip("airflow")
    from airflow.models.dag import DagModel
    from airflow.models.dagbundle import DagBundleModel
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.serialization.serialized_objects import (
        DagSerialization,
        LazyDeserializedDAG,
    )
    from airflow.utils.session import create_session

    def _serialize(dag, bundle_name: str = "dags-folder"):
        # Two transactions: dag.bundle_name is a foreign key, so the bundle row
        # has to be committed before the dag row referencing it is inserted.
        with create_session() as session:
            if session.get(DagBundleModel, bundle_name) is None:
                session.add(DagBundleModel(name=bundle_name))
                session.commit()
        with create_session() as session:
            if session.get(DagModel, dag.dag_id) is None:
                session.add(
                    DagModel(dag_id=dag.dag_id, bundle_name=bundle_name, is_stale=False)
                )
                session.commit()
        SerializedDagModel.write_dag(
            LazyDeserializedDAG(data=DagSerialization.to_dict(dag)),
            bundle_name=bundle_name,
        )

    return _serialize


@pytest.fixture()
def task_states():
    """Final state of every task in a dag_id, read back from the metadata database.

    `dag.test()` records task outcomes but does not raise when one fails, so a
    test that only looks at files on disk can pass while half the DAG errored.
    Asserting on the states is what makes a green test mean the run was green.
    """
    pytest.importorskip("airflow")
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from airflow.utils.session import create_session

    def _states(dag_id: str) -> dict[str, str | None]:
        # Only the newest run: the session-scoped database is shared, so every
        # test that runs this DAG leaves its task instances behind and an
        # unfiltered query would mix a later run with an earlier one.
        with create_session() as session:
            latest = (
                session.query(DagRun)
                .filter(DagRun.dag_id == dag_id)
                .order_by(DagRun.start_date.desc())
                .first()
            )
            if latest is None:
                return {}
            rows = (
                session.query(TaskInstance)
                .filter(
                    TaskInstance.dag_id == dag_id,
                    TaskInstance.run_id == latest.run_id,
                )
                .all()
            )
            return {
                # state comes back as a plain string, not the TaskInstanceState enum
                row.task_id: (str(row.state) if row.state else None)
                for row in rows
            }

    return _states


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
            rows.append(
                [
                    invoice,
                    code,
                    desc,
                    1 + (i % 6),
                    when,
                    price,
                    customer,
                    "United Kingdom",
                ]
            )

    # One cancellation, and one line with a non-positive quantity: the gate has
    # to quarantine both rather than load them.
    rows.append(
        [
            "C560001",
            "85123A",
            PRODUCTS[0][1],
            -3,
            "01/20/2011 10:00",
            2.55,
            17001,
            "United Kingdom",
        ]
    )
    rows.append(
        [
            "560002",
            "71053",
            PRODUCTS[1][1],
            0,
            "01/20/2011 10:05",
            3.39,
            17002,
            "United Kingdom",
        ]
    )

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
            rows.append(
                [when.strftime("%Y-%m-%d %H:%M:%S"), user, team, "open_report", ""]
            )
            if seat % 2 == 0:
                rows.append(
                    [when.strftime("%Y-%m-%d %H:%M:%S"), user, team, "feedback", 4]
                )

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
    _write_usage_events(
        tmp_path / "data" / "raw" / "usage_events.csv", cfg["adoption"]["roster"]
    )

    # dbt is not stubbed: it builds the mart and runs its data tests against
    # what the run publishes. It needs its own project, and `paths.dbt` in the
    # copied config resolves to this one, so the build writes its mart database,
    # its CURRENT.json and dbt's target/ and logs/ here rather than into the
    # checkout the tests are running from.
    shutil.copytree(ROOT / "dbt", tmp_path / "dbt", dirs_exist_ok=True)
    return tmp_path, config_path, rows
