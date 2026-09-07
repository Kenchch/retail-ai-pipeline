"""Test-session setup.

AIRFLOW_HOME has to be set before anything imports airflow, because Airflow
reads its configuration -- including where the metadata database lives -- at
import time. A fixture cannot do it: by the time one runs, the module under
test has already imported airflow and the connection string is fixed.

Pointing it at a temporary directory keeps the suite from writing an
airflow.cfg and a sqlite database into the working tree, and keeps one test
run from inheriting another's task history.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

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
    from airflow.serialization.serialized_objects import DagSerialization, LazyDeserializedDAG
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
            LazyDeserializedDAG(data=DagSerialization.to_dict(dag)), bundle_name=bundle_name
        )

    return _serialize
