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

from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("airflow")

ROOT = Path(__file__).resolve().parents[1]


# The `project` fixture -- the real config pointed at a temporary root, a small
# feed and a copy of the dbt project -- is in conftest.py, because
# tests/test_dbt_wrapper.py runs dbt against it too, without Airflow.


def _repository_dbt_state() -> dict[str, str]:
    """Fingerprint the checkout's dbt directory, target/ and logs/ included.

    Those are gitignored, so nothing here would ever show up in a diff -- which
    is the reason to assert on it rather than trust CI to notice. What a test
    run used to leave behind was a local mart pointed at a 60-basket fixture,
    silently replacing the one a person had built from the real extract.
    """
    import hashlib

    state = {}
    for path in sorted((ROOT / "dbt").rglob("*")):
        if path.is_file():
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            state[str(path.relative_to(ROOT))] = digest
    return state


def test_the_dag_runs_and_publishes(
    project, monkeypatch, airflow_metadata_db, serialize_dag, task_states
):
    tmp_path, config_path, source_rows = project
    monkeypatch.setenv("RETAIL_CONFIG", str(config_path))
    monkeypatch.setenv("AIRFLOW__CORE__DAGS_FOLDER", str(ROOT / "dags"))
    repository_dbt_before = _repository_dbt_state()

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
    assert (
        metrics["rows_loaded"] + metrics["rows_quarantined"] == metrics["rows_source"]
    ), "the run's own metrics do not account for every source row"
    assert metrics["rows_quarantined"] >= 2, (
        "the cancelled and zero-quantity rows should be held back"
    )
    assert metrics["recommendations"] > 0, "no co-purchase pairs were produced"

    # Every task has to have succeeded, dbt_build included. It is not stubbed:
    # it builds the mart and runs its own data tests against the run this DAG
    # just published, and raises if any fail. `watcher` is the exception -- it
    # is trigger_rule="one_failed", so a clean run skips it, and a run where it
    # succeeded would mean something upstream broke.
    states = task_states("retail_ai_pipeline")
    assert states.get("watcher") in (None, "skipped", "upstream_failed"), (
        f"the watcher fired, so a task failed: {states}"
    )
    failed = {t: s for t, s in states.items() if t != "watcher" and s != "success"}
    assert not failed, f"tasks did not succeed: {failed}"

    # clear_staging is all_success, so a clean run leaves nothing behind for it.
    staging = tmp_path / "data" / "staging" / run_id
    assert not staging.exists(), "this run's staging hand-off was not cleared"

    # And the run stayed inside the fixture. dbt_build resolves its project
    # from `paths.dbt`; when it resolved from the wrapper's own ROOT instead, a
    # single fixture run wrote 84 files into the checkout and repointed the
    # local mart at fixture data.
    after = _repository_dbt_state()
    added = sorted(set(after) - set(repository_dbt_before))
    changed = sorted(
        k
        for k in set(after) & set(repository_dbt_before)
        if after[k] != repository_dbt_before[k]
    )
    assert not added and not changed, (
        f"the run wrote into the repository's dbt project: added={added}, "
        f"changed={changed}"
    )


def test_a_failing_gate_publishes_nothing(
    project, monkeypatch, airflow_metadata_db, serialize_dag, task_states
):
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

    states = task_states("retail_ai_pipeline")
    assert states.get("data_quality_gate") == "failed", (
        f"the gate was supposed to fail: {states}"
    )
    assert states.get("publish") not in ("success",), (
        f"publish ran after a failed gate: {states}"
    )

    assert not (tmp_path / "published" / "CURRENT.json").exists(), (
        "a failed quality gate still published a pointer"
    )

    # The watcher is trigger_rule="one_failed" and fails on purpose: it exists
    # so that a run with a failed task cannot end up green at the DAG level.
    # The happy-path test asserts it stays skipped; nothing asserted that it
    # actually fires, so a watcher wired to the wrong rule -- or dropped from
    # the DAG -- would have been caught by neither.
    assert states.get("watcher") == "failed", (
        f"the gate failed and the watcher did not fire, so the run would not "
        f"have gone red: {states}"
    )
