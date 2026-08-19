"""Structural tests for the Airflow DAG.

The DAG had never been tested by anything. It is the code path production
actually uses - `run()` is the local convenience - and every bug it has carried
has been a *wiring* bug: a split publish that left the warehouse holding two
runs, `all_done` on the staging cleanup that deleted the files an operator
needed to re-run the gate, `one_failed` on the report archiver that fired
before a parallel branch had finished writing into the directory it was
archiving, and leaves that reported a failed run as a success.

None of those are visible in a unit test of a stage function. They are visible
in the trigger rules and the edges, which is what this file asserts.

Airflow is not in requirements.txt - `dags/` is only read by a scheduler, and
nothing else in this repo imports it - so these skip when it is absent. CI
installs it so they actually run.
"""

import pytest

pytest.importorskip("airflow", reason="Airflow is only needed to read the DAG")

from airflow.exceptions import AirflowException

from dags.retail_pipeline_dag import dag, task_watcher


def test_the_dag_loads_with_no_import_errors():
    """A DAG that does not import is a DAG the scheduler silently never runs."""
    assert dag.dag_id == "retail_ai_pipeline"
    assert len(dag.tasks) == 11


def test_every_task_feeds_the_watcher():
    """The watcher is only as good as its upstream set.

    A task missing from it is a failure the run reports as success, which is
    the whole point of having it - so the edges are built from dag.tasks rather
    than a hand-written list, and this asserts that held.
    """
    watcher = dag.get_task("watcher")
    assert watcher.upstream_task_ids == {t.task_id for t in dag.tasks} - {"watcher"}


def test_the_watcher_is_the_only_task_that_can_fail_a_clean_run():
    """`one_failed` means SKIPPED on a clean run and FAILED on a dirty one."""
    assert dag.get_task("watcher").trigger_rule == "one_failed"


def test_the_watcher_raises():
    """It does no work. Raising is the job."""

    class _TI:
        def __init__(self, task_id, state):
            self.task_id, self.state = task_id, state

    class _Run:
        def get_task_instances(self):
            return [_TI("data_quality_gate", "failed"), _TI("extract", "success")]

    with pytest.raises(AirflowException, match="data_quality_gate"):
        task_watcher(dag_run=_Run())


def test_the_run_cannot_be_green_while_a_task_is_red():
    """Airflow decides a run's state from its LEAF tasks.

    Both real leaves run on all_done - the report finaliser has to, or a failed
    run's diagnostics are never archived, and the staging pruner has to, or old
    directories accumulate forever. Both then SUCCEED on a failed run, so every
    leaf was green and a run with a failed quality gate inside it was marked
    SUCCESS. The watcher is the leaf that is not green.
    """
    leaves = {t.task_id for t in dag.tasks if not t.downstream_task_ids}
    assert leaves == {"watcher"}, (
        f"{leaves - {'watcher'}} are leaves that succeed on a failed run, so the "
        f"run state would not reflect the failure"
    )


def test_publish_is_the_only_task_that_writes_to_the_warehouse():
    """Splitting the publish is what let the warehouse hold two runs at once."""
    assert dag.get_task("publish").upstream_task_ids == {"write_run_metrics"}
    assert dag.get_task("write_run_metrics").upstream_task_ids == {
        "build_recommendations",
        "measure_adoption",
    }


def test_the_metrics_are_written_before_the_publish():
    """Every file in a report version is built from the same staged tables, so
    the version is complete before anything can point at it. Reading published
    Parquet after the publish assembled it from a different batch."""
    assert "publish" in dag.get_task("write_run_metrics").downstream_task_ids


def test_the_finaliser_waits_for_every_writer_and_the_publish():
    """`one_failed` fired as soon as ANY upstream failed, without waiting for
    the others: a gate failure archived the version while measure_adoption - a
    root task, running in parallel - was still writing into it."""
    finalize = dag.get_task("finalize_reports")
    assert finalize.trigger_rule == "all_done"
    assert finalize.upstream_task_ids == {
        "data_quality_gate",
        "measure_adoption",
        "write_run_metrics",
        "publish",
    }


def test_staging_is_cleared_only_on_success_and_pruned_by_age():
    """all_done on clear_staging deleted the raw.parquet an operator needed to
    clear and re-run the gate against, so recovery meant re-reading the whole
    feed - precisely what staging exists to avoid."""
    assert dag.get_task("clear_staging").trigger_rule == "all_success"
    assert dag.get_task("prune_staging").trigger_rule == "all_done"


def test_adoption_is_a_root_task():
    """It reads telemetry and nothing the extract produces, so a slow or broken
    extract must not stop it being calculated. It joins at the publish."""
    assert dag.get_task("measure_adoption").upstream_task_ids == set()
