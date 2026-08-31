"""Tests for the dbt wrapper's version binding and atomic mart promotion."""

import json
import sqlite3
import subprocess
from pathlib import Path

import pytest

duckdb = pytest.importorskip(
    "duckdb", reason="duckdb is only installed for the dbt test job"
)

from scripts import run_dbt


def _published(tmp_path, monkeypatch):
    data = tmp_path / "data" / "runs" / "data_v1"
    reports = tmp_path / "reports" / "runs" / "reports_v1"
    data.mkdir(parents=True)
    reports.mkdir(parents=True)
    (tmp_path / "dbt").mkdir()
    cfg = {
        "paths": {
            "data_runs": tmp_path / "data" / "runs",
            "reports": tmp_path / "reports",
            "raw": tmp_path / "raw" / "replacement.csv",
        }
    }
    manifest = {
        "run_id": "scheduled__2026-08-29",
        "data": "runs/data_v1",
        "reports": "runs/reports_v1",
    }
    with sqlite3.connect(data / "retail.db") as connection:
        connection.execute(
            "CREATE TABLE _publication (id INTEGER PRIMARY KEY, run_id TEXT)"
        )
        connection.execute(
            "INSERT INTO _publication VALUES (1, ?)", (manifest["run_id"],)
        )
    monkeypatch.setattr(run_dbt, "ROOT", tmp_path)
    monkeypatch.setattr(run_dbt, "load_config", lambda: cfg)
    monkeypatch.setattr(run_dbt, "published_manifest", lambda _: dict(manifest))
    monkeypatch.setattr(run_dbt, "_dbt_executable", lambda: Path("dbt"))
    return manifest


def test_sql_literal_content_escapes_legal_apostrophes():
    assert run_dbt._sql_literal_content("C:/Users/O'Brien/data") == (
        "C:/Users/O''Brien/data"
    )


def test_successful_build_atomically_replaces_the_validated_database(
    tmp_path, monkeypatch
):
    manifest = _published(tmp_path, monkeypatch)
    old = tmp_path / "dbt" / "runs" / "retail_old.duckdb"
    old.parent.mkdir()
    old.write_bytes(b"old validated database")
    current = tmp_path / "dbt" / "CURRENT.json"
    current.write_text(
        json.dumps({"run_id": "older", "database": "runs/retail_old.duckdb"})
    )

    def build(_, *, cwd, env, check):
        assert cwd == tmp_path
        assert check is True
        assert env["RETAIL_PUBLISHED_RUN_ID_SQL"] == manifest["run_id"]
        assert env["RETAIL_RAW_INPUT_NAME_SQL"] == "replacement.csv"
        Path(env["RETAIL_DBT_PATH"]).write_bytes(b"new validated database")

    monkeypatch.setattr(run_dbt.subprocess, "run", build)
    assert run_dbt.main(expected_run_id=manifest["run_id"]) == 0
    pointer = json.loads(current.read_text())
    assert pointer["run_id"] == manifest["run_id"]
    assert (tmp_path / "dbt" / pointer["database"]).read_bytes() == (
        b"new validated database"
    )
    assert old.read_bytes() == b"old validated database"
    assert list((tmp_path / "dbt").rglob("retail_candidate_*")) == []


def test_failed_build_keeps_the_previous_validated_database(tmp_path, monkeypatch):
    manifest = _published(tmp_path, monkeypatch)
    old = tmp_path / "dbt" / "runs" / "retail_old.duckdb"
    old.parent.mkdir()
    old.write_bytes(b"old validated database")
    current = tmp_path / "dbt" / "CURRENT.json"
    old_pointer = {"run_id": "older", "database": "runs/retail_old.duckdb"}
    current.write_text(json.dumps(old_pointer))

    def fail(_, *, cwd, env, check):
        Path(env["RETAIL_DBT_PATH"]).write_bytes(b"untested database")
        raise subprocess.CalledProcessError(1, "dbt build")

    monkeypatch.setattr(run_dbt.subprocess, "run", fail)
    with pytest.raises(subprocess.CalledProcessError):
        run_dbt.main(expected_run_id=manifest["run_id"])
    assert json.loads(current.read_text()) == old_pointer
    assert old.read_bytes() == b"old validated database"
    assert list((tmp_path / "dbt").rglob("retail_candidate_*")) == []


def test_historical_task_refuses_to_validate_the_current_run(tmp_path, monkeypatch):
    _published(tmp_path, monkeypatch)
    with pytest.raises(SystemExit, match="refusing to validate a different run"):
        run_dbt.main(expected_run_id="scheduled__2026-08-28")


def test_manifest_cannot_bind_reports_to_another_data_run(tmp_path, monkeypatch):
    manifest = _published(tmp_path, monkeypatch)
    data = tmp_path / "data" / manifest["data"]
    with sqlite3.connect(data / "retail.db") as connection:
        connection.execute(
            "UPDATE _publication SET run_id = 'scheduled__2026-08-28' WHERE id = 1"
        )

    with pytest.raises(SystemExit, match="published data is stamped"):
        run_dbt.main(expected_run_id=manifest["run_id"])


def test_manifest_change_during_build_prevents_promotion(tmp_path, monkeypatch):
    manifest = _published(tmp_path, monkeypatch)
    current = tmp_path / "dbt" / "CURRENT.json"
    old_pointer = {"run_id": "older", "database": "runs/retail_old.duckdb"}
    current.write_text(json.dumps(old_pointer))

    def build(_, *, cwd, env, check):
        Path(env["RETAIL_DBT_PATH"]).write_bytes(b"valid but stale database")
        manifest["run_id"] = "scheduled__2026-08-30"

    monkeypatch.setattr(run_dbt.subprocess, "run", build)
    with pytest.raises(SystemExit, match="changed during dbt build"):
        run_dbt.main(expected_run_id="scheduled__2026-08-29")
    assert json.loads(current.read_text()) == old_pointer
    assert list((tmp_path / "dbt").rglob("retail_candidate_*")) == []


def test_open_reader_keeps_its_version_while_pointer_advances(tmp_path, monkeypatch):
    manifest = _published(tmp_path, monkeypatch)
    root = tmp_path / "dbt"
    versions = root / "runs"
    versions.mkdir()
    old = versions / "retail_old.duckdb"
    with duckdb.connect(str(old)) as connection:
        connection.execute("create table old_mart as select 1 as value")
    current = root / "CURRENT.json"
    current.write_text(
        json.dumps({"run_id": "older", "database": "runs/retail_old.duckdb"})
    )

    def build(_, *, cwd, env, check):
        with duckdb.connect(env["RETAIL_DBT_PATH"]) as connection:
            connection.execute("create table new_mart as select 2 as value")

    monkeypatch.setattr(run_dbt.subprocess, "run", build)
    reader = duckdb.connect(str(old), read_only=True)
    try:
        assert run_dbt.main(expected_run_id=manifest["run_id"]) == 0
        pointer = json.loads(current.read_text())
        assert pointer["database"] != "runs/retail_old.duckdb"
        assert reader.execute("select value from old_mart").fetchone() == (1,)
        with duckdb.connect(str(root / pointer["database"]), read_only=True) as new:
            assert new.execute("select value from new_mart").fetchone() == (2,)
    finally:
        reader.close()
