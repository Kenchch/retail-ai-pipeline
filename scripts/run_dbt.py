"""Build the dbt consumer against one atomically published pipeline run."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import sysconfig
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from retail_pipeline.pipeline import load_config
from retail_pipeline.publish import (
    WAREHOUSE_FILE,
    _atomic_write_text,
    contained,
    published_manifest,
)


def _sql_literal_content(value: str) -> str:
    """Escape text that Jinja will place inside a DuckDB single-quoted literal."""
    return value.replace("'", "''")


def _dbt_executable() -> Path:
    """Prefer dbt from this Python environment, then an explicit PATH install."""
    same_environment = Path(sysconfig.get_path("scripts")) / (
        "dbt.exe" if os.name == "nt" else "dbt"
    )
    if same_environment.is_file():
        return same_environment
    found = shutil.which("dbt")
    if found:
        return Path(found)
    raise FileNotFoundError(
        "dbt is not installed for this worker - install requirements-dbt.txt"
    )


def _validate_data_stamp(data_dir: Path, run_id: str) -> None:
    """Bind the manifest to the publication stamp inside its SQLite version."""
    warehouse = data_dir / WAREHOUSE_FILE
    if not warehouse.is_file():
        raise SystemExit(f"published warehouse is missing: {warehouse}")
    try:
        with sqlite3.connect(warehouse) as conn:
            row = conn.execute(
                "SELECT run_id FROM _publication WHERE id = 1"
            ).fetchone()
    except sqlite3.Error as exc:
        raise SystemExit(f"cannot verify published warehouse stamp: {exc}") from exc
    stamped = row[0] if row else None
    if stamped != run_id:
        raise SystemExit(
            f"published data is stamped {stamped!r}, not manifest run {run_id!r}"
        )


def _identity(manifest: dict) -> tuple:
    return manifest.get("run_id"), manifest.get("data"), manifest.get("reports")


def _prune_mart_versions(root: Path, current: Path, keep: int = 3) -> None:
    """Keep recent generated marts; an open Windows reader may retain an older one."""
    versions = sorted(
        root.glob("retail_*.duckdb"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    protected = {current, *versions[:keep]}
    for version in versions:
        if version in protected:
            continue
        try:
            version.unlink()
            Path(str(version) + ".wal").unlink(missing_ok=True)
        except PermissionError:
            # A Windows BI reader may still own an older version. It is safe to
            # retain because CURRENT.json no longer sends new readers to it.
            pass


def main(expected_run_id: str | None = None) -> int:
    """Resolve the manifest once, then make both bound versions visible to dbt."""
    cfg = load_config()
    manifest = published_manifest(cfg)
    if manifest is None or manifest.get("reports") is None:
        raise SystemExit(
            "nothing fully published - run `python -m retail_pipeline.pipeline` first"
        )
    data_dir = contained(cfg["paths"]["data_runs"].parent, manifest["data"])
    reports_dir = contained(cfg["paths"]["reports"], manifest["reports"])
    if not data_dir.is_dir() or not reports_dir.is_dir():
        raise SystemExit("published manifest names a version directory that is missing")
    published_run_id = manifest.get("run_id")
    if not isinstance(published_run_id, str) or not published_run_id:
        raise SystemExit("published manifest has no run_id")
    if expected_run_id is not None and published_run_id != expected_run_id:
        raise SystemExit(
            f"dbt task belongs to {expected_run_id!r}, but the published manifest "
            f"now names {published_run_id!r}; refusing to validate a different run"
        )
    _validate_data_stamp(data_dir, published_run_id)

    env = os.environ.copy()
    env["RETAIL_PUBLISHED_DATA_DIR_SQL"] = _sql_literal_content(data_dir.as_posix())
    env["RETAIL_PUBLISHED_REPORTS_DIR_SQL"] = _sql_literal_content(
        reports_dir.as_posix()
    )
    env["RETAIL_PUBLISHED_RUN_ID_SQL"] = _sql_literal_content(published_run_id)
    env["RETAIL_RAW_INPUT_NAME_SQL"] = _sql_literal_content(
        Path(cfg["paths"]["raw"]).name
    )

    dbt_dir = ROOT / "dbt"
    mart_root = dbt_dir / "runs"
    mart_root.mkdir(parents=True, exist_ok=True)
    # DuckDB derives the catalog name from the filename. Keep the stem a valid
    # unquoted identifier; leading dots make dbt-generated catalog references
    # fail before any model can run.
    fd, candidate_name = tempfile.mkstemp(
        prefix="retail_candidate_", suffix=".duckdb", dir=mart_root
    )
    os.close(fd)
    candidate = Path(candidate_name)
    candidate.unlink()  # DuckDB creates a fresh database at this unique path.
    env["RETAIL_DBT_PATH"] = candidate.as_posix()

    try:
        subprocess.run(
            [
                str(_dbt_executable()),
                "build",
                "--project-dir",
                str(dbt_dir),
                "--profiles-dir",
                str(dbt_dir),
            ],
            cwd=ROOT,
            env=env,
            check=True,
        )
        # A CLI publisher can advance CURRENT while dbt is building. Do not let
        # an older candidate become the newest mart after that point.
        current_manifest = published_manifest(cfg)
        if current_manifest is None or _identity(current_manifest) != _identity(
            manifest
        ):
            raise SystemExit(
                "published manifest changed during dbt build; refusing to promote"
            )

        # A unique version plus one pointer swap avoids both stale-WAL replay
        # and Windows readers blocking replacement of a fixed database file.
        version = candidate.with_name(candidate.name.replace("_candidate", "", 1))
        os.replace(candidate, version)
        try:
            _atomic_write_text(
                dbt_dir / "CURRENT.json",
                json.dumps(
                    {
                        "run_id": published_run_id,
                        "database": version.relative_to(dbt_dir).as_posix(),
                    },
                    indent=2,
                ),
            )
        except BaseException:
            version.unlink(missing_ok=True)
            raise
        _prune_mart_versions(mart_root, version)
    finally:
        candidate.unlink(missing_ok=True)
        Path(str(candidate) + ".wal").unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
