"""Build the dbt consumer against one atomically published pipeline run."""

from __future__ import annotations

import os
import subprocess
import sys
import sysconfig
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from retail_pipeline.pipeline import load_config
from retail_pipeline.publish import contained, published_manifest


def main() -> int:
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

    env = os.environ.copy()
    env["RETAIL_PUBLISHED_DATA_DIR"] = data_dir.as_posix()
    env["RETAIL_PUBLISHED_REPORTS_DIR"] = reports_dir.as_posix()
    executable = Path(sysconfig.get_path("scripts")) / (
        "dbt.exe" if os.name == "nt" else "dbt"
    )
    subprocess.run(
        [
            str(executable),
            "build",
            "--project-dir",
            str(ROOT / "dbt"),
            "--profiles-dir",
            str(ROOT / "dbt"),
        ],
        cwd=ROOT,
        env=env,
        check=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
