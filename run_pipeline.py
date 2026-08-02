"""Entry point: `python run_pipeline.py [--config config.yaml]`."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from retail_pipeline.pipeline import run  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Retail ETL + recommendation pipeline")
    parser.add_argument("--config", default=None, help="path to config.yaml")
    args = parser.parse_args()
    run(args.config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
