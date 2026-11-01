"""Check that the pinned install files still satisfy pyproject.toml.

CI installs requirements.lock and then the package with --no-deps, and the dbt
job installs requirements-dbt.txt, so a pin that changes only in pyproject.toml
is never installed. Dependabot moved ruff to 0.17.2 there (#27) while every CI
run went on with the 0.16.6 in the lock. This fails when a requirement in
pyproject.toml is not met by the version its install file pins.
"""

from __future__ import annotations

import re
import sys
import tomllib
from collections import defaultdict
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

ROOT = Path(__file__).resolve().parents[1]
PIN = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s;]+)")
REFRESH = {
    "requirements.lock": (
        "uv pip compile pyproject.toml --universal --python-version 3.11 "
        "--extra dev -o requirements.lock"
    ),
    "requirements-dbt.txt": "set the same pins as the dbt extra",
}


def pinned(path: Path) -> dict[str, set[str]]:
    """Every version a file pins, per package; a universal lock may pin several."""
    pins: dict[str, set[str]] = defaultdict(set)
    for line in path.read_text(encoding="utf-8").splitlines():
        match = PIN.match(line)
        if match:
            pins[canonicalize_name(match[1])].add(match[2])
    return pins


def unmet(requirements: list[str], pins: dict[str, set[str]]) -> list[str]:
    """The requirements that some pinned version, or no pin at all, fails."""
    problems = []
    for text in requirements:
        req = Requirement(text)
        versions = sorted(pins.get(canonicalize_name(req.name), set()))
        if not versions or any(
            not req.specifier.contains(v, prereleases=True) for v in versions
        ):
            problems.append(f"{req} (pinned: {', '.join(versions) or 'nothing'})")
    return problems


def main() -> int:
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    project = tomllib.loads(pyproject)["project"]
    extras = project["optional-dependencies"]
    checks = {
        "requirements.lock": project["dependencies"] + extras["dev"],
        "requirements-dbt.txt": extras["dbt"],
    }
    failed = False
    for name, requirements in checks.items():
        problems = unmet(requirements, pinned(ROOT / name))
        if problems:
            failed = True
            print(f"{name} does not satisfy pyproject.toml:", file=sys.stderr)
            for problem in problems:
                print(f"  {problem}", file=sys.stderr)
            print(f"  fix: {REFRESH[name]}", file=sys.stderr)
        else:
            print(f"{name}: all {len(requirements)} requirements of pyproject.toml met")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
