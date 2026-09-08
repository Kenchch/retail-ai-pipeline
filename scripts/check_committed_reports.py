"""Are the committed reports what this source data produces?

Every figure in README.md and docs/DESIGN.md is asserted against a file under
`reports/`. That chain is only worth anything if the reports themselves are
what the pipeline produces from the pinned extract -- otherwise the tests prove
the documents agree with a snapshot nobody can regenerate.

Run after regenerating them. Two fields are expected to differ and are
normalised before comparison:

* `run_id` -- a timestamp, different by construction on every run;
* `compute_seconds` -- wall time, a property of the machine. The same code has
  recorded 9.3, 14.1, 10.6 and 32.0 seconds here.

Everything else must match to the byte. Nothing else is excused: a rounding
difference, a reordered key or a moved figure is the thing this exists to
catch.
"""

from __future__ import annotations

import difflib
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

VOLATILE = (
    # `"run_id": "local_2026..."` in JSON and `run_id: local_2026...` in prose.
    re.compile(r'(run_id"?\s*[:=]\s*"?)[\w.:+-]+'),
    re.compile(r'("compute_seconds"\s*:\s*)[\d.]+'),
)


def _normalise(text: str) -> str:
    for pattern in VOLATILE:
        text = pattern.sub(r"\1<varies>", text)
    return text


def _committed(path: Path) -> str | None:
    relative = path.relative_to(ROOT).as_posix()
    result = subprocess.run(  # noqa: S603
        ["git", "show", f"HEAD:{relative}"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    return result.stdout if result.returncode == 0 else None


def main() -> int:
    tracked = subprocess.run(  # noqa: S603
        ["git", "ls-files", "reports"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()

    checked, drifted = 0, []
    for relative in tracked:
        path = ROOT / relative
        if path.suffix not in {".json", ".md"}:
            continue
        committed = _committed(path)
        if committed is None or not path.exists():
            drifted.append((relative, ["file is missing on one side"]))
            continue
        checked += 1
        before = _normalise(committed).splitlines()
        after = _normalise(path.read_text(encoding="utf-8")).splitlines()
        if before != after:
            drifted.append(
                (
                    relative,
                    list(
                        difflib.unified_diff(
                            before, after, "committed", "regenerated", lineterm="", n=1
                        )
                    ),
                )
            )

    # `.gitignore` ignores reports/* and un-ignores the committed files by
    # name, so a report from a generator added later is invisible rather than
    # untracked: it never appears in `git status`, never gets committed, and
    # the documents can quote a figure nothing checks.
    runtime = {"CURRENT", ".gitkeep"}
    tracked_names = {Path(r).name for r in tracked}
    unlisted = sorted(
        path.name
        for path in (ROOT / "reports").iterdir()
        if path.is_file()
        and path.name not in tracked_names
        and path.name not in runtime
    )
    if unlisted:
        print(
            f"reports/ holds {len(unlisted)} file(s) that are neither committed nor "
            f"runtime output: {', '.join(unlisted)}\n\n"
            "reports/* is gitignored with the committed files un-ignored by name, "
            "so these will never show up in git status. Either add them to "
            ".gitignore's allow-list and commit them, or write them somewhere "
            "under reports/runs/."
        )
        return 1

    if drifted:
        print(
            f"{len(drifted)} of {checked} committed reports do not match a fresh run:\n"
        )
        for relative, diff in drifted:
            print(f"--- {relative}")
            for line in diff[:40]:
                print(f"    {line}")
            print()
        print(
            "Either the pipeline changed and the reports were not regenerated, or "
            "the reports were edited by hand. Both are the same problem: the "
            "figures in README.md and docs/DESIGN.md are asserted against these "
            "files, so a report nobody can reproduce makes those assertions "
            "circular."
        )
        return 1

    print(
        f"{checked} committed reports match a fresh run (run_id and compute_seconds excepted)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
