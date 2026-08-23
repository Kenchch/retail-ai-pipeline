"""Versioned publishing: data versions, report versions, and the one manifest
that binds them.

Split out of pipeline.py so the ETL (extract/quality/transform/load) and the
publish/version machinery can be read apart. The dependency is one-way -
pipeline.py imports from here; nothing here calls back into the ETL - so this
module stands alone and load()/run() drive it.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

log = logging.getLogger("pipeline")

__all__ = [
    "MANIFEST_FILE",
    "PUBLISHED_MARKER",
    "REPORT_NAMES",
    "WAREHOUSE_FILE",
    "_SAFE_RUN_ID",
    "_copy_database",
    "_current_file",
    "_data_current_file",
    "_manifest_file",
    "_reports_complete",
    "_restore_archived_reports",
    "_snapshot_for_readers",
    "_stamped_run_id",
    "_version_root",
    "contained",
    "current_run_id",
    "data_current_run_id",
    "data_version_dir",
    "finalize_reports",
    "keep_failed_reports",
    "mark_published",
    "new_data_version",
    "prune_data_versions",
    "prune_report_versions",
    "publish_data_version",
    "publish_run",
    "publish_version",
    "published_data_dir",
    "published_manifest",
    "published_reports",
    "reports_dir",
    "run_dir",
    "safe_run_id",
    "staged_data_run_id",
    "verify_data_version",
    "warehouse_path",
    "warehouse_run_id",
]

# The three files that make up one report version. Named once so the promote,
# the reader snapshot and the tests cannot disagree about what "complete" means.
REPORT_NAMES = (
    "data_quality_report.md",
    "adoption_report.md",
    "run_metrics.json",
)

# Written into a version directory by the step that publishes the data. Its
# presence is the only thing the finaliser consults, which is what lets the
# finaliser be re-run safely and lets it work without asking Airflow about
# another task's state.
PUBLISHED_MARKER = ".published"


WAREHOUSE_FILE = "retail.db"


def data_version_dir(cfg: dict, run_id: str) -> Path:
    """This run's data version: data/runs/<run_id>/, holding every Parquet file
    and the SQLite warehouse.

    The publish used to be a SQLite commit followed by N separate os.replace
    calls on the Parquet files, and N separate renames can half-succeed.
    Injecting an OSError into the second one produced exactly what that
    implies: SQLite on tonight's run, fact_sales.parquet on tonight's,
    dim_product.parquet and quarantine.parquet still on last night's - and
    because the report finaliser took the SQLite stamp as authority, the
    reports advanced too and the whole thing reported as a successful run.
    Anything reading the directory got tonight's facts joined against last
    night's dimensions.

    A version is a directory, built whole and verified, and publishing it is
    one write to data/CURRENT. See publish_data_version().
    """
    d = run_dir(cfg["paths"]["data_runs"], run_id)
    d.mkdir(parents=True, exist_ok=True)
    return d


def new_data_version(cfg: dict, run_id: str) -> Path:
    """A directory for THIS attempt, guaranteed not to be a published one.

    data_version_dir() derives the path from run_id alone and mkdirs it, which
    is safe only while run_id names a version that has never been published.
    In Airflow that is false on the second attempt: `context["run_id"]` is
    constant across every retry of a DAG run and across an operator clearing
    the task, which the DAG's own docstring recommends as the remedy for a
    failed publish. So a retry of `publish` re-entered load() pointing at the
    LIVE directory, rewrote its Parquet in place, ran the swap against the
    published database - and on failure rmtree'd the whole thing while
    data/CURRENT still named it. Reproduced: data/CURRENT naming a directory
    that no longer exists, published_data_dir() and warehouse_run_id() both
    None, and the previous good version not restored because the pointer was
    never moved back.

    A published version is immutable. A retry produces a NEW version - which is
    what versioning is for - so this takes the first free name and never reuses
    one. exist_ok=False rather than a pre-check, so two processes racing get
    different directories instead of the same one.
    """
    root = cfg["paths"]["data_runs"]
    root.mkdir(parents=True, exist_ok=True)
    attempt, suffix = 1, ""
    while True:
        candidate = run_dir(root, run_id, suffix)
        try:
            candidate.mkdir(exist_ok=False)
            return candidate
        except FileExistsError:
            attempt += 1
            suffix = f"__{attempt}"


MANIFEST_FILE = "CURRENT.json"


def _manifest_file(cfg: dict) -> Path:
    return cfg["paths"]["published"] / MANIFEST_FILE


def published_manifest(cfg: dict) -> dict | None:
    """What is published, as ONE record. The authority for every consumer.

    There used to be two pointers - data/CURRENT and reports/CURRENT - flipped
    one after the other, so between them a consumer saw tonight's warehouse
    beside last night's reports. No transaction spans two files, so the fix is
    not to order the writes better but to stop having two: one manifest names
    the data version, the report version and the run they both belong to, and
    it is replaced with a single os.replace.

    data/CURRENT and reports/CURRENT are still written, as compatibility caches
    and as the internal signal that each tree passed verification. Nothing
    authoritative reads them; see published_data_dir and published_reports.
    """
    f = _manifest_file(cfg)
    if not f.exists():
        return None
    try:
        return json.loads(f.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _reports_complete(cfg: dict, run_id: str) -> bool:
    version = run_dir(cfg["paths"]["reports"] / "runs", run_id)
    return all((version / name).is_file() for name in REPORT_NAMES)


def publish_run(
    cfg: dict, run_id: str, data_version: str, reports_version: str | None
) -> Path:
    """Bind both trees to one run and make them visible in one write.

    Everything has already been verified by the time this is called: the data
    version by verify_data_version, the report version by the completeness
    check in publish_version. This is the moment they become readable, and it
    is a single atomic filesystem operation.
    """
    manifest = {
        "run_id": run_id,
        "data": f"runs/{data_version}",
        # None only when nothing complete exists to bind - a bare load() with
        # no reports written. run() and the DAG both build the report version
        # BEFORE the data is published, so in the pipeline this is always set
        # and the two trees become visible in the same write.
        "reports": None if reports_version is None else f"runs/{reports_version}",
        "warehouse": f"runs/{data_version}/{WAREHOUSE_FILE}",
        "published_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    target = _manifest_file(cfg)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    os.replace(tmp, target)
    log.info(
        "Published run %s (data %s, reports %s)", run_id, data_version, reports_version
    )
    return target


def _data_current_file(cfg: dict) -> Path:
    return cfg["paths"]["data_runs"].parent / "CURRENT"


def data_current_run_id(cfg: dict) -> str | None:
    """The DIRECTORY data/CURRENT names, or None if nothing is published.

    This is a directory name, which is the run_id only on a run's first
    attempt - a retry publishes `<run_id>__2`. For "which logical run is
    published", use warehouse_run_id(), which reads the stamp written inside
    the version's own database.
    """
    f = _data_current_file(cfg)
    if not f.exists():
        return None
    return f.read_text(encoding="utf-8").strip() or None


def published_data_dir(cfg: dict) -> Path | None:
    """The data version every consumer should read. Always complete.

    bi/build_star_schema.py, and anything else reading the analytics layer,
    goes through here rather than at a fixed directory, so it cannot observe a
    half-swapped set.
    """
    manifest = published_manifest(cfg)
    if manifest is None:
        return None
    d = contained(cfg["paths"]["data_runs"].parent, manifest["data"])
    return d if d.is_dir() else None


def warehouse_path(cfg: dict) -> Path | None:
    """The published SQLite file, or None when nothing is published."""
    d = published_data_dir(cfg)
    return None if d is None else d / WAREHOUSE_FILE


def verify_data_version(
    cfg: dict, version_name: str, tables, run_id: str | None = None
) -> Path:
    """Every file present, readable, and the database stamped with this run.

    Checked before the pointer moves, because after it moves is too late. The
    Parquet files are opened rather than stat-ed: a rename that half-completed
    or a disk that filled leaves a file of the right name and the wrong
    contents, and `exists()` is happy with both.
    """
    version = cfg["paths"]["data_runs"] / version_name
    run_id = run_id or version_name
    missing = [n for n in tables if not (version / f"{n}.parquet").is_file()]
    if missing:
        raise FileNotFoundError(
            f"{version} is missing {', '.join(sorted(missing))} - refusing to "
            f"publish an incomplete data version."
        )
    for name in tables:
        try:
            pq.read_schema(version / f"{name}.parquet")
        except Exception as exc:
            raise OSError(
                f"{version / f'{name}.parquet'} is unreadable: {exc}"
            ) from exc

    db = version / WAREHOUSE_FILE
    if not db.is_file():
        raise FileNotFoundError(f"{db} is missing - refusing to publish.")
    with closing(sqlite3.connect(db, timeout=60.0)) as conn:
        row = conn.execute("SELECT run_id FROM _publication WHERE id = 1").fetchone()
    stamped = row[0] if row else None
    if stamped != run_id:
        # The database and the directory disagreeing about which run they are
        # is the one thing a version-directory scheme cannot tolerate.
        raise ValueError(
            f"{db} is stamped {stamped!r}, not {run_id!r} - refusing to publish."
        )
    return version


def publish_data_version(cfg: dict, version_name: str) -> Path:
    """Make this data version the published one, by writing ONE file.

    data/CURRENT is replaced with os.replace of a temp file, a single atomic
    filesystem operation on POSIX and Windows alike. Before it, consumers see
    the previous version in full; after it, this one. There is no state in
    between, which is the entire point.

    Takes the DIRECTORY name, which is the run_id only on a run's first
    attempt - see new_data_version().
    """
    version = cfg["paths"]["data_runs"] / version_name
    tmp = _data_current_file(cfg).with_suffix(".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(version_name + "\n", encoding="utf-8")
    os.replace(tmp, _data_current_file(cfg))
    log.info("Published data version %s", version_name)
    return version


def prune_data_versions(cfg: dict, keep: int = 3) -> list[str]:
    """Keep the newest `keep` data versions plus whatever CURRENT names.

    Fewer than the report versions, because these are half a gigabyte each
    rather than three markdown files. CURRENT is protected explicitly: a run
    that fails after a successful one leaves a newer directory that is not
    published.
    """
    root = cfg["paths"]["data_runs"]
    if not root.is_dir():
        return []
    protected = {data_current_run_id(cfg)}
    versions = sorted(
        (d for d in root.iterdir() if d.is_dir()),
        key=lambda d: d.stat().st_mtime,
        reverse=True,
    )
    removed = []
    for d in versions[keep:]:
        if d.name in protected:
            continue
        shutil.rmtree(d, ignore_errors=True)
        removed.append(d.name)
    return removed


def reports_dir(cfg: dict, run_id: str) -> Path:
    """This run's version directory: reports/runs/<run_id>/.

    Reports used to be written straight to reports/ by the tasks that compute
    them, all of which run before `publish`. A run whose publish then failed
    left last night's warehouse beside tonight's reports - reports describing
    data nobody can query.

    Staging them and moving them across afterwards was not enough either: three
    separate os.replace() calls can half-succeed, and on Windows they routinely
    do, because replacing a file another process holds open raises
    PermissionError. That leaves reports/ with one new report and two old ones
    and nothing recording it. So a version is a DIRECTORY, built complete and
    then pointed at, and the only thing that changes about the published set is
    one line in one file. See publish_version().
    """
    d = run_dir(cfg["paths"]["reports"] / "runs", run_id)
    d.mkdir(parents=True, exist_ok=True)
    return d


# --- run_id is a directory name, and it is chosen by whoever triggers the DAG -
#
# context["run_id"] reaches this module verbatim from Airflow, and a caller
# picks it: POST /api/v1/dags/.../dagRuns {"dag_run_id": ...}, or the UI's
# "Trigger DAG w/ config" Run ID field. It then becomes a path component in
# data/runs/, data/staging/, reports/runs/ and reports/failed_runs/ - paths
# handed straight to mkdir(parents=True), os.replace() and shutil.rmtree().
#
# Airflow's own allowed_run_id_pattern (default ^[A-Za-z0-9_.~:+-]+$) is not a
# defence here: it lists "." as a literal, so ".." is a legal run_id, and
# data/staging/.. is data/. rmtree() on that, with ignore_errors=True, empties
# the data layer on a fully green run and logs nothing.
#
# Two layers, because either one alone rots: the character class stops the
# payload, and the containment check stops the next person who adds a path
# built from run_id and forgets the character class.
_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._~:+=-]{0,199}$")


def safe_run_id(run_id: str) -> str:
    """The one gate. First character must be alphanumeric - that is what rules
    out "." and "..", which the character class alone would let through."""
    if not isinstance(run_id, str) or not _SAFE_RUN_ID.fullmatch(run_id):
        raise ValueError(
            f"run_id {run_id!r} is not a usable directory name. It must start "
            "with a letter or digit and contain only letters, digits and "
            "._~:+=- (at most 200 characters)."
        )
    return run_id


def run_dir(root: Path, run_id: str, suffix: str = "") -> Path:
    """<root>/<run_id>, verified to still be under <root>."""
    child = Path(root) / (safe_run_id(run_id) + suffix)
    child.resolve().relative_to(Path(root).resolve())  # raises if it escaped
    return child


def contained(root: Path, relative: str) -> Path:
    """<root>/<relative>, verified to still be under <root>.

    The same guard as run_dir for the pointers that name a path rather than a
    bare run_id - the manifest records "runs/<version>", so safe_run_id
    cannot be asked about it directly, but the containment check still can.
    A pointer that resolves outside its own tree is a corrupt or edited
    manifest, not an empty one, so this raises rather than reporting
    "nothing is published" and letting a consumer carry on.
    """
    child = Path(root) / relative
    try:
        child.resolve().relative_to(Path(root).resolve())
    except ValueError:
        raise ValueError(
            f"published pointer {relative!r} resolves outside {root}"
        ) from None
    return child


def _version_root(cfg: dict) -> Path:
    return cfg["paths"]["reports"] / "runs"


def _current_file(cfg: dict) -> Path:
    return cfg["paths"]["reports"] / "CURRENT"


def current_run_id(cfg: dict) -> str | None:
    """The run_id reports/CURRENT names, or None if nothing is published."""
    f = _current_file(cfg)
    if not f.exists():
        return None
    return f.read_text(encoding="utf-8").strip() or None


def published_reports(cfg: dict) -> Path | None:
    """The version directory a reader should read. Always complete.

    Everything that consumes these reports programmatically goes through here,
    so it can never observe a half-swapped set: CURRENT names the old version
    or the new one, never a mixture.
    """
    manifest = published_manifest(cfg)
    if manifest is None or manifest.get("reports") is None:
        return None
    d = contained(cfg["paths"]["reports"], manifest["reports"])
    return d if d.is_dir() else None


def _stamped_run_id(db: Path) -> str | None:
    """The run_id inside a version's own database, or None."""
    if not db.is_file():
        return None
    try:
        with closing(sqlite3.connect(db, timeout=60.0)) as conn:
            row = conn.execute(
                "SELECT run_id FROM _publication WHERE id = 1"
            ).fetchone()
    except sqlite3.Error:
        return None
    return row[0] if row else None


def staged_data_run_id(cfg: dict) -> str | None:
    """The run whose data version has been built and verified, but which may
    not yet be visible to consumers.

    data/CURRENT is the INTERNAL signal that a data version passed
    verify_data_version. The report finaliser needs it, because it has to
    decide whether to publish before anything is published - asking the
    manifest here would be circular, since the manifest is what it is about to
    write.
    """
    name = data_current_run_id(cfg)
    if name is None:
        return None
    return _stamped_run_id(run_dir(cfg["paths"]["data_runs"], name) / WAREHOUSE_FILE)


def warehouse_run_id(cfg: dict) -> str | None:
    """The LOGICAL run whose version is published, or None.

    Two things have to be true for this to answer, and both matter: data/CURRENT
    has to name a directory that exists, and that directory's database has to
    carry a run_id stamped inside load()'s own swap transaction. So it means
    "a complete, verified version for this run is what consumers are reading" -
    which is exactly the question the report finaliser needs answered, and is
    not the same as the directory name, since a retry publishes `<run_id>__2`.
    """
    db = warehouse_path(cfg)
    if db is None or not db.exists():
        return None
    try:
        with closing(sqlite3.connect(db, timeout=60.0)) as conn:
            row = conn.execute(
                "SELECT run_id FROM _publication WHERE id = 1"
            ).fetchone()
    except sqlite3.Error:
        return None  # no manifest table: a warehouse from before this existed
    return row[0] if row else None


def mark_published(cfg: dict, run_id: str) -> None:
    """Record that this version's data reached the warehouse, when it did.

    Diagnostic only. It says "this run published at the time it ran", which is
    not the same as "this run is what the warehouse holds now" - and only the
    second can authorise moving reports/CURRENT. finalize_reports uses it to
    tell a version that has been superseded ("stale") from one that never
    published at all ("failed"); see the note there.
    """
    # Best effort, on purpose. The load has already committed and the
    # warehouse already carries the run_id that authorises publishing, so this
    # file changes nothing about whether the run succeeded. Letting it raise
    # turned a full, committed run into a failed one over a diagnostic write.
    version = run_dir(_version_root(cfg), run_id)
    if not version.is_dir():
        # Do not conjure the directory. An empty runs/<run_id>/ holding only
        # this marker is indistinguishable from a version whose reports were
        # lost, and it is what made the finaliser raise instead of recovering.
        log.warning(
            "No report version for %s to mark; its reports were archived or "
            "never written.",
            run_id,
        )
        return
    try:
        (version / PUBLISHED_MARKER).write_text(run_id + "\n", encoding="utf-8")
    except OSError as exc:
        log.warning(
            "Could not write the %s marker for %s (%s). The warehouse stamp is "
            "what authorises the publish, so this is diagnostic only.",
            PUBLISHED_MARKER,
            run_id,
            exc,
        )


def _snapshot_for_readers(cfg: dict, version: Path) -> None:
    """Copy the published version up to reports/ for people reading on GitHub.

    These top-level copies are a CONVENIENCE, not the contract: the repository
    commits them so nobody has to clone and run the pipeline to see what it
    produces. Nothing in this codebase reads them. CURRENT is the authority,
    and each report names its own run_id, so a copy that failed half way is
    self-identifying rather than quietly wrong.

    Deliberately best-effort: a file somebody has open must not turn a
    successful publish into a failed run.
    """
    for name in REPORT_NAMES:
        src = version / name
        if not src.exists():
            continue
        try:
            shutil.copyfile(src, cfg["paths"]["reports"] / name)
        except OSError as exc:  # pragma: no cover - needs a locked file
            log.warning(
                "Could not refresh the reports/%s copy (%s). reports/CURRENT "
                "still names the published version.",
                name,
                exc,
            )


def publish_version(cfg: dict, run_id: str) -> Path:
    """Make this version the published one, by writing ONE file.

    reports/CURRENT is replaced with os.replace of a temp file - a single
    atomic filesystem operation on POSIX and on Windows. Before the call the
    previous version is published in full; after it, this one is. There is no
    state in between, which is the entire reason a version is a directory
    rather than three files promoted one at a time.
    """
    version = reports_dir(cfg, run_id)
    missing = [n for n in REPORT_NAMES if not (version / n).exists()]
    if missing:
        # Pointing at a version before it is complete is precisely the failure
        # the pointer exists to prevent.
        raise FileNotFoundError(
            "{} is missing {} - refusing to publish an incomplete report "
            "version.".format(version, ", ".join(missing))
        )
    tmp = _current_file(cfg).with_suffix(".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(run_id + "\n", encoding="utf-8")
    os.replace(tmp, _current_file(cfg))
    _snapshot_for_readers(cfg, version)
    # The manifest is the authority, so making a report version current means
    # updating it. Bind the data version finalize has ALREADY verified is
    # current - data_current_run_id - not the existing manifest's pointer.
    #
    # finalize only reaches here when staged_data_run_id(cfg) == run_id, i.e.
    # data/CURRENT names a complete, stamped, verified version for this run, so
    # that is the right data to bind, and in the normal case it equals the
    # manifest's pointer - the warehouse does not move. They DIVERGE only when
    # load() was killed between publish_data_version (which had already flipped
    # data/CURRENT to this run) and load()'s own publish_run (which had not yet
    # written the manifest): the manifest still named the previous run, and
    # reading it here bound the previous warehouse under this run's fresh
    # reports. data_current_run_id is the directory name, so a retry's
    # <run_id>__2 is handled the same way load()'s own publish_run handles it.
    existing = published_manifest(cfg) or {}
    data_dir = data_current_run_id(cfg) or (
        existing.get("data") or f"runs/{run_id}"
    ).removeprefix("runs/")
    publish_run(cfg, run_id, data_dir, run_id)
    log.info("Published report version %s", run_id)
    return version


def keep_failed_reports(cfg: dict, run_id: str) -> Path | None:
    """Park a version that never became current, under reports/failed_runs/.

    The gate's own message says "investigate the source extract", and this is
    what an investigator opens - so it has to survive. It must also never
    become the published set, which is why it moves out of runs/ rather than
    staying somewhere CURRENT could later name.
    """
    version = run_dir(_version_root(cfg), run_id)
    if not version.is_dir():
        return None
    dest = run_dir(cfg["paths"]["reports"] / "failed_runs", run_id)
    if dest.exists():
        shutil.rmtree(dest, ignore_errors=True)
    dest.parent.mkdir(parents=True, exist_ok=True)
    os.replace(version, dest)
    log.warning("Reports for the failed run are in %s", dest)
    return dest


def _restore_archived_reports(cfg: dict, run_id: str) -> None:
    """Bring a version back from failed_runs/ when its data later publishes.

    keep_failed_reports MOVES the directory out of runs/, and nothing moved it
    back. So: publish fails, the finaliser archives the reports, the operator
    fixes the cause and clears `publish` - which the DAG docstring recommends -
    and the publish succeeds. mark_published then recreated runs/<run_id>/
    containing only the marker, and the finaliser raised FileNotFoundError on
    every retry, permanently. The warehouse held the new run, reports/CURRENT
    was stuck on the previous one, and this run's real reports sat in
    failed_runs labelled a failure.

    Only ever called once the data for this run IS published, so restoring
    cannot resurrect a version that should have stayed archived.
    """
    version = run_dir(_version_root(cfg), run_id)
    archived = run_dir(cfg["paths"]["reports"] / "failed_runs", run_id)
    if not archived.is_dir():
        return
    if all((version / n).is_file() for n in REPORT_NAMES):
        return  # the live version is complete; leave the archive alone
    restored = []
    for name in (*REPORT_NAMES, PUBLISHED_MARKER):
        src = archived / name
        if not src.is_file():
            continue
        if (version / name).is_file():
            # Fill GAPS only, never overwrite. The archived copy is by
            # construction older than the live one - it was moved out of runs/
            # by an earlier finalise - so a file the re-run has already rebuilt
            # is this run's, and the archived one belongs to the attempt that
            # failed. Overwriting it published the failed attempt's
            # "GATE FAILED - NOTHING WAS PUBLISHED" report as the current one,
            # describing the rejected extract, beside a run_metrics.json
            # describing the data that DID publish.
            #
            # Recovery from a failed gate rebuilds the version only PARTIALLY -
            # measure_adoption is a root task and is not downstream of the
            # gate, so clearing the gate does not re-run it - which is exactly
            # when this fires.
            continue
        version.mkdir(parents=True, exist_ok=True)
        os.replace(src, version / name)
        restored.append(name)
    if not any(archived.iterdir()):
        # May not fire now: the archive keeps whatever the re-run rebuilt for
        # itself. That is right - those are the failed attempt's diagnostics
        # and belong in failed_runs/.
        archived.rmdir()
    log.info(
        "Restored %s from failed_runs (%s) - its data has since published",
        run_id,
        ", ".join(restored) or "nothing was missing",
    )


def finalize_reports(cfg: dict, run_id: str) -> str:
    """Decide what becomes of this run's version. Safe to run twice.

    Returns "published", "stale", "failed" or "noop".

    One function, run once every task has reached a terminal state, rather than
    an archive task on `one_failed`. `one_failed` fires as soon as ANY upstream
    fails, without waiting for the others, so a gate failure could archive the
    version while the adoption branch - which has no upstream and runs in
    parallel - was still computing. Adoption then wrote its report into a
    directory that had already been moved, and nobody ever saw it.

    **The warehouse is the only thing that can authorise a publish.** The
    marker file used to be accepted as a fallback, and it is not evidence of
    anything current: it records that this run published at the time it ran.
    Publish run_x, publish run_y, then re-run run_x's finaliser - a retry, a
    cleared task, a backfill - and the stale marker rolled CURRENT back to
    run_x while the warehouse held run_y. That is precisely the mixed state the
    pointer exists to prevent, reached by the mechanism meant to prevent it.

    So CURRENT only ever moves to the run the warehouse says it is holding.
    A version with a marker the warehouse has moved past is "stale": it did
    publish once, it is simply not current any more, and it is left where it is
    for prune_report_versions to age out. Archiving it would label a
    successful run a failure.

    Idempotent because Airflow retries tasks: if CURRENT already names this run
    there is nothing to publish, and if the version has already been archived
    there is nothing to move.
    """
    version = run_dir(_version_root(cfg), run_id)
    if current_run_id(cfg) == run_id:
        return "noop"  # already published - this is a retry

    # The published-data check comes FIRST, before "already archived". A run
    # whose publish failed has its reports archived by this function; if the
    # operator then fixes the cause and clears `publish` - which the DAG
    # docstring recommends - the data publishes and this has to be able to
    # bring those reports back. Ordered the other way it returned "noop" on
    # the archived directory and reports/CURRENT stayed a run behind forever.
    #
    # warehouse_run_id, not data_current_run_id: the first is the LOGICAL run
    # stamped inside the published version's own database, the second is a
    # directory name, and a retry publishes `<run_id>__2`. Both require a
    # complete verified version to be published, which is the property the
    # reports must agree with.
    if staged_data_run_id(cfg) == run_id:
        _restore_archived_reports(cfg, run_id)
        # Both trees are verified complete by the two calls below, and only
        # then does ONE write make them visible together.
        publish_version(cfg, run_id)
        return "published"

    if not version.is_dir():
        return "noop"  # already archived, and its data never published

    if (version / PUBLISHED_MARKER).exists():
        # It published, and the warehouse has since moved on. Not a failure,
        # and not something to point CURRENT at.
        log.info(
            "Version %s published earlier; the warehouse is now on %s, so "
            "reports/CURRENT is left alone.",
            run_id,
            warehouse_run_id(cfg),
        )
        return "stale"

    keep_failed_reports(cfg, run_id)
    return "failed"


def prune_report_versions(cfg: dict, keep: int = 5) -> list[str]:
    """Keep the newest `keep` versions, plus whatever CURRENT names.

    One directory per run is unbounded otherwise. CURRENT is protected
    explicitly rather than by assuming it is the newest - a run that fails
    after a successful one leaves a newer directory that is not published.
    """
    root = _version_root(cfg)
    if not root.is_dir():
        return []
    protected = {current_run_id(cfg)}
    versions = sorted(
        (d for d in root.iterdir() if d.is_dir()),
        key=lambda d: d.stat().st_mtime,
        reverse=True,
    )
    removed = []
    for d in versions[keep:]:
        if d.name in protected:
            continue
        shutil.rmtree(d, ignore_errors=True)
        removed.append(d.name)
    return removed


def _copy_database(source: Path, dest: Path) -> None:
    """Snapshot a SQLite database with the backup API, not shutil.copyfile.

    These databases run in WAL mode, and a `.db` file is only the whole
    database once the write-ahead log has been checkpointed into it. The last
    connection to close normally does that, so in a clean sequence there is no
    `-wal` left to miss - which is exactly why copying looked safe. But any
    concurrent reader keeps the log alive: Power BI, bi/build_star_schema.py,
    warehouse_run_id(), or a person with a sqlite3 shell open.

    Copying the file alone in that state does not lose a few recent rows. It
    loses everything since the last checkpoint, including the schema:

        committed rows in source : 1000
        -wal present             : True
        rows after copyfile      : ERROR: no such table: t
        rows after .backup()     : 1000

    The seed exists to carry the previous version's _published manifest
    forward, so a truncated copy would make every table look new and nothing
    look retired. backup() reads through the log and produces a consistent
    snapshot whatever else is attached.
    """
    with (
        closing(sqlite3.connect(source, timeout=60.0)) as src,
        closing(sqlite3.connect(dest, timeout=60.0)) as dst,
    ):
        src.backup(dst)
