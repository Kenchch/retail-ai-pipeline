"""Fetch the transaction data and generate the usage telemetry.

    python scripts/get_data.py

**Transactions** - UCI "Online Retail" (a UK online giftware retailer, Dec 2010
to Dec 2011), via the mirror in Databricks' "Spark: The Definitive Guide" repo
because the UCI host is not reliably reachable.

**Usage telemetry** - generated here, because the solution has not been rolled
out to anyone. The schema is the production schema and the adoption metrics are
computed from it for real; in production the same table comes from Power BI
report usage metrics (views), the merchandising tool's audit log (applies) and
an in-report feedback button. Swapping in a real extract is one line in
config.yaml. This is stated in the README too - an adoption number of unclear
provenance is worse than no adoption number.

The simulated rollout is deliberately imperfect - Store Ops lags badly, some
people never engage at all, and the weekly curve wobbles. Workshops in weeks 3
and 8 produce visible step changes. A clean upward line would demonstrate
nothing.
"""

from __future__ import annotations

import hashlib
import os
import random
import shutil
import sys
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
URL = (
    "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/"
    "master/data/retail-data/all/online-retail-dataset.csv"
)
TRANSACTIONS = ROOT / "data" / "raw" / "online_retail.csv"
EVENTS = ROOT / "data" / "raw" / "usage_events.csv"


# LAUNCH is a naive calendar anchor for simulated local timestamps, not a real
# instant. Making it tz-aware would rewrite every event_ts in the generated CSV
# and move the input digest with it - a formatting change relocating a
# provenance record is the exact failure that digest was fixed to stop.
SEED, LAUNCH, WEEKS = 20260802, datetime(2026, 4, 6), 12  # noqa: DTZ001
WORKSHOPS = {3, 8}
# Headcount comes from config.yaml - one source of truth. Only behaviour is here.
BEHAVIOUR = {
    "Category Management": {"base": 0.55, "dormant": 0.10},
    "Merchandising": {"base": 0.48, "dormant": 0.15},
    "Online Trading": {"base": 0.50, "dormant": 0.10},
    "Marketing": {"base": 0.32, "dormant": 0.35},
    "Store Ops": {"base": 0.16, "dormant": 0.55},
}
CURVE = {
    1: 0.35,
    2: 0.50,
    3: 0.72,
    4: 0.80,
    5: 0.86,
    6: 0.62,
    7: 0.80,
    8: 1.0,
    9: 1.06,
    10: 1.10,
    11: 1.08,
    12: 1.12,
}

# The mirror serves one fixed revision of the UCI file. Pinning its digest is
# what turns "the download finished" into "the download is the file the
# published numbers were computed from" - see _fetch below for why the first
# does not imply the second.
EXPECTED_SHA256 = "a2f79bbdd4463df6db8a3f5a50b9c980ae8f645a370bf5e2c0d6097f9e817b05"
EXPECTED_BYTES = 45_038_760
DOWNLOAD_TIMEOUT_S = 60


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _fetch(url: str, dest: Path) -> None:
    """Download to a sidecar, verify, then rename into place.

    Writing straight to `dest` is what makes a half-download durable. Both
    truncation modes were reproduced against a local server:

    * server sends no Content-Length and closes early -> urlretrieve returns
      *normally*, no exception at all;
    * server sends an honest Content-Length and closes early (what this mirror
      actually does) -> ContentTooShortError is raised, but the partial file is
      NOT removed.

    Either way a truncated CSV ends up at the final path, and download()'s
    exists() check then makes it permanent: every later run prints "Already
    present" and re-publishes from it. Nothing downstream catches this - a
    20 MB truncation still quarantines at 3.70%, far under the 30% ceiling, so
    the gate passes and the run exits 0 having silently dropped 55% of the
    data. Recommendations are ratios, so they do not come out smaller, they
    come out *different*, at the same apparent confidence.

    A `.part` sidecar plus a digest check makes the failure loud and, crucially,
    leaves no artefact behind for the next run to trust.
    """
    part = dest.with_name(dest.name + ".part")
    part.unlink(missing_ok=True)
    try:
        with (
            urllib.request.urlopen(url, timeout=DOWNLOAD_TIMEOUT_S) as resp,
            part.open("wb") as out,
        ):
            shutil.copyfileobj(resp, out)

        got = part.stat().st_size
        if got != EXPECTED_BYTES:
            raise OSError(
                f"{dest.name}: expected {EXPECTED_BYTES:,} bytes, got {got:,}. "
                "The download was truncated or the mirror changed; nothing was published."
            )
        digest = _sha256(part)
        if digest != EXPECTED_SHA256:
            raise OSError(
                f"{dest.name}: sha256 {digest[:16]} does not match the expected "
                f"{EXPECTED_SHA256[:16]}. The mirror's contents changed; update "
                "EXPECTED_SHA256/EXPECTED_BYTES deliberately rather than publishing "
                "from an unrecognised file."
            )
        os.replace(part, dest)  # atomic: dest is either old or complete
    finally:
        part.unlink(missing_ok=True)  # never leave a partial for the next run


def download() -> None:
    TRANSACTIONS.parent.mkdir(parents=True, exist_ok=True)
    if TRANSACTIONS.exists():
        # Verify rather than assume. An existing file may be a truncation left
        # by an older revision of this script, and "it is on disk" was exactly
        # the assumption that made such a file permanent.
        if _sha256(TRANSACTIONS) == EXPECTED_SHA256:
            print(f"Already present and verified: {TRANSACTIONS.name}")
            return
        print(
            f"{TRANSACTIONS.name} does not match the expected digest - re-downloading"
        )
        TRANSACTIONS.unlink()
    print(f"Downloading -> {TRANSACTIONS}")
    _fetch(URL, TRANSACTIONS)


def generate_events() -> None:
    roster = yaml.safe_load((ROOT / "config.yaml").read_text())["adoption"]["roster"]
    rng = random.Random(SEED)

    codes = [f"SKU{i:05d}" for i in range(400)]
    products = ROOT / "data" / "processed" / "dim_product.parquet"
    if products.exists():  # real stock codes, so usage joins back to the catalogue
        top = pd.read_parquet(products, columns=["stock_code", "n_invoices"])
        codes = top.nlargest(400, "n_invoices")["stock_code"].tolist()

    users = []
    for team, headcount in roster.items():
        spec = BEHAVIOUR.get(team, {"base": 0.4, "dormant": 0.2})
        for i in range(headcount):
            dormant = rng.random() < spec["dormant"]  # real rollouts always have some
            users.append(
                {
                    "user_id": f"{team[:2].upper()}{i:03d}",
                    "team": team,
                    "p": 0.0 if dormant else max(0.05, rng.gauss(spec["base"], 0.18)),
                }
            )

    rows = []
    for week in range(1, WEEKS + 1):
        start = LAUNCH + timedelta(weeks=week - 1)
        for u in users:
            p = min(0.95, u["p"] * CURVE[week])
            if week in WORKSHOPS and rng.random() < 0.35:
                p = min(0.95, p + 0.25)  # a workshop converts some non-users
            if rng.random() > p:
                continue
            for _ in range(rng.choice([1, 1, 1, 2, 2, 3])):
                ts = start + timedelta(
                    days=rng.randint(0, 4),
                    hours=rng.randint(8, 17),
                    minutes=rng.randint(0, 59),
                )
                for _ in range(rng.randint(1, 6)):
                    code = rng.choice(codes)
                    rows.append((ts, u["user_id"], u["team"], "view", code, None))
                    if rng.random() < 0.09 + 0.14 * min(u["p"], 1.0) + 0.008 * week:
                        rows.append(
                            (
                                ts + timedelta(minutes=rng.randint(1, 20)),
                                u["user_id"],
                                u["team"],
                                "apply",
                                code,
                                None,
                            )
                        )
                if rng.random() < 0.16:
                    mu = 3.4 if u["team"] == "Store Ops" else 4.3
                    rows.append(
                        (
                            ts + timedelta(minutes=rng.randint(1, 40)),
                            u["user_id"],
                            u["team"],
                            "feedback",
                            None,
                            int(min(5, max(1, round(rng.gauss(mu, 0.9))))),
                        )
                    )

    df = pd.DataFrame(
        rows,
        columns=[
            "event_ts",
            "user_id",
            "team",
            "event_type",
            "stock_code",
            "feedback_score",
        ],
    )

    # kind="stable": 1,395 of these rows share a timestamp with another row, and
    # the default quicksort gives no ordering guarantee among ties. It happens
    # to be reproducible on one pandas/numpy build; that is an implementation
    # accident, not a contract. A generated file with a seeded RNG should have a
    # row order that is pinned here, at the point of writing, rather than left
    # to whatever the sort does on the reader's machine.
    df = df.sort_values("event_ts", kind="stable")

    # Int64, not float: feedback_score is None for view/apply rows, which forces
    # float64 and writes "5.0". Older pandas writes "5" for the same data, so
    # the file's bytes -- and any digest over them -- would depend on the writer's
    # version. The nullable integer type writes "5" and "" on every version.
    df["feedback_score"] = df["feedback_score"].astype("Int64")

    # lineterminator="\n" for the same reason as the Int64 cast above: to_csv
    # otherwise defaults it to os.linesep, so this file lands as CRLF on Windows
    # and LF on Linux from an identical seed. The fingerprint in pipeline.py no
    # longer depends on that (it pins its own terminator), but "a seeded
    # generator produces the same file everywhere" is the property this function
    # claims, and one os.linesep is all it takes to lose it.
    df.to_csv(EVENTS, index=False, lineterminator="\n")
    print(
        f"Wrote {len(df):,} usage events for {df['user_id'].nunique()} users -> {EVENTS.name}"
    )


if __name__ == "__main__":
    download()
    generate_events()
    sys.exit(0)
