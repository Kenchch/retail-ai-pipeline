"""Generate synthetic usage telemetry for the deployed recommendation report.

WHY THIS EXISTS
---------------
The adoption metrics in this project are real code running over a real event
schema - but the solution has not actually been rolled out to anyone, so there
is no real telemetry to run it over. Rather than quote invented numbers in a
document, this script generates a seeded, reproducible event log with the exact
schema the production source would have, and the adoption module computes the
metrics from it for real.

IN PRODUCTION the same table would come from, in order of preference:
  1. Power BI / Fabric report usage metrics (`Get report usage metrics`), which
     already logs viewer, report page and timestamp per view;
  2. the merchandising tool's own audit log for `apply` events (a recommendation
     accepted onto a planogram or a bundle promotion);
  3. an in-report feedback button writing to a SharePoint list or Dataverse.

Nothing downstream of this file knows or cares that the data is simulated -
swapping in the real extract is a change to `paths.usage_events` in config.yaml.

The shape of the simulated rollout is deliberately imperfect, because a clean
upward line teaches nobody anything: Store Ops lags the other teams, week 6
shows a dip over the school holidays, and the two workshops in weeks 3 and 8
produce visible step changes. Those are the facts the business-side documents
in docs/ are written around.

Usage:
    python scripts/simulate_usage.py
"""

from __future__ import annotations

import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "raw" / "usage_events.csv"
PRODUCTS = ROOT / "data" / "processed" / "dim_product.parquet"

SEED = 20260802
LAUNCH = datetime(2026, 4, 6)      # Monday of week 1
WEEKS = 12
WORKSHOP_WEEKS = {3, 8}            # AI-literacy sessions - expect a step change

# Simulation-only behaviour per team. Headcount is NOT here - it is read from
# `adoption.roster` in config.yaml, which is the one place it is defined. A
# second copy would drift, and the drift would show up as an adoption number
# that quietly disagrees with the roster it claims to divide by.
#
# `base` is the weekly probability an engaged person uses it; `dormant` is the
# share of the team that never engages at all. Real rollouts always have some -
# averaging them away is how an adoption number flatters.
BEHAVIOUR = {
    "Category Management": {"base": 0.55, "dormant": 0.10},
    "Merchandising":       {"base": 0.48, "dormant": 0.15},
    "Online Trading":      {"base": 0.50, "dormant": 0.10},
    "Marketing":           {"base": 0.32, "dormant": 0.35},
    "Store Ops":           {"base": 0.16, "dormant": 0.55},
}
DEFAULT_BEHAVIOUR = {"base": 0.40, "dormant": 0.20}


def load_roster() -> dict[str, int]:
    with open(ROOT / "config.yaml", "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)["adoption"]["roster"]

# Weekly multiplier on engagement: slow start, workshop bumps, holiday dip.
WEEK_CURVE = {
    1: 0.35, 2: 0.50, 3: 0.72, 4: 0.80, 5: 0.86, 6: 0.62,
    7: 0.80, 8: 1.00, 9: 1.06, 10: 1.10, 11: 1.08, 12: 1.12,
}


def _load_stock_codes() -> list[str]:
    """Use real stock codes so usage joins back to dim_product."""
    if PRODUCTS.exists():
        codes = pd.read_parquet(PRODUCTS, columns=["stock_code", "n_invoices"])
        # Weight towards products that actually sell - that is what gets looked at.
        top = codes.sort_values("n_invoices", ascending=False).head(400)
        return top["stock_code"].tolist()
    print("dim_product.parquet not found - falling back to placeholder codes.")
    return [f"SKU{i:05d}" for i in range(400)]


def main() -> int:
    rng = random.Random(SEED)
    codes = _load_stock_codes()

    users = []
    for team, headcount in load_roster().items():
        spec = BEHAVIOUR.get(team, DEFAULT_BEHAVIOUR)
        for i in range(headcount):
            dormant = rng.random() < spec["dormant"]
            users.append(
                {
                    "user_id": f"{team[:2].upper()}{i:03d}",
                    "team": team,
                    # Per-person enthusiasm, so adoption is not uniform inside a team.
                    "propensity": 0.0 if dormant else max(0.05, rng.gauss(spec["base"], 0.18)),
                }
            )

    rows = []
    for week in range(1, WEEKS + 1):
        curve = WEEK_CURVE[week]
        week_start = LAUNCH + timedelta(weeks=week - 1)
        for user in users:
            p_active = min(0.95, user["propensity"] * curve)
            # A workshop converts some of the not-yet-engaged in that week.
            if week in WORKSHOP_WEEKS and rng.random() < 0.35:
                p_active = min(0.95, p_active + 0.25)
            if rng.random() > p_active:
                continue

            n_sessions = rng.choice([1, 1, 1, 2, 2, 3])
            for _ in range(n_sessions):
                ts = week_start + timedelta(
                    days=rng.randint(0, 4),                 # weekdays only
                    hours=rng.randint(8, 17),
                    minutes=rng.randint(0, 59),
                )
                n_views = rng.randint(1, 6)
                for _ in range(n_views):
                    code = rng.choice(codes)
                    rows.append((ts, user["user_id"], user["team"], "view", code, None))

                    # Acting on a recommendation is the behaviour that matters;
                    # experienced users act more often than new ones.
                    p_apply = 0.09 + 0.14 * min(user["propensity"], 1.0) + 0.008 * week
                    if rng.random() < p_apply:
                        rows.append(
                            (ts + timedelta(minutes=rng.randint(1, 20)),
                             user["user_id"], user["team"], "apply", code, None)
                        )
                if rng.random() < 0.18:
                    rows.append(
                        (ts + timedelta(minutes=rng.randint(1, 30)),
                         user["user_id"], user["team"], "export", None, None)
                    )
                if rng.random() < 0.16:
                    # Feedback skews positive but Store Ops is more critical -
                    # that gap is the finding the comms pack is written around.
                    mu = 3.4 if user["team"] == "Store Ops" else 4.3
                    score = int(min(5, max(1, round(rng.gauss(mu, 0.9)))))
                    rows.append(
                        (ts + timedelta(minutes=rng.randint(1, 40)),
                         user["user_id"], user["team"], "feedback", None, score)
                    )

    df = pd.DataFrame(
        rows, columns=["event_ts", "user_id", "team", "event_type", "stock_code", "feedback_score"]
    ).sort_values("event_ts")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)
    print(
        f"Wrote {len(df):,} events for {df['user_id'].nunique()} users "
        f"over {WEEKS} weeks -> {OUT}"
    )
    print(df["event_type"].value_counts().to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())
