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

import random
import sys
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
URL = ("https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/"
       "master/data/retail-data/all/online-retail-dataset.csv")
TRANSACTIONS = ROOT / "data" / "raw" / "online_retail.csv"
EVENTS = ROOT / "data" / "raw" / "usage_events.csv"

SEED, LAUNCH, WEEKS = 20260802, datetime(2026, 4, 6), 12
WORKSHOPS = {3, 8}
# Headcount comes from config.yaml - one source of truth. Only behaviour is here.
BEHAVIOUR = {
    "Category Management": {"base": 0.55, "dormant": 0.10},
    "Merchandising":       {"base": 0.48, "dormant": 0.15},
    "Online Trading":      {"base": 0.50, "dormant": 0.10},
    "Marketing":           {"base": 0.32, "dormant": 0.35},
    "Store Ops":           {"base": 0.16, "dormant": 0.55},
}
CURVE = {1: .35, 2: .50, 3: .72, 4: .80, 5: .86, 6: .62,
         7: .80, 8: 1.0, 9: 1.06, 10: 1.10, 11: 1.08, 12: 1.12}


def download() -> None:
    TRANSACTIONS.parent.mkdir(parents=True, exist_ok=True)
    if TRANSACTIONS.exists():
        print(f"Already present: {TRANSACTIONS.name}")
        return
    print(f"Downloading -> {TRANSACTIONS}")
    urllib.request.urlretrieve(URL, TRANSACTIONS)


def generate_events() -> None:
    roster = yaml.safe_load((ROOT / "config.yaml").read_text())["adoption"]["roster"]
    rng = random.Random(SEED)

    codes = [f"SKU{i:05d}" for i in range(400)]
    products = ROOT / "data" / "processed" / "dim_product.parquet"
    if products.exists():   # real stock codes, so usage joins back to the catalogue
        top = pd.read_parquet(products, columns=["stock_code", "n_invoices"])
        codes = top.nlargest(400, "n_invoices")["stock_code"].tolist()

    users = []
    for team, headcount in roster.items():
        spec = BEHAVIOUR.get(team, {"base": 0.4, "dormant": 0.2})
        for i in range(headcount):
            dormant = rng.random() < spec["dormant"]   # real rollouts always have some
            users.append({"user_id": f"{team[:2].upper()}{i:03d}", "team": team,
                          "p": 0.0 if dormant else max(0.05, rng.gauss(spec["base"], 0.18))})

    rows = []
    for week in range(1, WEEKS + 1):
        start = LAUNCH + timedelta(weeks=week - 1)
        for u in users:
            p = min(0.95, u["p"] * CURVE[week])
            if week in WORKSHOPS and rng.random() < 0.35:
                p = min(0.95, p + 0.25)       # a workshop converts some non-users
            if rng.random() > p:
                continue
            for _ in range(rng.choice([1, 1, 1, 2, 2, 3])):
                ts = start + timedelta(days=rng.randint(0, 4), hours=rng.randint(8, 17),
                                       minutes=rng.randint(0, 59))
                for _ in range(rng.randint(1, 6)):
                    code = rng.choice(codes)
                    rows.append((ts, u["user_id"], u["team"], "view", code, None))
                    if rng.random() < 0.09 + 0.14 * min(u["p"], 1.0) + 0.008 * week:
                        rows.append((ts + timedelta(minutes=rng.randint(1, 20)),
                                     u["user_id"], u["team"], "apply", code, None))
                if rng.random() < 0.16:
                    mu = 3.4 if u["team"] == "Store Ops" else 4.3
                    rows.append((ts + timedelta(minutes=rng.randint(1, 40)),
                                 u["user_id"], u["team"], "feedback", None,
                                 int(min(5, max(1, round(rng.gauss(mu, 0.9)))))))

    df = pd.DataFrame(rows, columns=["event_ts", "user_id", "team", "event_type",
                                     "stock_code", "feedback_score"]).sort_values("event_ts")
    df.to_csv(EVENTS, index=False)
    print(f"Wrote {len(df):,} usage events for {df['user_id'].nunique()} users -> {EVENTS.name}")


if __name__ == "__main__":
    download()
    generate_events()
    sys.exit(0)
