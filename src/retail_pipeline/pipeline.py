"""Pipeline orchestration: extract -> quality -> transform -> load -> recommend.

Each stage is importable on its own (that is what lets the Airflow DAG in
dags/ call them as separate tasks), and this module is the single place that
wires them together and records what the run did.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import pandas as pd

from .config import Config, get_logger
from .extract import extract
from .load import load
from .quality import run_checks, split_quarantine, write_report
from .recommend import recommend
from .transform import transform

log = get_logger(__name__)


def run(config_path: str | None = None) -> dict:
    started = time.perf_counter()
    cfg = Config.load(config_path)
    cfg.ensure_dirs()

    log.info("=" * 78)
    log.info("STAGE 1/5  EXTRACT")
    raw = extract(cfg)

    log.info("=" * 78)
    log.info("STAGE 2/5  DATA QUALITY")
    results, flags = run_checks(raw, cfg)
    clean, quarantine = split_quarantine(raw, flags, cfg)
    write_report(results, cfg, len(raw), len(clean), len(quarantine))

    log.info("=" * 78)
    log.info("STAGE 3/5  TRANSFORM (star schema)")
    tables = transform(clean)
    tables["quarantine"] = quarantine
    tables["dq_results"] = results

    log.info("=" * 78)
    log.info("STAGE 4/5  LOAD")
    load(tables, cfg)

    log.info("=" * 78)
    log.info("STAGE 5/5  RECOMMEND (frequently bought together)")
    recs = recommend(tables, cfg)
    load({"recommendations": recs}, cfg)

    elapsed = time.perf_counter() - started
    metrics = {
        "run_ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "runtime_seconds": round(elapsed, 1),
        "rows_source": int(len(raw)),
        "rows_quarantined": int(len(quarantine)),
        "quarantine_rate_pct": round(100 * len(quarantine) / len(raw), 3),
        "rows_loaded": int(len(clean)),
        "invoices_source": int(raw["invoice_no"].nunique()),
        "invoices_loaded": int(tables["fact_sales"]["invoice_no"].nunique()),
        "products": int(len(tables["dim_product"])),
        "customers": int(len(tables["dim_customer"])),
        "revenue_loaded": round(float(tables["fact_sales"]["revenue"].sum()), 2),
        "recommendation_rows": int(len(recs)),
        "products_with_recommendations": int(recs["stock_code"].nunique()) if len(recs) else 0,
        "catalogue_coverage_pct": (
            round(100 * recs["stock_code"].nunique() / len(tables["dim_product"]), 2)
            if len(recs) else 0.0
        ),
        "checks": results.to_dict(orient="records"),
    }
    out = cfg.paths["reports"] / "run_metrics.json"
    out.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    log.info("=" * 78)
    log.info("Pipeline finished in %.1fs | metrics -> %s", elapsed, out)
    _print_sample(recs)
    return metrics


def _print_sample(recs: pd.DataFrame, n: int = 5) -> None:
    if recs.empty:
        return
    sample = (
        recs[recs["method"] == "co_purchase"]
        .sort_values("lift", ascending=False)
        .head(n)
    )
    log.info("Top co-purchase rules by lift:")
    for _, r in sample.iterrows():
        log.info(
            "  %-38s -> %-38s lift %.1f  conf %.2f  (%s baskets)",
            str(r["description"])[:38], str(r["recommended_description"])[:38],
            r["lift"], r["confidence"], f"{int(r['pair_baskets']):,}",
        )


if __name__ == "__main__":
    run()
