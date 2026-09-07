"""Recompute credit evidence and reconcile it to the published sales fact."""

import hashlib
import json

import pandas as pd

from retail_pipeline.pipeline import (
    check_quality,
    extract,
    load_config,
    published_data_dir,
    published_manifest,
    transform,
)


def main():
    cfg = load_config()
    raw = extract(cfg)
    clean, _, _ = check_quality(raw, cfg)
    expected = transform(clean)["fact_sales"]
    published = pd.read_parquet(published_data_dir(cfg) / "fact_sales.parquet")
    # Parquet normalizes datetime resolution; compare exact values, not units.
    pd.testing.assert_frame_equal(
        expected, published, check_dtype=False, check_exact=True
    )
    flagged = published["reversed_by_credit"]
    gross = round(float(published.revenue.sum()), 2)
    credits = round(float(published.loc[flagged, "revenue"].sum()), 2)
    credit_rows = int(raw.invoice_no.str.startswith("C", na=False).sum())
    report = {
        "run_id": published_manifest(cfg)["run_id"],
        "source_sha256": hashlib.sha256(cfg["paths"]["raw"].read_bytes()).hexdigest(),
        "source_rows": len(raw),
        "accepted_sales_rows": len(published),
        "credit_note_rows": credit_rows,
        "matched_accepted_sales_rows": int(flagged.sum()),
        "matched_share_of_all_credit_rows_pct": round(
            100 * flagged.sum() / credit_rows, 2
        )
        if credit_rows
        else 0,
        "gross_gbp": gross,
        "matched_credit_gbp": credits,
        "net_of_matched_cancellations_gbp": round(gross - credits, 2),
        "policy": "Exact chronological one-to-one matches against quality-accepted sales; "
        "sale-date attribution; full-extract hindsight; unmatched/partial credits excluded.",
    }
    output = cfg["paths"]["reports"] / "cancellations.json"
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(output.read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
