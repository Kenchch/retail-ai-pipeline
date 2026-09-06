"""Inspect the positive-sale selection difference against the R policy."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from retail_pipeline.pipeline import check_quality, extract, load_config
from scripts.get_data import EXPECTED_SHA256, _sha256

if __name__ == "__main__":
    cfg = load_config()
    assert _sha256(cfg["paths"]["raw"]) == EXPECTED_SHA256
    raw = extract(cfg)
    clean, _, _ = check_quality(raw, cfg)
    services = {
        "POST",
        "DOT",
        "C2",
        "M",
        "S",
        "D",
        "B",
        "BANK CHARGES",
        "AMAZONFEE",
        "CRUK",
    }
    r_positive = raw.loc[
        ~raw.invoice_no.str.startswith("C", na=False)
        & ~raw.stock_code.str.upper().isin(services)
        & (raw.quantity > 0)
        & (raw.unit_price > 0)
    ]
    extras = r_positive.loc[~r_positive.index.isin(clean.index)]
    duplicates = raw.duplicated(
        subset=["invoice_no", "stock_code", "quantity", "unit_price", "invoice_ts"]
    )
    pads = extras.stock_code.eq("PADS")
    assert duplicates.loc[extras.index[~pads]].all()
    assert len(extras.loc[~pads]) == 5223
    assert len(extras.loc[pads]) == 3

    def revenue(rows):
        return round(float((rows.quantity * rows.unit_price).sum()), 3)

    report = {
        "source_sha256": EXPECTED_SHA256,
        "python_positive_revenue": revenue(clean),
        "r_positive_revenue": revenue(r_positive),
        "duplicate_rows": int((~pads).sum()),
        "duplicate_revenue": revenue(extras.loc[~pads]),
        "pads_rows": int(pads.sum()),
        "pads_revenue": revenue(extras.loc[pads]),
        "scope": "Positive-sale selection only; cancellation netting is in the R analysis.",
    }
    path = cfg["paths"]["reports"] / "reconciliation.json"
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(path.read_text())
