"""Every data-quality rule gets a test with a known answer.

These are the tests that matter most in a pipeline like this: if a rule
silently stops firing, nothing crashes - bad rows just start flowing into the
warehouse. A unit test is the only thing that catches that.
"""

import pandas as pd

from retail_pipeline.quality import CHECKS, run_checks, split_quarantine, write_report

EXPECTED_FAILURES = {
    "duplicate_line_items": 1,
    "missing_invoice_key": 0,
    "cancelled_invoice": 1,
    "non_positive_quantity": 1,
    "non_positive_price": 1,
    "price_outlier": 1,
    "non_product_stock_code": 1,
    "missing_description": 1,
    "missing_customer_id": 1,
}


def test_every_check_fires_the_expected_number_of_times(sample, cfg):
    results, _ = run_checks(sample, cfg)
    counts = dict(zip(results["check"], results["failed_rows"]))
    assert counts == EXPECTED_FAILURES


def test_check_names_are_unique():
    names = [c.name for c in CHECKS]
    assert len(names) == len(set(names))


def test_quarantine_keeps_only_clean_rows(sample, cfg):
    _, flags = run_checks(sample, cfg)
    clean, quarantine = split_quarantine(sample, flags, cfg)

    # 9 rows in, 6 of them break a blocking rule
    assert len(clean) == 3
    assert len(quarantine) == 6
    assert len(clean) + len(quarantine) == len(sample)

    # No cancellations, no negative quantities, no zero prices survive.
    assert not clean["invoice_no"].str.startswith("C").any()
    assert (clean["quantity"] > 0).all()
    assert (clean["unit_price"] >= cfg.quality["min_unit_price"]).all()


def test_non_blocking_rows_are_not_quarantined(sample, cfg):
    """A guest checkout with a blank description is still a real sale."""
    _, flags = run_checks(sample, cfg)
    clean, _ = split_quarantine(sample, flags, cfg)
    assert "536384" in set(clean["invoice_no"])


def test_quarantine_records_the_reason(sample, cfg):
    _, flags = run_checks(sample, cfg)
    _, quarantine = split_quarantine(sample, flags, cfg)
    reasons = dict(zip(quarantine["invoice_no"], quarantine["quarantine_reasons"]))
    assert "cancelled_invoice" in reasons["C536379"]
    assert "non_positive_quantity" in reasons["536380"]
    assert "price_outlier" in reasons["536382"]
    assert all(r for r in quarantine["quarantine_reasons"])  # never blank


def test_pipeline_refuses_to_load_when_quality_collapses(sample, cfg):
    """The gate must actually stop the run, not just log a warning."""
    import pytest

    cfg.quality["max_quarantine_rate"] = 0.01
    _, flags = run_checks(sample, cfg)
    with pytest.raises(ValueError, match="exceeds the configured ceiling"):
        split_quarantine(sample, flags, cfg)


def test_report_is_written_and_readable(sample, cfg, tmp_path):
    cfg.paths["reports"] = tmp_path
    results, flags = run_checks(sample, cfg)
    clean, quarantine = split_quarantine(sample, flags, cfg)
    text = write_report(results, cfg, len(sample), len(clean), len(quarantine))

    assert (tmp_path / "data_quality_report.md").exists()
    for check in CHECKS:
        assert check.name in text
    assert isinstance(results, pd.DataFrame) and len(results) == len(CHECKS)
