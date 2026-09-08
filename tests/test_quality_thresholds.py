"""A ceiling nothing can reach is not a ceiling.

`max_quarantine_rate` was 0.30 against an observed 3.57% -- 8.4x headroom, so
every blocking rule could have fired three times as often and the run would
still have loaded and called itself healthy. These tests hold the two
thresholds against the run that is actually published, so a value drifting back
to decorative fails rather than passing quietly.
"""

import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def _config() -> dict:
    return yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))


def _metrics() -> dict:
    return json.loads((ROOT / "reports/run_metrics.json").read_text(encoding="utf-8"))


def test_the_quarantine_ceiling_is_close_enough_to_the_observed_rate_to_fire():
    """Between 1.2x and 2.5x the published rate.

    Below 1.2x the run trips on ordinary variation in returns and
    cancellations; above 2.5x the rate can double -- the event this exists to
    catch -- without the ceiling noticing.
    """
    ceiling = _config()["quality"]["max_quarantine_rate"]
    observed = _metrics()["quarantine_rate_pct"] / 100
    ratio = ceiling / observed
    assert 1.2 <= ratio <= 2.5, (
        f"the ceiling is {ceiling:.0%} against an observed {observed:.2%}, "
        f"{ratio:.1f}x. Under 1.2x it fires on ordinary variation; over 2.5x "
        f"the rate can double without tripping it."
    )


def test_a_doubling_of_the_observed_rate_would_trip_the_ceiling():
    """Stated as the event rather than as a ratio, because that is the claim
    the config comment makes and the reason the number is what it is."""
    ceiling = _config()["quality"]["max_quarantine_rate"]
    observed = _metrics()["quarantine_rate_pct"] / 100
    assert observed * 2 > ceiling, (
        f"the rate could double, from {observed:.2%} to {2 * observed:.2%}, "
        f"and stay under the {ceiling:.0%} ceiling"
    )


def test_the_price_ceiling_clears_the_dearest_real_product():
    """The cap is a tripwire for a product line priced like an adjustment. If
    it drops below a price the catalogue genuinely charges, it stops being a
    tripwire and starts deleting sales.

    GBP 649.50 is the highest product-coded unit price in the extract: a
    60-piece wicker picnic basket, stock code 22502.
    """
    assert _config()["quality"]["max_unit_price"] > 649.50


def test_the_config_records_that_the_price_rule_catches_nothing_alone():
    """Measured: all 120 rows over the cap carry a stock code that
    `non_product_stock_code` already rejects, so the rule quarantines no row on
    its own. That is worth knowing about a rule someone might otherwise read as
    load-bearing, and the reason it is kept is worth stating next to it.
    """
    config_text = (ROOT / "config.yaml").read_text(encoding="utf-8")
    assert "catches nothing on its own" in config_text
    assert "non_product_stock_code" in config_text
