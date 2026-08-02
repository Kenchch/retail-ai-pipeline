"""Tests for the star schema and the recommendation rules."""

import pandas as pd

from retail_pipeline.quality import run_checks, split_quarantine
from retail_pipeline.recommend import build_baskets, co_purchase_rules
from retail_pipeline.transform import transform


def test_star_schema_shape_and_keys(sample, cfg):
    _, flags = run_checks(sample, cfg)
    clean, _ = split_quarantine(sample, flags, cfg)
    tables = transform(clean)

    assert set(tables) == {"fact_sales", "dim_product", "dim_customer", "dim_date"}

    fact, dim_p, dim_c = tables["fact_sales"], tables["dim_product"], tables["dim_customer"]

    # Dimensions have exactly one row per key.
    assert dim_p["stock_code"].is_unique
    assert dim_c["customer_id"].is_unique
    assert tables["dim_date"]["date_key"].is_unique

    # Referential integrity: every fact key exists in its dimension.
    assert set(fact["stock_code"]) <= set(dim_p["stock_code"])
    assert set(fact["customer_id"].dropna()) <= set(dim_c["customer_id"])
    assert set(fact["date_key"]) <= set(tables["dim_date"]["date_key"])

    # Descriptive attributes stay out of the fact table.
    assert "description" not in fact.columns


def test_revenue_is_quantity_times_price(sample, cfg):
    _, flags = run_checks(sample, cfg)
    clean, _ = split_quarantine(sample, flags, cfg)
    fact = transform(clean)["fact_sales"]
    expected = (fact["quantity"] * fact["unit_price"]).round(4)
    pd.testing.assert_series_equal(fact["revenue"], expected, check_names=False)


def _basket_frame() -> pd.DataFrame:
    """20 baskets: A and B always bought together, C bought alone."""
    rows = []
    for i in range(20):
        rows += [(f"INV{i}", "A"), (f"INV{i}", "B")]
    for i in range(20, 40):
        rows += [(f"INV{i}", "C"), (f"INV{i}", "D")]
    return pd.DataFrame(rows, columns=["invoice_no", "stock_code"])


def test_lift_flags_a_real_association(cfg):
    cfg.recommend["min_support_count"] = 5
    baskets = build_baskets(_basket_frame(), cfg)
    rules = co_purchase_rules(baskets, cfg)

    ab = rules[(rules["stock_code"] == "A") & (rules["recommended_stock_code"] == "B")]
    assert len(ab) == 1
    # A and B co-occur in 20 of 40 baskets and never apart:
    #   confidence = 1.0, support = 0.5, lift = 1.0 / 0.5 = 2.0
    assert ab["confidence"].iat[0] == 1.0
    assert ab["support"].iat[0] == 0.5
    assert round(ab["lift"].iat[0], 6) == 2.0

    # Products from disjoint baskets never produce a rule.
    assert rules[(rules["stock_code"] == "A") &
                 (rules["recommended_stock_code"] == "C")].empty


def test_rules_are_directional(cfg):
    cfg.recommend["min_support_count"] = 5
    rules = co_purchase_rules(build_baskets(_basket_frame(), cfg), cfg)
    pairs = set(zip(rules["stock_code"], rules["recommended_stock_code"]))
    assert ("A", "B") in pairs and ("B", "A") in pairs


def test_oversized_baskets_are_dropped(cfg):
    cfg.recommend["max_basket_size"] = 3
    df = pd.DataFrame(
        [("BIG", c) for c in "ABCDE"] + [("SMALL", "A"), ("SMALL", "B")],
        columns=["invoice_no", "stock_code"],
    )
    baskets = build_baskets(df, cfg)
    assert list(baskets.index) == ["SMALL"]
