import pandas as pd
import pytest

from retail_pipeline.credits import flag_reversed_sales
from retail_pipeline.pipeline import check_quality, load_config, transform


def frame(rows):
    out = pd.DataFrame(
        rows, columns=["invoice_no", "invoice_ts", "quantity", "customer_id"]
    )
    out["invoice_ts"] = pd.to_datetime(out["invoice_ts"])
    return out.assign(stock_code="A", unit_price=2.5, description="MUG", country="UK")


def test_chronology_latest_sale_and_one_to_one_with_unsorted_input():
    sales = frame(
        [
            ("later", "2011-01-03", 2, 1),
            ("old", "2011-01-01", 2, 1),
            ("recent", "2011-01-02", 2, 1),
        ]
    )
    credits = frame(
        [
            ("Csecond", "2011-01-02 12:00", -2, 1),
            ("Cfirst", "2011-01-02 10:00", -2, 1),
            ("Cthird", "2011-01-02 13:00", -2, 1),
        ]
    )
    flagged = flag_reversed_sales(sales, credits).set_index("invoice_no")
    assert not flagged.loc["later", "reversed_by_credit"]
    assert flagged.loc["recent", "matched_credit_invoice"] == "Cfirst"
    assert flagged.loc["old", "matched_credit_invoice"] == "Csecond"
    assert flagged.reversed_by_credit.sum() == 2


def test_same_time_ties_are_deterministic_and_duplicate_index_is_safe():
    sales = frame([("first", "2011-01-01", 1, 1), ("last", "2011-01-01", 1, 1)])
    sales.index = [0, 0]
    flagged = flag_reversed_sales(sales, frame([("C1", "2011-01-01", -1, 1)]))
    assert flagged.reversed_by_credit.tolist() == [False, True]


@pytest.mark.parametrize(
    "change",
    [
        {"quantity": -1},
        {"customer_id": 2},
        {"customer_id": None},
        {"stock_code": "B"},
        {"unit_price": 2.51},
        {"invoice_ts": pd.NaT},
        {"invoice_no": "uncoded"},
        {"quantity": 2},
    ],
)
def test_partial_or_ineligible_credits_do_not_net(change):
    sales = frame([("sale", "2011-01-01", 2, 1)])
    credit = frame([("C1", "2011-01-02", -2, 1)]).assign(**change)
    assert not flag_reversed_sales(sales, credit).reversed_by_credit.any()


def test_quality_duplicates_gross_and_quarantine_are_preserved():
    source = frame(
        [
            ("sale", "2011-01-01", 2, 1),
            ("sale", "2011-01-01", 2, 1),
            ("C1", "2011-01-02", -2, 1),
            ("C2", "2011-01-02", -2, 1),
            ("guest", "2011-01-01", 2, None),
            ("Cg", "2011-01-02", -2, None),
        ]
    )
    cfg = load_config()
    cfg["quality"]["max_quarantine_rate"] = 1
    clean, quarantine, _ = check_quality(source, cfg)
    fact = transform(clean)["fact_sales"]
    assert len(clean) == 2 and len(quarantine) == 4
    assert len(clean) + len(quarantine) == len(source)
    assert fact.revenue.sum() == 10
    assert fact.loc[~fact.reversed_by_credit, "revenue"].sum() == 5
    assert fact.loc[fact.reversed_by_credit, "matched_credit_invoice"].tolist() == [
        "C1"
    ]


def test_transform_requires_credit_matching_instead_of_silent_false_net():
    with pytest.raises(ValueError, match="check_quality"):
        transform(frame([("sale", "2011-01-01", 2, 1)]))
