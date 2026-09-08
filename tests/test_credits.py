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


def test_a_credit_recorded_twice_reverses_one_sale_not_two():
    """The extract contains 37 credit lines that are exact duplicates by the
    same key the quality rules use to quarantine duplicate sales, and they
    reversed 4 real sales twice over -- GBP 2,363.28 of gross treated as
    cancelled by a credit note that exists once.

    Duplicate *sales* were already quarantined by `duplicate_line_items`. The
    credit side read the raw source, so the same shape of row was quarantined
    in one direction and honoured in the other.
    """
    sales = pd.DataFrame(
        {
            "customer_id": [1.0, 1.0],
            "stock_code": ["A", "A"],
            "quantity": [2, 2],
            "unit_price": [5.0, 5.0],
            "invoice_ts": pd.to_datetime(["2011-01-01 09:00", "2011-01-02 09:00"]),
            "invoice_no": ["100", "101"],
        }
    )
    duplicated_credit = pd.DataFrame(
        {
            "customer_id": [1.0, 1.0],
            "stock_code": ["A", "A"],
            "quantity": [-2, -2],
            "unit_price": [5.0, 5.0],
            "invoice_ts": pd.to_datetime(["2011-01-03 09:00"] * 2),
            "invoice_no": ["C900", "C900"],
        }
    )
    source = pd.concat([sales, duplicated_credit], ignore_index=True)

    flagged = flag_reversed_sales(sales, source)
    assert flagged["reversed_by_credit"].sum() == 1, (
        "one credit note recorded twice reversed two separate sales"
    )
    assert flagged.loc[flagged["reversed_by_credit"], "invoice_no"].tolist() == ["101"]


def test_two_genuinely_separate_credits_still_reverse_two_sales():
    """The guard on the guard: deduplicating must not collapse real repeats.

    Same customer, product, quantity and price, but two different credit
    invoices at different times -- two returns, not one recorded twice.
    """
    sales = pd.DataFrame(
        {
            "customer_id": [1.0, 1.0],
            "stock_code": ["A", "A"],
            "quantity": [2, 2],
            "unit_price": [5.0, 5.0],
            "invoice_ts": pd.to_datetime(["2011-01-01 09:00", "2011-01-02 09:00"]),
            "invoice_no": ["100", "101"],
        }
    )
    separate_credits = pd.DataFrame(
        {
            "customer_id": [1.0, 1.0],
            "stock_code": ["A", "A"],
            "quantity": [-2, -2],
            "unit_price": [5.0, 5.0],
            "invoice_ts": pd.to_datetime(["2011-01-03 09:00", "2011-01-04 09:00"]),
            "invoice_no": ["C900", "C901"],
        }
    )
    source = pd.concat([sales, separate_credits], ignore_index=True)

    flagged = flag_reversed_sales(sales, source)
    assert flagged["reversed_by_credit"].sum() == 2, (
        "two separate credit notes were collapsed into one"
    )


def test_the_credit_key_is_the_quality_rule_key():
    """Two definitions of "duplicate line" in one pipeline is a defect waiting
    to happen, so the rule's subset is read rather than restated."""
    import inspect

    from retail_pipeline import pipeline
    from retail_pipeline.credits import DUPLICATE_KEY

    rule = next(c for c in pipeline.CHECKS if c.name == "duplicate_line_items")
    source = inspect.getsource(rule.fn)
    for column in DUPLICATE_KEY:
        assert f'"{column}"' in source, (
            f"the quality rule no longer keys on {column}, so the credit side "
            f"is now deduplicating by a different definition"
        )
