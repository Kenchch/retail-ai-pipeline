import pandas as pd

from retail_pipeline.evaluate import co_occurrence_lookup, evaluate, score_baskets
from retail_pipeline.pipeline import load_config


def test_basket_scoring_excludes_self_and_counts_uncovered_queries():
    metrics = score_baskets([["A", "B", "B"], ["C"]], {"A": ["A", "B"]}, 1)
    by_size = metrics.pop("by_basket_size")
    assert metrics == {
        "baskets": 1,
        "queries": 2,
        "hits": 1,
        "basket_hit_rate_at_k": 0.5,
        "hit_rate_at_k": 0.5,
        "query_coverage": 0.5,
    }
    assert by_size["2-5"] == {
        "baskets": 1,
        "queries": 2,
        "basket_hit_rate_at_k": 0.5,
        "hit_rate_at_k": 0.5,
    }
    assert by_size["61+"]["baskets"] == 0
    assert by_size["61+"]["basket_hit_rate_at_k"] is None


def test_a_large_basket_counts_once_in_the_headline_rate():
    """Ten products that all hit, and a pair that misses.

    Per query that is 10 hits in 12, which is the large basket's result with a
    little noise. Per basket it is (1.0 + 0.0) / 2: the model served one basket
    of the two.
    """
    big = [f"P{i}" for i in range(10)]
    lookup = {code: [other for other in big if other != code][:1] for code in big}
    metrics = score_baskets([big, ["X", "Y"]], lookup, 1)
    assert metrics["hit_rate_at_k"] == 10 / 12
    assert metrics["basket_hit_rate_at_k"] == 0.5
    assert metrics["by_basket_size"]["6-10"]["basket_hit_rate_at_k"] == 1.0
    assert metrics["by_basket_size"]["2-5"]["basket_hit_rate_at_k"] == 0.0


def test_co_occurrence_baseline_ranks_by_shared_baskets_then_fills_from_popular():
    rows = [("1", "A"), ("1", "B"), ("2", "A"), ("2", "B"), ("3", "A"), ("3", "C")]
    rows += [("4", "D")]
    rows += [("5", p) for p in "ABCDE"]  # over the cap: its pairs are not counted
    train = pd.DataFrame(rows, columns=["invoice_no", "stock_code"])
    lookup = co_occurrence_lookup(train, list("ABCDE"), 2, max_basket_size=3)
    assert lookup["A"] == ["B", "C"]  # 2 shared baskets, then 1
    assert lookup["C"] == ["A", "B"]  # 1 shared basket, then the best seller left
    assert lookup["D"] == ["A", "B"]  # never paired: best sellers only


def test_temporal_evaluation_does_not_learn_test_only_products():
    rows = []
    for invoice, day, products in [
        ("1", "2011-09-01", ["A", "B"]),
        ("2", "2011-10-02", ["C", "D"]),
    ]:
        for product in products:
            rows.append(
                dict(
                    invoice_no=invoice,
                    invoice_ts=pd.Timestamp(day),
                    stock_code=product,
                    description=f"GARDEN ITEM {product}",
                    quantity=1,
                    unit_price=1.0,
                    customer_id="customer",
                    country="UK",
                    reversed_by_credit=False,
                    matched_credit_invoice=None,
                )
            )
    cfg = load_config()
    cfg["recommend"].update(min_support_count=1, min_confidence=0, min_lift=0)
    result = evaluate(pd.DataFrame(rows), cfg)
    assert result["training_rows"] == result["test_rows"] == 2
    assert result["models"]["hybrid"]["query_coverage"] == 0
    assert all(model["hits"] == 0 for model in result["models"].values())
    assert set(result["hybrid_by_rank_key"]) == {"lift", "confidence"}
    assert result["published_rank_by"] == "lift"
