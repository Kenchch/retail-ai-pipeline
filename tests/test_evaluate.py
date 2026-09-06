import pandas as pd

from retail_pipeline.evaluate import evaluate, score_baskets
from retail_pipeline.pipeline import load_config


def test_basket_scoring_excludes_self_and_counts_uncovered_queries():
    metrics = score_baskets([["A", "B", "B"], ["C"]], {"A": ["A", "B"]}, 1)
    assert metrics == {
        "queries": 2,
        "hits": 1,
        "hit_rate_at_k": 0.5,
        "query_coverage": 0.5,
    }


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
                )
            )
    cfg = load_config()
    cfg["recommend"].update(min_support_count=1, min_confidence=0, min_lift=0)
    result = evaluate(pd.DataFrame(rows), cfg)
    assert result["training_rows"] == result["test_rows"] == 2
    assert result["models"]["hybrid"]["query_coverage"] == 0
    assert all(model["hits"] == 0 for model in result["models"].values())
