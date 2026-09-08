import json
from pathlib import Path

import pandas as pd

from retail_pipeline.doc_figures import document_figures

ROOT = Path(__file__).resolve().parents[1]


def test_workshop_direction_is_pink_to_blue():
    rows = pd.DataFrame(
        [
            dict(
                stock_code="P",
                description="CHILDS GARDEN SPADE PINK",
                recommended_description="CHILDS GARDEN SPADE BLUE",
                method="co_purchase",
                pair_baskets=40,
                confidence=0.48,
                lift=95.11,
            ),
            dict(
                stock_code="B",
                description="CHILDS GARDEN SPADE BLUE",
                recommended_description="CHILDS GARDEN SPADE PINK",
                method="co_purchase",
                pair_baskets=40,
                confidence=0.40,
                lift=95.11,
            ),
        ]
    )
    assert document_figures(rows, 2)["pairs"]["spade"]["confidence"] == 0.48


def test_documented_examples_match_committed_figures():
    figures = json.loads((ROOT / "reports/doc_figures.json").read_text())
    guide = (ROOT / "docs/02_user_guide.md").read_text(encoding="utf-8")
    comms = (ROOT / "docs/03_adoption_and_comms.md").read_text(encoding="utf-8")
    lantern = figures["pairs"]["lantern"]
    assert (
        f"{lantern['baskets']} baskets, confidence {lantern['confidence']:.2f}, lift {lantern['lift']:.2f}"
        in comms
    )
    assert f"{figures['t_light_products']} different products" in guide
    assert f"median lift of {figures['t_light_median_lift']:.1f}" in guide


def test_readme_evaluation_table_matches_the_evaluation_report():
    """The three hit-rates move whenever the recommender changes.

    They did in the change that added this test: fixing the content fallback's
    tie handling moved hybrid from 56.30% to 56.32% and content from 51.66% to
    51.71%. A table typed once and left alone would have kept the old pair, and
    nothing in the repository would have disagreed with it.
    """
    report = json.loads((ROOT / "reports/evaluation.json").read_text(encoding="utf-8"))
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    labels = {
        "Hybrid": "hybrid",
        "Most popular": "most_popular",
        "Content TF-IDF": "content_tfidf",
    }
    for label, key in labels.items():
        model = report["models"][key]
        row = (
            f"| {label} | {100 * model['hit_rate_at_k']:.2f}% "
            f"| {100 * model['query_coverage']:.2f}% |"
        )
        assert row in readme, f"README does not carry the current row for {label}"


def test_readme_query_count_matches_the_evaluation_report():
    report = json.loads((ROOT / "reports/evaluation.json").read_text(encoding="utf-8"))
    queries = report["models"]["hybrid"]["queries"]
    assert f"The {queries:,} queries are basket-completion queries" in (
        ROOT / "README.md"
    ).read_text(encoding="utf-8")
