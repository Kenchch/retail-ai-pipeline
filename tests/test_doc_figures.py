import json
from pathlib import Path

import pandas as pd
import pytest

from retail_pipeline.doc_figures import document_figures
from retail_pipeline.pipeline import check_quality, extract, load_config, transform
from retail_pipeline.recommend import recommend

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


def _evaluation() -> dict:
    return json.loads((ROOT / "reports/evaluation.json").read_text(encoding="utf-8"))


def _readme_flat() -> str:
    return " ".join((ROOT / "README.md").read_text(encoding="utf-8").split())


def test_readme_evaluation_table_matches_the_evaluation_report():
    """The hit-rates move whenever the recommender changes.

    They did in the change that added this test: fixing the content fallback's
    tie handling moved hybrid from 56.30% to 56.32% and content from 51.66% to
    51.71%. A table typed once and left alone would have kept the old pair, and
    nothing in the repository would have disagreed with it.

    The README rounds to one decimal and states each model against the
    most-popular baseline on the basket rate, so the difference is checked as
    well: it is the number a reader takes away, and it moves when either rate
    does.
    """
    report = _evaluation()
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    labels = {
        "Co-occurrence baseline": "co_occurrence",
        "Hybrid (published)": "hybrid",
        "Content TF-IDF": "content_tfidf",
        "Most popular": "most_popular",
    }
    assert set(labels.values()) == set(report["models"]), "a model has no README row"
    baseline = 100 * report["models"]["most_popular"]["basket_hit_rate_at_k"]
    for label, key in labels.items():
        model = report["models"][key]
        rate = 100 * model["basket_hit_rate_at_k"]
        if key == "most_popular":
            against = "baseline"
        else:
            # U+2212 for a negative difference, as the README typesets it.
            against = f"{rate - baseline:+.1f} pts".replace("-", "\u2212")
        row = (
            f"| {label} | {rate:.1f}% | {against} "
            f"| {100 * model['hit_rate_at_k']:.1f}% "
            f"| {100 * model['query_coverage']:.1f}% |"
        )
        assert row in readme, f"README does not carry the current row for {label}"


def test_readme_summary_states_the_evaluation_against_its_baselines():
    """The summary at the top repeats the hybrid and baseline rates and their
    difference, so it is held to the same report as the table. It also names
    the baseline that beats the published model, which is the sentence most
    likely to be dropped the next time the summary is tidied."""
    models = _evaluation()["models"]
    readme = _readme_flat()
    hybrid = 100 * models["hybrid"]["basket_hit_rate_at_k"]
    baseline = 100 * models["most_popular"]["basket_hit_rate_at_k"]
    simple = 100 * models["co_occurrence"]["basket_hit_rate_at_k"]
    assert (
        f"{hybrid:.1f}% basket hit-rate@5 against {baseline:.1f}% for a most-popular "
        f"baseline ({hybrid - baseline:+.1f} points) on "
        f"{models['hybrid']['baskets']:,} held-out baskets"
    ) in readme
    assert simple > hybrid, "the README says the co-occurrence baseline wins"
    assert f"no thresholds reaches {simple:.1f}%" in readme
    assert f"it costs {simple - hybrid:.1f} points of basket hit-rate" in readme


def test_readme_explains_the_two_rates_with_the_reports_own_counts():
    """The paragraph on why per-query flatters most-popular quotes four counts
    and two rates. All six come from the report."""
    models = _evaluation()["models"]
    popular, hybrid = models["most_popular"], models["hybrid"]
    large = hybrid["by_basket_size"]["61+"]
    readme = _readme_flat()
    assert f"then the {hybrid['baskets']:,} baskets" in readme
    assert (
        f"the {large['baskets']:,} baskets of more than 60 products supply "
        f"{large['queries']:,} of the {hybrid['queries']:,} queries"
    ) in readme
    assert (
        f"most popular scores {100 * popular['hit_rate_at_k']:.1f}% per query and "
        f"{100 * popular['basket_hit_rate_at_k']:.1f}% per basket"
    ) in readme


def test_readme_states_what_each_ranking_key_scores():
    report = _evaluation()
    keys = report["hybrid_by_rank_key"]
    assert report["published_rank_by"] == "lift", "the README says lift is published"
    lift = 100 * keys["lift"]["basket_hit_rate_at_k"]
    confidence = 100 * keys["confidence"]["basket_hit_rate_at_k"]
    assert confidence > lift, "the README says confidence ranking scores higher"
    assert (
        f"confidence reach {confidence:.1f}%, against {lift:.1f}% by lift"
    ) in _readme_flat()


def test_readme_cold_start_and_oversized_counts_match_the_published_table():
    """Two counts the README takes from the recommendation table itself. They
    are recomputed only where the source extract is present."""
    cfg = load_config()
    if not cfg["paths"]["raw"].exists():
        pytest.skip("the source extract is not downloaded")
    clean, _, _ = check_quality(extract(cfg), cfg)
    tables = transform(clean)
    recs = recommend(tables, cfg)
    cold = recs.loc[recs.method == "content_tfidf", "stock_code"].nunique()
    sizes = tables["fact_sales"].groupby("invoice_no").stock_code.nunique()
    oversized = int((sizes > cfg["recommend"]["max_basket_size"]).sum())
    readme = _readme_flat()
    assert (
        f"leaves {cold:,} of {len(tables['dim_product']):,} products to the text "
        "fallback"
    ) in readme
    assert f"The {oversized:,} larger ones are outside every rule" in readme


def test_readme_results_table_matches_its_reports():
    """Every row of the results table, from the report that produces it.

    The cancellation row went unchecked and stayed at the figures from before
    credit lines were deduplicated (GBP 388,322.16 / 9,859,031.12) while the
    prose under it moved on, so the table and the paragraph beneath it
    disagreed on the same measure.
    """
    m = json.loads((ROOT / "reports/run_metrics.json").read_text(encoding="utf-8"))
    c = json.loads((ROOT / "reports/cancellations.json").read_text(encoding="utf-8"))
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert f"| Input invoice lines | {m['rows_source']:,} |" in readme
    assert (
        f"| Loaded / quarantined | {m['rows_loaded']:,} / {m['rows_quarantined']:,} "
        f"({m['quarantine_rate_pct']:.1f}%, each with the rules it broke) |"
    ) in readme
    assert f"| Gross accepted positive sales | £{c['gross_gbp']:,.2f} over" in readme
    assert (
        f"| £{c['matched_credit_gbp']:,.2f} / "
        f"£{c['net_of_matched_cancellations_gbp']:,.2f}, the same as the R analysis |"
    ) in readme
    assert (
        f"| Recommendation rows | {m['recommendations']:,}, covering all "
        f"{m['products']:,} products |"
    ) in readme
    assert m["catalogue_coverage_pct"] == 100.0, (
        "the table says every product is covered"
    )


def test_readme_query_count_matches_the_evaluation_report():
    queries = _evaluation()["models"]["hybrid"]["queries"]
    assert f"The {queries:,} queries are basket-completion queries" in (
        ROOT / "README.md"
    ).read_text(encoding="utf-8")


def test_readme_credit_matching_figures_match_their_report():
    """The README states the same three figures as DESIGN.md, in its own
    wording. Two copies of a number is two chances to leave one behind."""
    c = json.loads((ROOT / "reports/cancellations.json").read_text(encoding="utf-8"))
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert (
        f"flags {c['matched_accepted_sales_rows']:,} accepted lines matched" in readme
    )
    assert (
        f"({c['matched_share_of_all_credit_rows_pct']:.1f}% of all "
        f"{c['credit_note_rows']:,} credit-note lines)" in readme
    )
    net = c["net_of_matched_cancellations_gbp"]
    assert f"**net of these exact matches is \u00a3{net:,.2f}**" in readme
