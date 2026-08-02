"""Tests for the two remaining seams: assembling recommendations, and rendering.

Both were at or near zero coverage. `recommend()` and `dashboard.build()` are
glue - they take correct pieces and put them together - and every defect found
in this project so far has been in glue rather than in an algorithm.
"""

import pandas as pd

from retail_pipeline.adoption import headline_metrics, team_metrics, weekly_metrics
from retail_pipeline.dashboard import build
from retail_pipeline.recommend import RECOMMENDATION_COLUMNS, recommend


def _tables(pairs: int = 40) -> dict[str, pd.DataFrame]:
    """`pairs` baskets each containing A and B, plus a lone product C."""
    rows = []
    for i in range(pairs):
        rows += [(f"INV{i}", "A"), (f"INV{i}", "B")]
    rows += [("INVC", "C"), ("INVC", "D"), ("INVE", "E"), ("INVE", "F")]
    fact = pd.DataFrame(rows, columns=["invoice_no", "stock_code"])
    dim = pd.DataFrame(
        {
            "stock_code": ["A", "B", "C", "D", "E", "F"],
            "description": ["RED MUG LARGE", "RED MUG SMALL",
                            "BLUE PLATE ROUND", "BLUE PLATE SQUARE",
                            "BLUE MUG ROUND", "RED PLATE LARGE"],
        }
    )
    return {"fact_sales": fact, "dim_product": dim}


def test_recommendations_carry_names_and_method(cfg):
    cfg.recommend["min_support_count"] = 5
    recs = recommend(_tables(), cfg)

    assert list(recs.columns) == RECOMMENDATION_COLUMNS
    ab = recs[(recs["stock_code"] == "A") & (recs["recommended_stock_code"] == "B")]
    assert len(ab) == 1
    # Names are joined on so the table can go straight to a business user.
    assert ab["description"].iat[0] == "RED MUG LARGE"
    assert ab["recommended_description"].iat[0] == "RED MUG SMALL"
    assert ab["method"].iat[0] == "co_purchase"


def test_the_two_signals_are_labelled_and_never_conflated(cfg):
    """A reader must be able to tell a behavioural rule from a text-similarity
    guess - the user guide is written around that column."""
    cfg.recommend["min_support_count"] = 5
    recs = recommend(_tables(), cfg)
    assert set(recs["method"]) <= {"co_purchase", "content_tfidf"}
    # C and D never met the support threshold, so they can only be covered by
    # the content fallback - and must be labelled as such.
    cd = recs[recs["stock_code"].isin(["C", "D"])]
    assert not cd.empty
    assert (cd["method"] == "content_tfidf").all()
    assert (cd["pair_baskets"] == 0).all()


def test_a_catalogue_with_no_shared_vocabulary_degrades_quietly(cfg):
    """The defect: TF-IDF raises when no term survives `min_df=2` pruning, or
    when descriptions are only stop words. The content fallback is optional -
    its failure must not take down the co-purchase rules that did compute."""
    cfg.recommend["min_support_count"] = 5
    tables = _tables()
    tables["dim_product"]["description"] = [
        f"WIDGET{i} ALPHA{i}" for i in range(len(tables["dim_product"]))
    ]
    recs = recommend(tables, cfg)
    assert not recs.empty                                   # A->B survived
    assert set(recs["method"]) == {"co_purchase"}           # fallback bowed out


def test_stop_word_only_descriptions_do_not_crash(cfg):
    cfg.recommend["min_support_count"] = 5
    tables = _tables()
    tables["dim_product"]["description"] = "the and of it"
    recs = recommend(tables, cfg)
    assert set(recs["method"]) == {"co_purchase"}


def test_an_empty_result_keeps_its_schema(cfg):
    """The defect: with nothing to recommend, this returned a DataFrame with
    zero columns, and the load stage then failed on `CREATE TABLE x ()` with a
    syntax error pointing at a bracket."""
    cfg.recommend["min_support_count"] = 10_000        # nothing can qualify
    tables = _tables()
    tables["dim_product"]["description"] = "UNKNOWN"   # kills the fallback too

    recs = recommend(tables, cfg)
    assert recs.empty
    assert list(recs.columns) == RECOMMENDATION_COLUMNS


def _adoption_from(events, cfg):
    weekly = weekly_metrics(events, cfg)
    return {
        "adoption_headline": headline_metrics(events, weekly, cfg),
        "adoption_weekly": weekly,
        "adoption_by_team": team_metrics(events, cfg, cfg.adoption["roster"]),
        "adoption_top_products": pd.DataFrame(),
    }


def test_dashboard_renders_from_real_metrics(events, cfg, tmp_path):
    cfg.paths["reports"] = tmp_path
    html = (tmp_path / "adoption_dashboard.html")
    build(_adoption_from(events, cfg), cfg)

    text = html.read_text(encoding="utf-8")
    assert text.startswith("<!DOCTYPE html>")
    assert "adoption" in text.lower()
    # Every roster team reaches the table, including ones with no activity.
    for team in cfg.adoption["roster"]:
        assert team in text


def test_dashboard_renders_when_nobody_used_it(cfg, tmp_path):
    """The defect: an empty weekly table made the period header format a NaN as
    a date, so the page that reports "nobody used it" was the one page that
    could not be produced."""
    cfg.paths["reports"] = tmp_path
    empty = pd.DataFrame(
        columns=["event_ts", "user_id", "team", "event_type", "stock_code",
                 "feedback_score", "week_start"]
    )
    empty["event_ts"] = pd.to_datetime(empty["event_ts"])

    build(_adoption_from(empty, cfg), cfg)
    text = (tmp_path / "adoption_dashboard.html").read_text(encoding="utf-8")

    assert "no activity recorded" in text
    assert "no data this period" in text     # tiles say unknown, not zero
    for team in cfg.adoption["roster"]:
        assert team in text
