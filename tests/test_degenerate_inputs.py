"""Regression tests for degenerate inputs.

Every test here corresponds to a defect that shipped. They exist because the
original test suite only ever exercised the one dataset where nothing is
missing - all five teams present, all twelve weeks populated, every column
filled - and a green suite over the happy path says nothing about what happens
when a team stops using the product or a source feed arrives empty.

The recurring defect they guard against has one shape: **deriving the set of
things that should exist from the data that happens to be present.** A team
with no events, a week with no activity and a day with no sales are all facts
worth reporting, and all three used to vanish instead.
"""

import pandas as pd
import pytest

from retail_pipeline.adoption import (
    headline_metrics,
    team_metrics,
    weekly_metrics,
)
from retail_pipeline.config import Config
from retail_pipeline.quality import run_checks, split_quarantine
from retail_pipeline.transform import build_dim_date, transform


# --------------------------------------------------------------------------- #
# The roster is the source of truth
# --------------------------------------------------------------------------- #

def test_licensed_headcount_is_derived_from_the_roster(cfg):
    """It must not be possible for the total and the per-team numbers to
    disagree, so the total is never stored separately."""
    assert cfg.adoption["licensed_users"] == sum(cfg.adoption["roster"].values())


def test_team_with_zero_adoption_is_reported_at_zero_not_omitted(events, cfg):
    """The defect: the team list came from the event log, so a team that never
    opened the report disappeared from the report about who opens the report."""
    cfg.adoption["roster"] = {"Team A": 1, "Team B": 1, "Ghost Team": 5}
    out = team_metrics(events, cfg, cfg.adoption["roster"]).set_index("team")

    assert "Ghost Team" in out.index
    assert out.loc["Ghost Team", "reach_pct"] == 0.0
    assert out.loc["Ghost Team", "activation_pct"] == 0.0
    assert out.loc["Ghost Team", "licensed_users"] == 5


def test_team_using_it_but_not_on_the_roster_is_surfaced(events, cfg):
    """The mirror image: someone using it who was never licensed means the
    rollout list is stale. Dropping them hides that too."""
    cfg.adoption["roster"] = {"Team A": 1}
    out = team_metrics(events, cfg, cfg.adoption["roster"]).set_index("team")
    assert "Team B" in out.index


# --------------------------------------------------------------------------- #
# The weekly trend is a calendar, not a list of weeks that had traffic
# --------------------------------------------------------------------------- #

def _three_weeks_with_a_gap() -> pd.DataFrame:
    rows = [
        ("2026-04-06 10:00", "U1", "Team A", "view", "S1", None),
        ("2026-04-06 10:01", "U1", "Team A", "apply", "S1", None),
        # 2026-04-13 -> nobody used it at all
        ("2026-04-20 10:00", "U1", "Team A", "view", "S1", None),
    ]
    df = pd.DataFrame(
        rows,
        columns=["event_ts", "user_id", "team", "event_type", "stock_code", "feedback_score"],
    )
    df["event_ts"] = pd.to_datetime(df["event_ts"])
    df["week_start"] = df["event_ts"].dt.to_period("W-SUN").dt.start_time
    return df


def test_week_with_zero_activity_is_kept_as_a_zero(cfg):
    """The defect: grouping by week dropped the empty week, the trend line
    closed over the gap, and every later week's label shifted one to the left."""
    w = weekly_metrics(_three_weeks_with_a_gap(), cfg)

    assert len(w) == 3, "the empty middle week must still be a row"
    assert list(w["week_no"]) == [1, 2, 3]
    assert w.iloc[1]["active_users"] == 0
    assert w.iloc[1]["reach_pct"] == 0.0
    # Week 3 must still be labelled week 3, not promoted into week 2's slot.
    assert w.iloc[2]["week_start"] == pd.Timestamp("2026-04-20")


def test_action_rate_is_undefined_not_zero_when_nothing_was_viewed(cfg):
    """0 applies out of 0 views is not a 0% action rate - nobody ignored
    anything. A fabricated zero drags the trend down and reads as rejection."""
    w = weekly_metrics(_three_weeks_with_a_gap(), cfg)
    assert pd.isna(w.iloc[1]["action_rate_pct"])
    assert pd.isna(w.iloc[1]["sessions_per_active_user"])
    assert w.iloc[0]["action_rate_pct"] == 100.0   # 1 apply / 1 view


# --------------------------------------------------------------------------- #
# The date dimension is a calendar
# --------------------------------------------------------------------------- #

def test_dim_date_is_continuous_over_the_period(sample, cfg):
    """The defect: dim_date only contained days that traded. This retailer is
    shut on Saturdays, so 53 of them did not exist - and a BI user grouping by
    week silently got six-day weeks."""
    _, flags = run_checks(sample, cfg)
    clean, _ = split_quarantine(sample, flags, cfg)

    df = clean.copy()
    # Force a gap: move one row a week later so the days between are empty.
    df.loc[df.index[-1], "invoice_ts"] = df["invoice_ts"].max() + pd.Timedelta(days=7)
    df["date_key"] = pd.to_datetime(df["invoice_ts"].dt.date)
    df["revenue"] = df["quantity"] * df["unit_price"]

    dim = build_dim_date(df)
    expected = pd.date_range(dim["date_key"].min(), dim["date_key"].max(), freq="D")
    assert list(dim["date_key"]) == list(expected)
    assert (~dim["has_sales"]).any(), "closed days must be present and flagged"


def test_fact_dates_all_resolve_in_the_calendar(sample, cfg):
    _, flags = run_checks(sample, cfg)
    clean, _ = split_quarantine(sample, flags, cfg)
    tables = transform(clean)
    assert set(tables["fact_sales"]["date_key"]) <= set(tables["dim_date"]["date_key"])


def test_dateless_input_fails_with_an_explanation(sample, cfg):
    """A source system changing its date format turns every timestamp into NaT.
    That must say so, not surface as a pandas internal error."""
    _, flags = run_checks(sample, cfg)
    clean, _ = split_quarantine(sample, flags, cfg)
    clean["invoice_ts"] = pd.NaT
    clean["date_key"] = pd.NaT
    with pytest.raises(ValueError, match="date_format"):
        build_dim_date(clean)


# --------------------------------------------------------------------------- #
# Empty and near-empty frames
# --------------------------------------------------------------------------- #

def test_quarantine_bookkeeping_survives_zero_and_one_row(sample, cfg):
    """`.apply(axis=1)` over an empty frame returns a DataFrame, so writing the
    reasons column used to raise - the run died on bookkeeping, not on data."""
    for frame in (sample.iloc[0:0], sample.iloc[[0]], sample.iloc[[3]]):
        _, flags = run_checks(frame, cfg)
        clean, quarantine = split_quarantine(frame, flags, cfg)
        assert len(clean) + len(quarantine) == len(frame)
        assert "quarantine_reasons" in quarantine.columns


def test_no_usage_at_all_reports_zeros_rather_than_failing(cfg):
    """"Nobody has used it yet" is a real state for an adoption report to be
    in - arguably the most important one - and must not be an exception."""
    empty = pd.DataFrame(
        columns=["event_ts", "user_id", "team", "event_type", "stock_code",
                 "feedback_score", "week_start"]
    )
    empty["event_ts"] = pd.to_datetime(empty["event_ts"])

    weekly = weekly_metrics(empty, cfg)
    teams = team_metrics(empty, cfg, cfg.adoption["roster"])
    head = headline_metrics(empty, weekly, cfg).set_index("metric")

    assert weekly.empty
    assert len(teams) == len(cfg.adoption["roster"])   # every team still listed
    assert (teams["reach_pct"] == 0).all()
    assert head.loc["reach_pct", "value"] == 0.0
    assert pd.isna(head.loc["action_rate_pct", "value"])   # undefined, not 0
    assert head.loc["action_rate_pct", "status"] == "no data"


def test_a_metric_with_no_data_is_not_reported_as_below_target(events, cfg):
    """A null CSAT means nobody answered, not that everybody was unhappy."""
    no_feedback = events[events["event_type"] != "feedback"]
    head = headline_metrics(no_feedback, weekly_metrics(no_feedback, cfg), cfg)
    row = head.set_index("metric").loc["csat"]
    assert pd.isna(row["value"])
    assert row["status"] == "no data"
