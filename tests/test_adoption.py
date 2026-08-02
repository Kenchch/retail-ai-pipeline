"""Tests for the adoption metrics.

An adoption number that is quietly wrong is worse than no number - it gets
quoted in a steering pack and then defended. These tests pin the definitions to
hand-computable examples so a refactor cannot silently change what "reach" means.
"""

import pandas as pd
import pytest

from retail_pipeline.adoption import (
    _sessions,
    headline_metrics,
    load_events,
    team_metrics,
    weekly_metrics,
)


def test_sessions_split_on_the_gap_not_on_the_day(events):
    s = _sessions(events)
    # U1's 14:00 event is 4 hours after the 10:12 one -> a second session.
    assert s[s["user_id"] == "U1"]["session_key"].nunique() == 2
    assert s[s["user_id"] == "U2"]["session_key"].nunique() == 1


def test_session_keys_do_not_collide_across_users(events):
    """The bug this guards: session ids restart at 0 per user, so counting
    distinct ids without the user prefix merges everyone's first session."""
    s = _sessions(events)
    assert s["session_key"].nunique() == 3          # 2 for U1 + 1 for U2
    assert s.groupby("user_id")["session_key"].nunique().sum() == 3


def test_weekly_metrics_match_a_hand_count(events, cfg):
    cfg.adoption["licensed_users"] = 10
    w = weekly_metrics(events, cfg)
    assert len(w) == 1
    row = w.iloc[0]
    assert row["active_users"] == 2
    assert row["sessions"] == 3
    assert row["views"] == 4
    assert row["applies"] == 1
    assert row["reach_pct"] == 20.0                  # 2 of 10 licensed
    assert row["action_rate_pct"] == 25.0            # 1 apply / 4 views
    assert row["sessions_per_active_user"] == 1.5    # 3 sessions / 2 users


def test_reach_counts_licensed_users_not_users_seen(events, cfg):
    """A team whose members never open the report must drag its reach down -
    dividing by "users who showed up" would always report 100%."""
    by_team = team_metrics(events, cfg, {"Team A": 1, "Team B": 4})
    b = by_team.set_index("team")
    assert b.loc["Team A", "reach_pct"] == 100.0
    assert b.loc["Team B", "reach_pct"] == 25.0      # 1 active of 4 licensed


def test_activation_only_counts_users_who_acted(events, cfg):
    by_team = team_metrics(events, cfg, {"Team A": 1, "Team B": 1}).set_index("team")
    assert by_team.loc["Team A", "activated_users"] == 1   # U1 applied
    assert by_team.loc["Team B", "activated_users"] == 0   # U2 only viewed


def test_csat_is_null_not_zero_when_nobody_responded(events, cfg):
    """A team with no feedback has an unknown score, not a bad one."""
    by_team = team_metrics(events, cfg, {"Team A": 1, "Team B": 1}).set_index("team")
    assert pd.isna(by_team.loc["Team A", "csat"])
    assert by_team.loc["Team A", "csat_responses"] == 0
    assert by_team.loc["Team B", "csat"] == 5.0


def test_headline_targets_and_status(events, cfg):
    cfg.adoption["licensed_users"] = 10
    cfg.adoption["target_action_rate_pct"] = 50   # 25% actual -> below
    cfg.adoption["target_reach_pct"] = 10         # 20% actual -> on track
    h = headline_metrics(events, weekly_metrics(events, cfg), cfg).set_index("metric")

    assert h.loc["reach_pct", "status"] == "on track"
    assert h.loc["action_rate_pct", "status"] == "below target"
    # Metrics with no target must not be reported as failing.
    assert h.loc["stickiness_pct", "status"] == "no target"
    assert h.loc["activation_pct", "status"] == "no target"


def test_missing_event_file_fails_with_a_useful_message(cfg, tmp_path):
    cfg.paths["usage_events"] = tmp_path / "nope.csv"
    with pytest.raises(FileNotFoundError, match="simulate_usage"):
        load_events(cfg)
