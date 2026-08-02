"""Stage 6 - Adoption measurement.

A pipeline that nobody uses has delivered nothing, so the solution's usage is
measured with the same rigour as its data. This module reads the usage event log
produced by the deployed report, computes a fixed set of adoption metrics, and
writes them back into the warehouse alongside the sales tables so they can be
reported on with the same tools.

Metric definitions are deliberately written down once, here and in
docs/06_adoption_and_comms.md, because the fastest way to lose trust in an
adoption number is to have two people compute "active users" differently.

    reach          distinct users with >=1 event in the last `active_window_weeks`
                   / licensed users. "Did it get into people's hands?"
    activation     users who have ever recorded an `apply` / licensed users.
                   "Did it change what anyone did?" - the metric that matters.
    stickiness     weekly active / monthly active. "Is it a habit or a novelty?"
    action rate    apply events / view events. Per-look usefulness; independent
                   of how many people are looking, so it survives a rollout.
    CSAT           mean of 1-5 in-report feedback scores.
    depth          sessions per active user per week.

Every metric is also cut by team, because a healthy overall number routinely
hides one function that never came along - and that team is the next piece of
work, which is exactly what this measurement is for.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .config import Config, get_logger

log = get_logger(__name__)

SESSION_GAP = pd.Timedelta(minutes=30)  # events >30 min apart start a new session


def load_events(cfg: Config, source: Path | None = None) -> pd.DataFrame:
    path = source or cfg.paths["usage_events"]
    if not Path(path).exists():
        raise FileNotFoundError(
            f"Usage events not found: {path}\n"
            "Run `python scripts/simulate_usage.py` first (see that file for what "
            "the production source would be)."
        )
    df = pd.read_csv(path, parse_dates=["event_ts"])
    df["week_start"] = df["event_ts"].dt.to_period("W-SUN").dt.start_time
    log.info(
        "Usage events: %s rows | %s users | %s teams | %s -> %s",
        f"{len(df):,}", df["user_id"].nunique(), df["team"].nunique(),
        df["event_ts"].min().date(), df["event_ts"].max().date(),
    )
    return df


def _sessions(df: pd.DataFrame) -> pd.DataFrame:
    """Group each user's events into sessions - a burst of activity with no
    gap longer than SESSION_GAP. Raw event counts overstate engagement because
    one sitting produces a dozen views."""
    d = df.sort_values(["user_id", "event_ts"]).copy()
    gap = d.groupby("user_id")["event_ts"].diff() > SESSION_GAP
    # Session ids restart per user, so pair them with the user to get a key that
    # is unique across the whole frame - otherwise counting distinct sessions in
    # a week silently merges different people's first sessions into one.
    d["session_key"] = d["user_id"] + "#" + gap.groupby(d["user_id"]).cumsum().astype(str)
    return d


def weekly_metrics(events: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """Weekly trend over a CONTINUOUS calendar.

    A week in which nobody touched the solution produces no events, so grouping
    the event log by week silently drops that week - the trend line closes over
    the gap and every later week's number shifts one position left. A week of
    zero activity is a fact worth plotting, not an absence.
    """
    licensed = cfg.adoption["licensed_users"]
    if events.empty:
        return pd.DataFrame(
            columns=["week_no", "week_start", "active_users", "reach_pct", "sessions",
                     "sessions_per_active_user", "views", "applies", "action_rate_pct"]
        )
    sessions = _sessions(events)

    weekly = sessions.groupby("week_start").agg(
        active_users=("user_id", "nunique"),
        sessions=("session_key", "nunique"),
        views=("event_type", lambda s: int((s == "view").sum())),
        applies=("event_type", lambda s: int((s == "apply").sum())),
    )

    calendar = pd.date_range(weekly.index.min(), weekly.index.max(), freq="7D")
    missing = calendar.difference(weekly.index)
    if len(missing):
        log.warning(
            "%s week(s) with zero activity: %s - reported as 0%%, not skipped",
            len(missing), ", ".join(d.strftime("%d %b") for d in missing),
        )
    weekly = weekly.reindex(calendar, fill_value=0).rename_axis("week_start").reset_index()

    weekly["week_no"] = range(1, len(weekly) + 1)
    weekly["reach_pct"] = (100 * weekly["active_users"] / licensed).round(1)
    # A week with no views has an UNDEFINED action rate, not a zero one - there
    # was nothing to act on. Null keeps it out of the average and leaves a
    # visible gap in the chart instead of a fabricated 0%.
    # `.where()` rather than `.replace(0, pd.NA)`: pd.NA turns the column to
    # object dtype, and Series.round() on object dtype falls back to the builtin
    # round() per element, which pd.NA does not implement. NaN keeps it float.
    views = weekly["views"].astype(float)
    weekly["action_rate_pct"] = (100 * weekly["applies"] / views.where(views > 0)).round(1)
    active = weekly["active_users"].astype(float)
    weekly["sessions_per_active_user"] = (
        weekly["sessions"] / active.where(active > 0)
    ).round(2)
    return weekly[
        ["week_no", "week_start", "active_users", "reach_pct", "sessions",
         "sessions_per_active_user", "views", "applies", "action_rate_pct"]
    ]


def team_metrics(events: pd.DataFrame, cfg: Config, team_sizes: dict[str, int]) -> pd.DataFrame:
    """One row per team on the ROSTER - including teams with no events at all.

    `team_sizes` is the roster, not a count of who showed up. A team that never
    opened the report contributes no rows to the event log, so any team list
    derived from that log omits exactly the team the report exists to surface.
    """
    if not team_sizes:
        return pd.DataFrame(
            columns=["team", "licensed_users", "active_users_last_4w", "reach_pct",
                     "activated_users", "activation_pct", "views", "applies",
                     "action_rate_pct", "csat", "csat_responses"]
        )
    # Anyone using it who is not on the roster is also worth surfacing rather
    # than dropping - it means the rollout list is out of date.
    unlisted = sorted(set(events["team"].dropna()) - set(team_sizes)) if len(events) else []
    if unlisted:
        log.warning("Teams active but not on the roster: %s", ", ".join(unlisted))

    if events.empty:
        recent = events
    else:
        window_end = events["event_ts"].max()
        window_start = window_end - pd.Timedelta(weeks=cfg.adoption["active_window_weeks"])
        recent = events[events["event_ts"] >= window_start]

    team_sizes = dict(team_sizes)
    for t in unlisted:
        team_sizes[t] = int(events[events["team"] == t]["user_id"].nunique())

    rows = []
    for team, size in team_sizes.items():
        t_all = events[events["team"] == team]
        t_recent = recent[recent["team"] == team]
        views = int((t_all["event_type"] == "view").sum())
        applies = int((t_all["event_type"] == "apply").sum())
        activated = t_all[t_all["event_type"] == "apply"]["user_id"].nunique()
        fb = t_all[t_all["event_type"] == "feedback"]["feedback_score"]
        rows.append(
            {
                "team": team,
                "licensed_users": size,
                "active_users_last_4w": t_recent["user_id"].nunique(),
                "reach_pct": round(100 * t_recent["user_id"].nunique() / size, 1),
                "activated_users": activated,
                "activation_pct": round(100 * activated / size, 1),
                "views": views,
                "applies": applies,
                "action_rate_pct": round(100 * applies / views, 1) if views else 0.0,
                "csat": round(float(fb.mean()), 2) if len(fb) else None,
                "csat_responses": int(len(fb)),
            }
        )
    out = pd.DataFrame(rows).sort_values("reach_pct", ascending=False).reset_index(drop=True)
    return out


def headline_metrics(
    events: pd.DataFrame, weekly: pd.DataFrame, cfg: Config
) -> pd.DataFrame:
    """Headline metrics, including for a period in which nothing happened.

    "Nobody used it" is a legitimate - and important - state for this report to
    be in. It must produce zeros and nulls, not an exception.
    """
    licensed = cfg.adoption["licensed_users"]
    if events.empty:
        log.warning("No usage events in the period - reporting zeros, not failing")
    win = cfg.adoption["active_window_weeks"]
    if events.empty:
        recent = last_week = events
    else:
        window_end = events["event_ts"].max()
        recent = events[events["event_ts"] >= window_end - pd.Timedelta(weeks=win)]
        last_week = events[events["event_ts"] >= window_end - pd.Timedelta(weeks=1)]

    views = int((events["event_type"] == "view").sum()) if len(events) else 0
    applies = int((events["event_type"] == "apply").sum()) if len(events) else 0
    fb = (
        events[events["event_type"] == "feedback"]["feedback_score"].dropna()
        if len(events) else pd.Series(dtype=float)
    )
    activated = (
        events[events["event_type"] == "apply"]["user_id"].nunique() if len(events) else 0
    )

    mau = recent["user_id"].nunique()
    wau = last_week["user_id"].nunique()

    rows = [
        ("reach_pct", round(100 * mau / licensed, 1), cfg.adoption["target_reach_pct"],
         f"Distinct users active in the last {win} weeks / {licensed} licensed users"),
        ("activation_pct", round(100 * activated / licensed, 1), None,
         "Users who have acted on at least one recommendation / licensed users"),
        # No views means the action rate is undefined, not zero - there was
        # nothing to act on. Reporting 0% would read as "people ignored it".
        ("action_rate_pct", round(100 * applies / views, 1) if views else None,
         cfg.adoption["target_action_rate_pct"],
         "Recommendations acted on / recommendations viewed"),
        ("csat", round(float(fb.mean()), 2) if len(fb) else None,
         cfg.adoption["target_csat"],
         f"Mean in-report feedback score, 1-5 ({len(fb)} responses)"),
        ("stickiness_pct", round(100 * wau / mau, 1) if mau else 0.0, None,
         "Weekly active / monthly active - habit vs novelty"),
        ("sessions_per_active_user", _last_week_depth(weekly), None,
         "Sessions per active user in the most recent week"),
    ]
    out = pd.DataFrame(rows, columns=["metric", "value", "target", "definition"])
    out["status"] = [
        "no target" if pd.isna(t)
        else "no data" if v is None or pd.isna(v)
        else ("on track" if v >= t else "below target")
        for v, t in zip(out["value"], out["target"])
    ]
    for _, r in out.iterrows():
        log.info("  %-26s %8s  target %-6s %s", r["metric"], r["value"],
                 "-" if pd.isna(r["target"]) else r["target"], r["status"])
    return out


def _last_week_depth(weekly: pd.DataFrame) -> float | None:
    """Depth in the most recent week - None if there is no week to report on."""
    if weekly.empty:
        return None
    value = weekly["sessions_per_active_user"].iloc[-1]
    return None if pd.isna(value) else float(value)


def top_viewed_products(events: pd.DataFrame, dim_product: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """What people actually look at - feeds the 'iteration priorities' conversation."""
    views = events[(events["event_type"] == "view") & events["stock_code"].notna()]
    applies = events[(events["event_type"] == "apply") & events["stock_code"].notna()]
    agg = (
        views.groupby("stock_code").size().rename("views").to_frame()
        .join(applies.groupby("stock_code").size().rename("applies"))
        .fillna({"applies": 0})
        .reset_index()
    )
    agg["applies"] = agg["applies"].astype(int)
    agg["action_rate_pct"] = (100 * agg["applies"] / agg["views"]).round(1)
    agg = agg.merge(dim_product[["stock_code", "description"]], on="stock_code", how="left")
    return agg.sort_values("views", ascending=False).head(n).reset_index(drop=True)


def measure(tables: dict[str, pd.DataFrame], cfg: Config) -> dict[str, pd.DataFrame]:
    events = load_events(cfg)
    # The roster lives in config.yaml and is the source of truth for both the
    # denominator and the team list. Nothing here is derived from who happens
    # to appear in the events.
    team_sizes = cfg.adoption["roster"]

    weekly = weekly_metrics(events, cfg)
    by_team = team_metrics(events, cfg, team_sizes)
    headline = headline_metrics(events, weekly, cfg)
    top = top_viewed_products(events, tables["dim_product"])

    return {
        "adoption_headline": headline,
        "adoption_weekly": weekly,
        "adoption_by_team": by_team,
        "adoption_top_products": top,
    }
