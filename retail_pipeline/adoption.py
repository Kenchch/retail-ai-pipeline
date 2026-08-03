"""Stage 5 - is anyone actually using it?

A solution nobody opens has delivered nothing, so usage is measured with the
same rigour as the data. Definitions are written down once - here and in
docs/03_adoption_and_comms.md - because the fastest way to lose trust in an
adoption number is two people computing "active users" differently.

    reach        distinct users active in the last 4 weeks / licensed users
    activation   users who have ever acted on a recommendation / licensed users
    action rate  apply events / view events - usefulness per look
    CSAT         mean of 1-5 in-report feedback scores

**Activation is the one that matters.** A view changes nothing, and reach is
easy to move with an email.

Every metric is also cut by team, because a healthy total routinely hides one
function that never came along - and that team is the next piece of work.
"""

from __future__ import annotations

import logging

import pandas as pd

log = logging.getLogger("pipeline")


def load_events(cfg: dict) -> pd.DataFrame:
    path = cfg["paths"]["usage_events"]
    if not path.exists():
        raise FileNotFoundError(f"{path} not found - run `python scripts/get_data.py`.")
    return pd.read_csv(path, parse_dates=["event_ts"])


def weekly_metrics(events: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Weekly trend over a CONTINUOUS calendar.

    A week nobody used the solution produces no events, so grouping the log by
    week drops that week - the trend closes over the gap and every later week
    shifts one position left. Zero activity is a fact worth plotting.
    """
    licensed = cfg["adoption"]["licensed_users"]
    if events.empty:
        return pd.DataFrame(columns=["week_no", "week_start", "active_users",
                                     "reach_pct", "views", "applies", "action_rate_pct"])

    # Derived here rather than expected from the caller, so this function works
    # on any event frame with a timestamp.
    events = events.assign(
        week_start=events["event_ts"].dt.to_period("W-SUN").dt.start_time
    )
    weekly = events.groupby("week_start").agg(
        active_users=("user_id", "nunique"),
        views=("event_type", lambda s: int((s == "view").sum())),
        applies=("event_type", lambda s: int((s == "apply").sum())),
    )
    calendar = pd.date_range(weekly.index.min(), weekly.index.max(), freq="7D")
    weekly = weekly.reindex(calendar, fill_value=0).rename_axis("week_start").reset_index()

    weekly.insert(0, "week_no", range(1, len(weekly) + 1))
    weekly["reach_pct"] = (100 * weekly["active_users"] / licensed).round(1)
    # No views means the action rate is UNDEFINED, not zero - there was nothing
    # to act on. A fabricated 0% reads as "people ignored it".
    views = weekly["views"].astype(float)
    weekly["action_rate_pct"] = (100 * weekly["applies"] / views.where(views > 0)).round(1)
    return weekly


def team_metrics(events: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """One row per team on the ROSTER, including teams with no events at all -
    a team that never opened the report contributes no rows to the log, so any
    team list derived from the log omits exactly the team worth surfacing."""
    roster = dict(cfg["adoption"]["roster"])
    if len(events):
        unlisted = sorted(set(events["team"].dropna()) - set(roster))
        if unlisted:   # using it but never licensed = the rollout list is stale
            log.warning("Active but not on the roster: %s", ", ".join(unlisted))
            for team in unlisted:
                roster[team] = int(events[events["team"] == team]["user_id"].nunique())
        window = events["event_ts"].max() - pd.Timedelta(
            weeks=cfg["adoption"]["active_window_weeks"])
        recent = events[events["event_ts"] >= window]
    else:
        recent = events

    rows = []
    for team, size in roster.items():
        allt = events[events["team"] == team] if len(events) else events
        views = int((allt["event_type"] == "view").sum()) if len(allt) else 0
        applies = int((allt["event_type"] == "apply").sum()) if len(allt) else 0
        active = recent[recent["team"] == team]["user_id"].nunique() if len(recent) else 0
        acted = allt[allt["event_type"] == "apply"]["user_id"].nunique() if len(allt) else 0
        fb = allt[allt["event_type"] == "feedback"]["feedback_score"].dropna() if len(allt) \
            else pd.Series(dtype=float)
        rows.append({
            "team": team, "licensed_users": size, "active_users": active,
            "reach_pct": round(100 * active / size, 1),
            "activation_pct": round(100 * acted / size, 1),
            "views": views, "applies": applies,
            "action_rate_pct": round(100 * applies / views, 1) if views else None,
            "csat": round(float(fb.mean()), 2) if len(fb) else None,
            "csat_responses": len(fb),
        })
    return pd.DataFrame(rows).sort_values("reach_pct", ascending=False).reset_index(drop=True)


def headline_metrics(events: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    a = cfg["adoption"]
    licensed = a["licensed_users"]
    if len(events):
        end = events["event_ts"].max()
        recent = events[events["event_ts"] >= end - pd.Timedelta(weeks=a["active_window_weeks"])]
        views = int((events["event_type"] == "view").sum())
        applies = int((events["event_type"] == "apply").sum())
        fb = events[events["event_type"] == "feedback"]["feedback_score"].dropna()
        acted = events[events["event_type"] == "apply"]["user_id"].nunique()
        active = recent["user_id"].nunique()
    else:
        log.warning("No usage events - reporting zeros, not failing")
        views = applies = acted = active = 0
        fb = pd.Series(dtype=float)

    rows = [
        ("reach_pct", round(100 * active / licensed, 1), a["target_reach_pct"]),
        ("activation_pct", round(100 * acted / licensed, 1), None),
        ("action_rate_pct", round(100 * applies / views, 1) if views else None,
         a["target_action_rate_pct"]),
        ("csat", round(float(fb.mean()), 2) if len(fb) else None, a["target_csat"]),
    ]
    out = pd.DataFrame(rows, columns=["metric", "value", "target"])
    out["status"] = [
        "no target" if pd.isna(t) else
        "no data" if v is None or pd.isna(v) else
        ("on track" if v >= t else "below target")
        for v, t in zip(out["value"], out["target"])
    ]
    for r in out.itertuples():
        log.info("  %-16s %7s  target %-5s  %s", r.metric,
                 "-" if pd.isna(r.value) else r.value,
                 "-" if pd.isna(r.target) else r.target, r.status)
    return out


def write_report(headline: pd.DataFrame, weekly: pd.DataFrame,
                 teams: pd.DataFrame, cfg: dict) -> None:
    def cell(v):
        return "&ndash;" if v is None or pd.isna(v) else v

    lines = ["# Adoption report", "",
             f"{cfg['adoption']['licensed_users']} licensed users across "
             f"{len(cfg['adoption']['roster'])} teams.", "",
             "| Metric | Value | Target | Status |", "|---|---|---|---|"]
    lines += [f"| {r.metric} | {cell(r.value)} | {cell(r.target)} | {r.status} |"
              for r in headline.itertuples()]
    lines += ["", "## By team", "",
              "| Team | Licensed | Active (4w) | Reach | Activation | Action rate | CSAT | Responses |",
              "|---|---|---|---|---|---|---|---|"]
    lines += [f"| {r.team} | {r.licensed_users} | {r.active_users} | {r.reach_pct}% "
              f"| {r.activation_pct}% | {cell(r.action_rate_pct)}% | {cell(r.csat)} "
              f"| {r.csat_responses} |" for r in teams.itertuples()]
    lines += ["", "## Weekly", "", "| Week | Active | Reach | Views | Applies | Action rate |",
              "|---|---|---|---|---|---|"]
    lines += [f"| {r.week_no} | {r.active_users} | {r.reach_pct}% | {r.views} "
              f"| {r.applies} | {cell(r.action_rate_pct)}% |" for r in weekly.itertuples()]
    lines += ["", "Definitions are in `docs/03_adoption_and_comms.md`. A metric with no data "
              "shows &ndash;, never 0 - the two mean different things."]
    (cfg["paths"]["reports"] / "adoption_report.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def measure_adoption(cfg: dict) -> dict[str, pd.DataFrame]:
    events = load_events(cfg)
    headline = headline_metrics(events, cfg)
    weekly = weekly_metrics(events, cfg)
    teams = team_metrics(events, cfg)
    write_report(headline, weekly, teams, cfg)
    return {"adoption_headline": headline, "adoption_weekly": weekly, "adoption_by_team": teams}
