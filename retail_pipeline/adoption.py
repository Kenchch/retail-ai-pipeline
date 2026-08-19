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
from pathlib import Path

import pandas as pd

log = logging.getLogger("pipeline")


def load_events(cfg: dict) -> pd.DataFrame:
    path = cfg["paths"]["usage_events"]
    if not path.exists():
        raise FileNotFoundError(f"{path} not found - run `python scripts/get_data.py`.")
    events = pd.read_csv(path, parse_dates=["event_ts"])

    # Two checks the metrics used to take on trust. Both are cheap and both
    # would otherwise surface as a plausible-looking number.
    scale = cfg["adoption"].get("csat_scale", [1, 5])
    fb = events.loc[events["event_type"] == "feedback", "feedback_score"].dropna()
    bad = fb[(fb < scale[0]) | (fb > scale[1])]
    if len(bad):
        raise ValueError(
            f"{len(bad)} feedback score(s) outside {scale[0]}-{scale[1]}: "
            f"{sorted(bad.unique())[:5]}. CSAT is a mean, so one out-of-range "
            f"value moves the headline without ever looking wrong."
        )
    blank = events["team"].isna() | (events["team"].astype(str).str.strip() == "")
    if blank.any():
        raise ValueError(
            f"{int(blank.sum())} event(s) have no team. Every metric is also cut "
            f"by team, so these would count in the totals and in no team's row - "
            f"the parts would silently stop summing to the whole."
        )
    return events


def resolve_as_of(events: pd.DataFrame, cfg: dict) -> pd.Timestamp | None:
    """The date every window is measured back from.

    `adoption.analysis_as_of` in config.yaml when set, otherwise the last event
    in the log. The default is the honest one for a fixed simulated extract, but
    it is a trap on a live feed: "active in the last four weeks" measured back
    from the newest event slides backwards with the data, so a telemetry feed
    that died a month ago still reports full reach. Making it explicit is what
    lets the report state the date it is speaking about, rather than implying
    today.
    """
    configured = cfg["adoption"].get("analysis_as_of")
    if configured:
        ts = pd.Timestamp(configured)
        # A date carries no time, so pd.Timestamp gives midnight - and "as of
        # 28 June" would then exclude everything that happened on 28 June.
        # Anyone writing a date in config means the end of that day.
        return (
            ts + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
            if ts == ts.normalize()
            else ts
        )
    return events["event_ts"].max() if len(events) else None


def eligible_events(events: pd.DataFrame, as_of: pd.Timestamp | None) -> pd.DataFrame:
    """Everything at or before the analysis date, and nothing after it.

    `as_of` used to bound only the four-week reach window, so activation, action
    rate, CSAT, the weekly trend and every team row still read the whole log.
    With as_of pinned to 7 April, injecting two events dated 20 May moved action
    rate 50 -> 100, CSAT from no-data to 1.0, activation 50 -> 100 and the
    weekly table from 1 row to 7. A report headed "four weeks ending 7 April"
    was describing May.

    Applied once, at the top of every metric function, so a caller cannot get a
    half-filtered answer.
    """
    if as_of is None or events.empty:
        return events
    return events.loc[events["event_ts"] <= as_of]


def effective_roster(events: pd.DataFrame, cfg: dict) -> dict[str, int]:
    """The configured roster, repaired with any team that is using the solution
    but was never licensed for it.

    This repair used to live inside team_metrics(), on a local copy that was
    never handed back. So the by-team table divided by a repaired denominator
    while headline_metrics() divided by the configured one - same numerator,
    two different denominators - and headline was computed first, before the
    "not on the roster" warning was even emitted. Measured on the real telemetry
    with one genuinely unlisted team appended: headline reported reach 82.30%
    against a true 75.0%; with a 20-person unlisted team all active it reported
    `reach_pct 106.50 ... on track`.

    One function, called once, is what keeps the two tables answering the same
    question. The repaired size is the count of distinct users observed, which
    is a lower bound on that team's real headcount, so reach for an unlicensed
    team stays optimistic - unavoidable without a real roster, and still far
    better than the >100% that dropping them produced.
    """
    roster = dict(cfg["adoption"]["roster"])
    if not len(events):
        return roster
    unlisted = sorted(set(events["team"].dropna()) - set(roster))
    if unlisted:  # using it but never licensed = the rollout list is stale
        log.warning("Active but not on the roster: %s", ", ".join(unlisted))
        for team in unlisted:
            roster[team] = int(events[events["team"] == team]["user_id"].nunique())
    return roster


def weekly_metrics(
    events: pd.DataFrame, cfg: dict, as_of: pd.Timestamp | None = None
) -> pd.DataFrame:
    """Weekly trend over a CONTINUOUS calendar.

    A week nobody used the solution produces no events, so grouping the log by
    week drops that week - the trend closes over the gap and every later week
    shifts one position left. Zero activity is a fact worth plotting.
    """
    events = eligible_events(events, as_of)
    # effective_roster, not cfg["licensed_users"]. headline and team were moved
    # onto the repaired roster and this was left behind, so the moment a team
    # appeared in the log without being on the roster, weekly reach was computed
    # against a smaller denominator than the headline reach beside it.
    licensed = sum(effective_roster(events, cfg).values())
    if events.empty:
        return pd.DataFrame(
            columns=[
                "week_no",
                "week_start",
                "active_users",
                "reach_pct",
                "views",
                "applies",
                "action_rate_pct",
            ]
        )

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
    weekly = (
        weekly.reindex(calendar, fill_value=0).rename_axis("week_start").reset_index()
    )

    weekly.insert(0, "week_no", range(1, len(weekly) + 1))
    weekly["reach_pct"] = (100 * weekly["active_users"] / licensed).round(1)
    # No views means the action rate is UNDEFINED, not zero - there was nothing
    # to act on. A fabricated 0% reads as "people ignored it".
    views = weekly["views"].astype(float)
    weekly["action_rate_pct"] = (
        100 * weekly["applies"] / views.where(views > 0)
    ).round(1)
    return weekly


def team_metrics(
    events: pd.DataFrame, cfg: dict, as_of: pd.Timestamp | None = None
) -> pd.DataFrame:
    """One row per team on the ROSTER, including teams with no events at all -
    a team that never opened the report contributes no rows to the log, so any
    team list derived from the log omits exactly the team worth surfacing."""
    as_of = as_of if as_of is not None else resolve_as_of(events, cfg)
    events = eligible_events(events, as_of)
    roster = effective_roster(events, cfg)
    if len(events):
        window = as_of - pd.Timedelta(weeks=cfg["adoption"]["active_window_weeks"])
        # Bounded at both ends, like headline. With as_of pinned in config an
        # event after it is a late arrival for a period already reported on, and
        # counting it here but not there would split the two tables apart again.
        recent = events[(events["event_ts"] >= window) & (events["event_ts"] <= as_of)]
    else:
        recent = events

    rows = []
    for team, size in roster.items():
        allt = events[events["team"] == team] if len(events) else events
        views = int((allt["event_type"] == "view").sum()) if len(allt) else 0
        applies = int((allt["event_type"] == "apply").sum()) if len(allt) else 0
        active = (
            recent[recent["team"] == team]["user_id"].nunique() if len(recent) else 0
        )
        acted = (
            allt[allt["event_type"] == "apply"]["user_id"].nunique() if len(allt) else 0
        )
        fb = (
            allt[allt["event_type"] == "feedback"]["feedback_score"].dropna()
            if len(allt)
            else pd.Series(dtype=float)
        )
        rows.append(
            {
                "team": team,
                "licensed_users": size,
                "active_users": active,
                "reach_pct": round(100 * active / size, 1),
                "activation_pct": round(100 * acted / size, 1),
                "views": views,
                "applies": applies,
                "action_rate_pct": round(100 * applies / views, 1) if views else None,
                "csat": round(float(fb.mean()), 2) if len(fb) else None,
                "csat_responses": len(fb),
            }
        )
    return (
        pd.DataFrame(rows)
        .sort_values("reach_pct", ascending=False)
        .reset_index(drop=True)
    )


def headline_metrics(
    events: pd.DataFrame, cfg: dict, as_of: pd.Timestamp | None = None
) -> pd.DataFrame:
    a = cfg["adoption"]
    as_of = as_of if as_of is not None else resolve_as_of(events, cfg)
    events = eligible_events(events, as_of)
    # The SAME denominator team_metrics uses. Reading a["licensed_users"] here
    # meant the numerator counted every user in the log while the denominator
    # counted only the configured roster.
    licensed = sum(effective_roster(events, cfg).values())
    if len(events):
        end = as_of
        recent = events[
            (events["event_ts"] >= end - pd.Timedelta(weeks=a["active_window_weeks"]))
            & (events["event_ts"] <= end)
        ]
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
        (
            "action_rate_pct",
            round(100 * applies / views, 1) if views else None,
            a["target_action_rate_pct"],
        ),
        ("csat", round(float(fb.mean()), 2) if len(fb) else None, a["target_csat"]),
    ]
    out = pd.DataFrame(rows, columns=["metric", "value", "target"])
    out["status"] = [
        "no target"
        if pd.isna(t)
        else "no data"
        if v is None or pd.isna(v)
        else ("on track" if v >= t else "below target")
        for v, t in zip(out["value"], out["target"], strict=True)
    ]
    for r in out.itertuples():
        log.info(
            "  %-16s %7s  target %-5s  %s",
            r.metric,
            "-" if pd.isna(r.value) else r.value,
            "-" if pd.isna(r.target) else r.target,
            r.status,
        )
    return out


def write_report(
    headline: pd.DataFrame,
    weekly: pd.DataFrame,
    teams: pd.DataFrame,
    cfg: dict,
    as_of: pd.Timestamp | None = None,
    dest: Path | None = None,
) -> None:
    def cell(v):
        return "&ndash;" if v is None or pd.isna(v) else v

    # Derived from the by-team table, so the stated denominator is the one the
    # metrics were actually divided by rather than the configured roster.
    # The report says which day it is speaking about. Without it "active in the
    # last 4 weeks" silently means "the 4 weeks before whatever the newest event
    # happens to be", which is not a date a reader can check against anything.
    stamp = "" if as_of is None else f" &middot; four weeks ending {as_of:%d %b %Y}"
    source = (
        "Simulated telemetry from `scripts/get_data.py` - this has not been "
        "deployed to real users. Schema and metrics are the production ones."
    )
    lines = [
        "# Adoption report",
        "",
        (
            f"{int(teams['licensed_users'].sum())} licensed users across "
            f"{len(teams)} teams{stamp}."
        ),
        "",
        f"_{source}_",
        "",
        "| Metric | Value | Target | Status |",
        "|---|---|---|---|",
    ]
    lines += [
        f"| {r.metric} | {cell(r.value)} | {cell(r.target)} | {r.status} |"
        for r in headline.itertuples()
    ]
    lines += [
        "",
        "## By team",
        "",
        "| Team | Licensed | Active (4w) | Reach | Activation | Action rate | CSAT | Responses |",
        "|---|---|---|---|---|---|---|---|",
    ]
    lines += [
        f"| {r.team} | {r.licensed_users} | {r.active_users} | {r.reach_pct}% "
        f"| {r.activation_pct}% | {cell(r.action_rate_pct)}% | {cell(r.csat)} "
        f"| {r.csat_responses} |"
        for r in teams.itertuples()
    ]
    lines += [
        "",
        "## Weekly",
        "",
        "| Week | Active | Reach | Views | Applies | Action rate |",
        "|---|---|---|---|---|---|",
    ]
    lines += [
        f"| {r.week_no} | {r.active_users} | {r.reach_pct}% | {r.views} "
        f"| {r.applies} | {cell(r.action_rate_pct)}% |"
        for r in weekly.itertuples()
    ]
    lines += [
        "",
        (
            "Definitions are in `docs/03_adoption_and_comms.md`. A metric with no data "
            "shows &ndash;, never 0 - the two mean different things."
        ),
    ]
    out_dir = dest or cfg["paths"]["reports"]
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "adoption_report.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def measure_adoption(
    cfg: dict, reports_dest: Path | None = None
) -> dict[str, pd.DataFrame]:
    events = load_events(cfg)
    # Resolved once and shared, so the three tables cannot disagree about which
    # day "the last four weeks" ends on.
    as_of = resolve_as_of(events, cfg)
    headline = headline_metrics(events, cfg, as_of)
    weekly = weekly_metrics(events, cfg, as_of)
    teams = team_metrics(events, cfg, as_of)
    write_report(headline, weekly, teams, cfg, as_of, dest=reports_dest)
    return {
        "adoption_headline": headline,
        "adoption_weekly": weekly,
        "adoption_by_team": teams,
    }
