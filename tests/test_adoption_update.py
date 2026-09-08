"""The example monthly update quotes real figures; it has to keep quoting them.

docs/03 Part 3 is a template, and its narrative findings are written -- the
document says so. Its figures are not: every one comes from
reports/adoption_report.md, rounded for prose. That makes it the same hazard as
any other number typed into a document, with the extra trap that a reader who
knows the findings are illustrative may assume the numbers are too.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _update() -> str:
    text = (ROOT / "docs/03_adoption_and_comms.md").read_text(encoding="utf-8")
    start = text.index("## Part 3")
    return text[start : text.index("## Part 4")]


def _report() -> str:
    return (ROOT / "reports/adoption_report.md").read_text(encoding="utf-8")


def _headline(metric: str) -> float:
    match = re.search(rf"^\| {metric} \| ([\d.]+) \|", _report(), re.M)
    assert match, f"{metric} is no longer a row in the adoption report"
    return float(match.group(1))


def _by_team() -> dict[str, list[str]]:
    rows = {}
    for line in _report().splitlines():
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) == 8 and cells[1].isdigit():
            rows[cells[0]] = cells[1:]
    return rows


def _weekly() -> dict[int, str]:
    weeks = {}
    for line in _report().splitlines():
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) == 6 and cells[0].isdigit():
            weeks[int(cells[0])] = cells[2]  # reach
    return weeks


def test_the_four_headline_figures_are_the_reports():
    update = _update()
    assert f"**{_headline('reach_pct'):.0f}% of you used it" in update
    assert f"**{_headline('activation_pct'):.0f}% have acted" in update
    assert f"Feedback sits at {_headline('csat'):.1f} out of 5" in update
    assert f"Action rate is {_headline('action_rate_pct')}%" in update


def test_the_response_count_is_the_sum_of_the_teams():
    """Stated as one number in the update and as five in the report."""
    responses = sum(int(row[-1]) for row in _by_team().values())
    assert f"across {responses} responses" in _update()


def test_the_team_singled_out_is_the_one_the_report_singles_out():
    """The update names Store Ops as lowest on both reach and feedback. If
    another team drops below it, the update is pointing at the wrong one."""
    teams = _by_team()
    lowest_reach = min(teams, key=lambda t: float(teams[t][2].rstrip("%")))
    lowest_csat = min(teams, key=lambda t: float(teams[t][5]))
    assert lowest_reach == lowest_csat == "Store Ops", (
        f"the update names Store Ops, but the report's lowest reach is "
        f"{lowest_reach} and its lowest CSAT is {lowest_csat}"
    )

    reach = float(teams["Store Ops"][2].rstrip("%"))
    csat = float(teams["Store Ops"][5])
    update = _update()
    assert f"Store Ops is at {reach:.0f}% reach and {csat:.1f} on feedback" in update


def test_the_workshop_effect_is_read_off_the_weekly_table():
    """The two before/after pairs are weeks 2->3 and 7->8, because the
    workshops fall in weeks 3 and 8. Quoting a step change is the strongest
    claim in the update, so it is the one most worth pinning to the data.
    """
    weeks = _weekly()
    update = _update()
    for before, after in ((2, 3), (7, 8)):
        pair = (
            f"{float(weeks[before].rstrip('%')):.0f}% → "
            f"{float(weeks[after].rstrip('%')):.0f}%"
        )
        assert pair in update, f"weeks {before}->{after} now read {pair}"

    # And the decay claim: a fortnight after the first workshop is week 5.
    decayed = float(weeks[5].rstrip("%"))
    assert f"decayed to {decayed:.0f}% within a fortnight" in update
