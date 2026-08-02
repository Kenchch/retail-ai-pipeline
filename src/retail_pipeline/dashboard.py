"""Renders the adoption metrics as a self-contained HTML page.

This is the artefact that goes to the Business Engagement Lead and into the
monthly steering pack: four headline tiles against target, the weekly trend,
the per-team breakdown, and the underlying table. One file, no build step, no
server - it can be emailed or dropped on SharePoint and it still works.

Design notes: one series per chart and never two y-scales, so nothing has to be
mentally un-scaled; colour is a single hue because every chart shows magnitude,
not identity; status colour always ships with a word next to it, so the
red/green never carries meaning on its own; and the full table is on the page
because three of the figures sit below 3:1 contrast at small sizes.
"""

from __future__ import annotations

import json

import pandas as pd

from .config import Config, get_logger

log = get_logger(__name__)

_CSS = """
:root { color-scheme: light dark; }
* { box-sizing: border-box; }
body {
  margin: 0; padding: 32px 28px 56px;
  font: 15px/1.55 system-ui, -apple-system, "Segoe UI", sans-serif;
  background: var(--page); color: var(--ink);
}
.viz-root {
  --page:#f9f9f7; --surface:#fcfcfb; --ink:#0b0b0b; --ink-2:#52514e; --muted:#898781;
  --grid:#e1e0d9; --axis:#c3c2b7; --border:rgba(11,11,11,.10);
  --series:#2a78d6; --series-soft:#cde2fb;
  --good:#0ca30c; --warning:#fab219; --critical:#d03b3b;
}
@media (prefers-color-scheme: dark) {
  :root:where(:not([data-theme="light"])) .viz-root {
    --page:#0d0d0d; --surface:#1a1a19; --ink:#ffffff; --ink-2:#c3c2b7; --muted:#898781;
    --grid:#2c2c2a; --axis:#383835; --border:rgba(255,255,255,.10);
    --series:#3987e5; --series-soft:#184f95;
    --good:#0ca30c; --warning:#fab219; --critical:#d03b3b;
  }
}
:root[data-theme="dark"] .viz-root {
  --page:#0d0d0d; --surface:#1a1a19; --ink:#ffffff; --ink-2:#c3c2b7; --muted:#898781;
  --grid:#2c2c2a; --axis:#383835; --border:rgba(255,255,255,.10);
  --series:#3987e5; --series-soft:#184f95;
}
.wrap { max-width: 1080px; margin: 0 auto; }
h1 { font-size: 25px; margin: 0 0 4px; letter-spacing: -.01em; }
.sub { color: var(--ink-2); margin: 0 0 4px; font-size: 14px; }
.prov { color: var(--muted); font-size: 12.5px; margin: 0 0 26px; }
h2 { font-size: 15px; margin: 34px 0 3px; letter-spacing: .01em; }
.note { color: var(--muted); font-size: 12.5px; margin: 0 0 12px; }
.tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px,1fr)); gap: 12px; }
.tile {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: 10px; padding: 15px 16px 14px;
}
.tile .lbl { font-size: 12.5px; color: var(--ink-2); margin-bottom: 7px; }
.tile .val { font-size: 34px; line-height: 1.05; font-weight: 600; letter-spacing: -.02em; }
.tile .val .unit { font-size: 18px; font-weight: 500; color: var(--ink-2); margin-left: 1px; }
.tile .meta { font-size: 12.5px; color: var(--muted); margin-top: 7px; }
.status { display: inline-flex; align-items: center; gap: 5px; font-size: 12.5px; font-weight: 500; }
.status .dot { width: 9px; height: 9px; border-radius: 50%; flex: none; }
.ok   { color: var(--good); }     .ok .dot   { background: var(--good); }
.bad  { color: var(--critical); } .bad .dot  { background: var(--critical); }
.card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: 10px; padding: 14px 16px 8px; position: relative;
}
.grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
@media (max-width: 820px) { .grid2 { grid-template-columns: 1fr; } }
svg { display: block; width: 100%; height: auto; overflow: visible; }
.tick { font-size: 11px; fill: var(--muted); }
.blabel { font-size: 13px; fill: var(--ink-2); }
.dlabel { font-size: 11.5px; fill: var(--ink-2); font-weight: 500; }
table { border-collapse: collapse; width: 100%; font-size: 13.5px;
        background: var(--surface); border: 1px solid var(--border); border-radius: 10px; }
th, td { padding: 9px 12px; text-align: right; border-bottom: 1px solid var(--grid); }
th:first-child, td:first-child { text-align: left; }
th { font-size: 12px; color: var(--ink-2); font-weight: 600; }
tbody tr:last-child td { border-bottom: none; }
td.num { font-variant-numeric: tabular-nums; }
.tip {
  position: fixed; pointer-events: none; opacity: 0; transition: opacity .09s;
  background: var(--surface); color: var(--ink); border: 1px solid var(--border);
  border-radius: 8px; padding: 8px 11px; font-size: 12.5px; line-height: 1.45;
  box-shadow: 0 4px 16px rgba(0,0,0,.16); z-index: 20; white-space: nowrap;
}
.tip b { font-weight: 600; }
.defs { margin-top: 30px; font-size: 12.5px; color: var(--ink-2); }
.defs dt { font-weight: 600; margin-top: 9px; color: var(--ink); }
.defs dd { margin: 1px 0 0; color: var(--ink-2); }
"""

_JS = r"""
const $ = (s, r=document) => r.querySelector(s);
const tip = $('#tip');
function showTip(evt, html) {
  tip.innerHTML = html; tip.style.opacity = 1;
  const pad = 14, w = tip.offsetWidth, h = tip.offsetHeight;
  let x = evt.clientX + pad, y = evt.clientY - h - pad;
  if (x + w > innerWidth - 8) x = evt.clientX - w - pad;
  if (y < 8) y = evt.clientY + pad;
  tip.style.left = x + 'px'; tip.style.top = y + 'px';
}
const hideTip = () => tip.style.opacity = 0;
const svgEl = (n, a={}) => {
  const e = document.createElementNS('http://www.w3.org/2000/svg', n);
  for (const k in a) e.setAttribute(k, a[k]);
  return e;
};

function lineChart(mount, pts, opts) {
  const W = 520, H = 232, m = {t: 16, r: 42, b: 30, l: 38};
  const iw = W - m.l - m.r, ih = H - m.t - m.b;
  const ymax = Math.max(opts.target || 0, ...pts.map(p => p.y)) * 1.18;
  const x = i => m.l + (pts.length === 1 ? iw/2 : iw * i / (pts.length - 1));
  const y = v => m.t + ih - ih * (v / ymax);
  const svg = svgEl('svg', {viewBox: `0 0 ${W} ${H}`, role: 'img',
                            'aria-label': opts.aria});

  // recessive gridlines + y ticks
  const step = ymax > 60 ? 25 : 10;
  for (let v = 0; v <= ymax; v += step) {
    svg.appendChild(svgEl('line', {x1: m.l, x2: W - m.r, y1: y(v), y2: y(v),
      stroke: 'var(--grid)', 'stroke-width': 1}));
    const t = svgEl('text', {x: m.l - 8, y: y(v) + 4, class: 'tick',
      'text-anchor': 'end'}); t.textContent = v; svg.appendChild(t);
  }
  // target reference - dashed, labelled, never colour-alone
  if (opts.target) {
    svg.appendChild(svgEl('line', {x1: m.l, x2: W - m.r, y1: y(opts.target),
      y2: y(opts.target), stroke: 'var(--muted)', 'stroke-width': 1.5,
      'stroke-dasharray': '5 4'}));
    const t = svgEl('text', {x: m.l + 4, y: y(opts.target) - 6, class: 'dlabel'});
    t.textContent = `target ${opts.target}${opts.unit}`;
    svg.appendChild(t);
  }
  // x ticks
  pts.forEach((p, i) => {
    if (i % 2) return;
    const t = svgEl('text', {x: x(i), y: H - 8, class: 'tick', 'text-anchor': 'middle'});
    t.textContent = p.x; svg.appendChild(t);
  });
  // 2px series line
  const d = pts.map((p, i) => `${i ? 'L' : 'M'}${x(i)},${y(p.y)}`).join(' ');
  svg.appendChild(svgEl('path', {d, fill: 'none', stroke: 'var(--series)',
    'stroke-width': 2, 'stroke-linejoin': 'round', 'stroke-linecap': 'round'}));
  // markers with a 2px surface ring so overlaps stay readable
  pts.forEach((p, i) => {
    svg.appendChild(svgEl('circle', {cx: x(i), cy: y(p.y), r: 4.5,
      fill: 'var(--series)', stroke: 'var(--surface)', 'stroke-width': 2}));
    if (p.note) {
      const t = svgEl('text', {x: x(i), y: y(p.y) - 13, class: 'dlabel',
        'text-anchor': 'middle'}); t.textContent = p.note; svg.appendChild(t);
    }
  });
  // direct label on the last point only
  const last = pts[pts.length - 1];
  // sits to the RIGHT of the final marker, in the reserved margin, so it can
  // never land on the line itself
  const lt = svgEl('text', {x: x(pts.length - 1) + 9, y: y(last.y) + 4,
    class: 'dlabel'});
  lt.textContent = last.y.toFixed(1) + opts.unit; svg.appendChild(lt);
  // baseline
  svg.appendChild(svgEl('line', {x1: m.l, x2: W - m.r, y1: m.t + ih, y2: m.t + ih,
    stroke: 'var(--axis)', 'stroke-width': 1}));
  // hit targets, wider than the marks
  pts.forEach((p, i) => {
    const hit = svgEl('rect', {x: x(i) - iw / (pts.length * 2) - 2, y: m.t,
      width: iw / pts.length + 4, height: ih, fill: 'transparent'});
    hit.addEventListener('mousemove', e => showTip(e,
      `<b>${p.full}</b><br>${opts.label}: ${p.y}${opts.unit}${p.extra ? '<br>' + p.extra : ''}`));
    hit.addEventListener('mouseleave', hideTip);
    svg.appendChild(hit);
  });
  mount.appendChild(svg);
}

function barChart(mount, rows, opts) {
  const rowH = 42, m = {t: 12, r: 140, b: 30, l: 176};
  const W = 1040, H = m.t + rows.length * rowH + m.b;
  const iw = W - m.l - m.r;
  const xmax = Math.max(opts.target || 0, ...rows.map(r => r.value)) * 1.1;
  const x = v => iw * (v / xmax);
  const svg = svgEl('svg', {viewBox: `0 0 ${W} ${H}`, role: 'img', 'aria-label': opts.aria});

  if (opts.target) {
    svg.appendChild(svgEl('line', {x1: m.l + x(opts.target), x2: m.l + x(opts.target),
      y1: m.t - 2, y2: m.t + rows.length * rowH, stroke: 'var(--muted)',
      'stroke-width': 1.5, 'stroke-dasharray': '5 4'}));
    const t = svgEl('text', {x: m.l + x(opts.target), y: H - 8, class: 'dlabel',
      'text-anchor': 'middle'}); t.textContent = `target ${opts.target}%`;
    svg.appendChild(t);
  }
  rows.forEach((r, i) => {
    // 2px surface gap between bars, 4px rounded data-end
    const yy = m.t + i * rowH + 7, h = rowH - 16;
    const name = svgEl('text', {x: m.l - 14, y: yy + h / 2 + 4.5, class: 'blabel',
      'text-anchor': 'end'}); name.textContent = r.label; svg.appendChild(name);
    svg.appendChild(svgEl('rect', {x: m.l, y: yy, width: Math.max(x(r.value), 2),
      height: h, rx: 4, fill: 'var(--series)'}));
    const v = svgEl('text', {x: m.l + x(r.value) + 9, y: yy + h / 2 + 4, class: 'dlabel'});
    v.textContent = r.value.toFixed(1) + '%'; svg.appendChild(v);
    const hit = svgEl('rect', {x: m.l, y: m.t + i * rowH, width: iw + 120,
      height: rowH, fill: 'transparent'});
    hit.addEventListener('mousemove', e => showTip(e, `<b>${r.label}</b><br>${r.detail}`));
    hit.addEventListener('mouseleave', hideTip);
    svg.appendChild(hit);
  });
  svg.appendChild(svgEl('line', {x1: m.l, x2: m.l, y1: m.t, y2: m.t + rows.length * rowH,
    stroke: 'var(--axis)', 'stroke-width': 1}));
  mount.appendChild(svg);
}

lineChart($('#c-reach'), DATA.reach, {target: DATA.targets.reach, unit: '%',
  label: 'Reach', aria: 'Weekly reach as a percentage of licensed users'});
lineChart($('#c-action'), DATA.action, {target: DATA.targets.action, unit: '%',
  label: 'Action rate', aria: 'Weekly action rate'});
barChart($('#c-team'), DATA.teams, {target: DATA.targets.reach,
  aria: 'Reach by team over the last four weeks'});
"""


def _tile(label: str, value, unit: str, target, meta: str) -> str:
    if value is None:
        return ""
    ok = target is None or value >= target
    status = (
        f'<span class="status {"ok" if ok else "bad"}"><span class="dot"></span>'
        f'{"On track" if ok else "Below target"}</span>'
        if target is not None else f'<span class="meta">{meta}</span>'
    )
    tgt = f" &middot; target {target}{unit}" if target is not None else ""
    return f"""      <div class="tile">
        <div class="lbl">{label}</div>
        <div class="val">{value}<span class="unit">{unit}</span></div>
        <div class="meta">{status}{tgt}</div>
      </div>
"""


def build(adoption: dict[str, pd.DataFrame], cfg: Config) -> str:
    headline = adoption["adoption_headline"].set_index("metric")
    weekly = adoption["adoption_weekly"]
    teams = adoption["adoption_by_team"]

    def hv(metric: str):
        return headline.loc[metric, "value"] if metric in headline.index else None

    workshops = {3: "workshop", 8: "workshop"}
    data = {
        "targets": {
            "reach": cfg.adoption["target_reach_pct"],
            "action": cfg.adoption["target_action_rate_pct"],
        },
        "reach": [
            {
                "x": f"W{int(r.week_no)}",
                "full": f"Week {int(r.week_no)} ({r.week_start:%d %b})",
                "y": float(r.reach_pct),
                "extra": f"{int(r.active_users)} active users, {int(r.sessions)} sessions",
                "note": workshops.get(int(r.week_no), ""),
            }
            for r in weekly.itertuples()
        ],
        "action": [
            {
                "x": f"W{int(r.week_no)}",
                "full": f"Week {int(r.week_no)} ({r.week_start:%d %b})",
                "y": float(r.action_rate_pct),
                "extra": f"{int(r.applies)} acted on / {int(r.views)} viewed",
                "note": "",
            }
            for r in weekly.itertuples()
        ],
        "teams": [
            {
                "label": r.team,
                "value": float(r.reach_pct),
                "detail": (
                    f"{int(r.active_users_last_4w)} of {int(r.licensed_users)} active"
                    f" &middot; action rate {r.action_rate_pct}%"
                    f" &middot; CSAT {r.csat if pd.notna(r.csat) else 'n/a'}"
                ),
            }
            for r in teams.itertuples()
        ],
    }

    rows = "\n".join(
        f"        <tr><td>{r.team}</td>"
        f"<td class='num'>{int(r.licensed_users)}</td>"
        f"<td class='num'>{int(r.active_users_last_4w)}</td>"
        f"<td class='num'>{r.reach_pct}%</td>"
        f"<td class='num'>{r.activation_pct}%</td>"
        f"<td class='num'>{int(r.views)}</td>"
        f"<td class='num'>{r.action_rate_pct}%</td>"
        f"<td class='num'>{r.csat if pd.notna(r.csat) else '&ndash;'}</td>"
        f"<td class='num'>{int(r.csat_responses)}</td></tr>"
        for r in teams.itertuples()
    )

    period = f"{weekly['week_start'].min():%d %b} &ndash; {weekly['week_start'].max():%d %b %Y}"
    tiles = (
        _tile("Reach (last 4 weeks)", hv("reach_pct"), "%",
              cfg.adoption["target_reach_pct"], "")
        + _tile("Activation", hv("activation_pct"), "%", None,
                "have acted on a recommendation")
        + _tile("Action rate", hv("action_rate_pct"), "%",
                cfg.adoption["target_action_rate_pct"], "")
        + _tile("Feedback score", hv("csat"), "", cfg.adoption["target_csat"], "")
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Product Recommendations &mdash; adoption dashboard</title>
<style>{_CSS}</style>
</head>
<body class="viz-root">
<div class="wrap">
  <h1>Product Recommendations &mdash; adoption</h1>
  <p class="sub">{period} &middot; {cfg.adoption['licensed_users']} licensed users across 5 teams</p>
  <p class="prov">Source: in-report usage telemetry. Definitions at the foot of this page &mdash;
     figures are comparable week to week because the definitions have not changed.</p>

  <div class="tiles">
{tiles}  </div>

  <h2>Weekly reach and action rate</h2>
  <p class="note">Reach answers &ldquo;did it get into people&rsquo;s hands&rdquo;; action rate answers
     &ldquo;was it useful when it did&rdquo;. They are plotted separately, on their own scales &mdash;
     the two move for different reasons and a shared axis would hide that.</p>
  <div class="grid2">
    <div class="card"><div id="c-reach"></div></div>
    <div class="card"><div id="c-action"></div></div>
  </div>

  <h2>Reach by team, last 4 weeks</h2>
  <p class="note">The overall number hides the spread. Store Ops is the gap this quarter&rsquo;s
     engagement work is aimed at.</p>
  <div class="card"><div id="c-team"></div></div>

  <h2>All team metrics</h2>
  <p class="note">The table is the accessible view of the charts above &mdash; same numbers, no colour needed.</p>
  <table>
    <thead><tr>
      <th>Team</th><th>Licensed</th><th>Active 4w</th><th>Reach</th><th>Activation</th>
      <th>Views</th><th>Action rate</th><th>CSAT</th><th>Responses</th>
    </tr></thead>
    <tbody>
{rows}
    </tbody>
  </table>

  <dl class="defs">
    <dt>Reach</dt><dd>Distinct users with at least one event in the last 4 weeks, over licensed users.</dd>
    <dt>Activation</dt><dd>Users who have acted on at least one recommendation, over licensed users. The one that matters &mdash; a view changes nothing.</dd>
    <dt>Action rate</dt><dd>Recommendations acted on over recommendations viewed. Per-look usefulness, so it stays comparable as the user base grows.</dd>
    <dt>CSAT</dt><dd>Mean of 1&ndash;5 in-report feedback scores. Small sample &mdash; read alongside the response count, not on its own.</dd>
  </dl>
</div>
<div class="tip" id="tip"></div>
<script>const DATA = {json.dumps(data)};{_JS}</script>
</body>
</html>
"""
    out = cfg.paths["reports"] / "adoption_dashboard.html"
    out.write_text(html, encoding="utf-8")
    log.info("Adoption dashboard written to %s", out)
    return str(out)
