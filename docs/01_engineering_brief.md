# Brief — Frequently Bought Together

**From:** business engagement  ·  **To:** AI Engineering  ·  **Status:** delivered, in adoption

This is the document between "Merchandising asked for something" and the code.
If a requirement below is not testable, that is a defect in this brief.

---

## The problem

Category managers set adjacencies, bundles and "customers also bought" slots
from experience. That works for the top 200 lines and breaks down across a
3,803-line catalogue — 7.2 million pairs are possible (3,803 × 3,802 / 2) and
1.4 million of them actually occur in the same basket at least once, which is
the figure the pipeline counts and logs. Nobody holds either in their head.
Cross-sell decisions concentrate on products people already know sell, and
affinities in the long tail are never found.

## Scope

**In:** nightly refresh of sales facts; automated data-quality checks with a
hard gate; ranked product-to-product recommendations for the full catalogue;
adoption measurement.

**Out, and why — so nobody re-litigates it in a stand-up:**

- *Personalised customer-level recommendations.* 24.9% of transactions have no
  customer id, so a per-customer model would be built on three-quarters of the
  picture and quietly biased toward loyalty-card holders. Basket-level affinity
  has no such gap.
- *Automated pricing or promotion actions.* The output is a ranked suggestion a
  human accepts or rejects. Nothing writes to a trading system.
- *Real-time serving.* A nightly refresh matches how often planograms change.

## Requirements

**R1 — Ingest the transaction feed.** Daily CSV of invoice lines.
*Accepted when:* row count and date range logged per run; source columns mapped
in config, not in code; a renamed or missing source column fails with a message
naming the column, not a `KeyError`.

**R2 — Enforce data quality before anything is published.** Reject
cancellations, non-positive quantities and prices, duplicates and non-product
stock codes. Report but keep missing customer ids and descriptions.
*Accepted when:* every rule reports a count and a percentage; rejected rows are
written to a `quarantine` table with the rules they broke; the run **fails**
above a configured rejection rate; every rule has a unit test.

> The blocking/flagging split is a business decision. Rejecting the 24.9% with
> no customer id would discard a quarter of the basket evidence to satisfy a
> rule only customer analytics cares about.

**R3 — Publish a sales model BI can also use.** Star schema: `fact_sales` +
`dim_product` / `dim_customer` / `dim_date`.
*Accepted when:* dimension keys unique; every fact key resolves; no descriptive
attributes in the fact table; `dim_date` is a continuous calendar (the business
is shut on Saturdays — a dimension built from observed dates would omit 53 of
them and hand BI six-day weeks).

**R4 — Ranked recommendations with the evidence attached.** Pairwise
co-purchase; expose support, confidence and lift; rank by lift; top 5 per
product, both directions.
*Accepted when:* every row carries its basket count; lift is unit-tested against
a hand-worked example; pairs below the minimum basket count are excluded.

> Rank by **lift**, not raw co-occurrence. Ranking by count makes the best
> sellers the recommendation for everything — confidently useless.

**R5 — Cover the long tail.** Products with no qualifying rule fall back to
TF-IDF similarity over the description text.
*Accepted when:* ≥90% of the catalogue carries a recommendation and every row is
labelled with the `method` that produced it, so the two signals are never
conflated in the UI.

**R6 — Schedule nightly with the quality gate in front of the load.**
*Accepted when:* per-stage runtime and failure are visible in the scheduler; a
quality failure leaves the previous night's data untouched.

**R7 — Instrument adoption.** Capture view / apply / feedback events with user,
team and timestamp; compute reach, activation, action rate and CSAT, weekly and
by team.
*Accepted when:* definitions are written down once and referenced by both the
code and the comms; a team with zero adoption appears at 0% rather than
disappearing from the report.

## Non-functional

All thresholds and the team roster in `config.yaml`; no magic numbers in code.
Full refresh inside the overnight window — 12.4 s on the reference run, with
`runtime_seconds` in `reports/run_metrics.json` as the authoritative figure,
written by the run itself. Machine-readable run metrics plus a human-readable
quality report per run, and a sha256 of each raw input so any published number
can be traced back to the file that produced it. For the telemetry the digest
covers the metric-bearing columns rather than the whole file, because
`stock_code` is drawn from the catalogue when one exists and from synthetic
codes when it does not — a difference no metric reads. A provenance check that
fires on a legitimate first run teaches people to ignore it.

## Open questions

1. Rolling 30-day window vs all-history for the co-purchase counts? All-history
   is stable; a window reacts to seasonality. Worth measuring once there is an
   A/B harness to measure against.
2. `min_support_count = 30` is judgement, not evidence. Action-rate per rule
   would turn it into evidence.
3. Product hierarchy in `dim_product` — not required by this release, but every
   Merchandising conversation reaches "show me this by category" within five
   minutes.
