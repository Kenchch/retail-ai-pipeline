# Engineering brief — Frequently Bought Together

**From:** business engagement
**To:** AI Engineering team
**Purpose:** translate the agreed business requirement into something buildable,
with acceptance criteria that can be checked without another meeting.

This is the document that sits between `01_stakeholder_brief.md` (what the
business asked for and why) and the code. If a requirement below is not
testable, that is a defect in this brief, not in the build.

---

## Requirement summary

Publish, for every product in the catalogue, a ranked list of products commonly
bought alongside it, refreshed nightly, with the evidence attached.

## Requirements

### R1 — Ingest and standardise the transaction feed

| | |
|---|---|
| **Business need** | Everything else depends on a trustworthy sales history |
| **Detail** | Daily CSV extract of invoice lines: invoice number, stock code, description, quantity, timestamp, unit price, customer id, country |
| **Acceptance** | Row count and date range logged per run; source columns mapped to internal names in config, not in code; no rows dropped at this stage |

### R2 — Enforce data quality before anything is published

| | |
|---|---|
| **Business need** | Merchandising will stop using the tool the first time it recommends a postage line |
| **Detail** | Reject cancellations (`C`-prefixed invoices), non-positive quantities, non-positive or outlier prices, duplicate line items, and non-product stock codes (`POST`, `BANK CHARGES`, `M`, …). Report — but do **not** reject — missing customer id and missing description |
| **Acceptance** | Every rule reported per run with a failure count and % of source; rejected rows written to a `quarantine` table with a `quarantine_reasons` column naming each rule broken; run **fails** if the rejection rate exceeds a configured ceiling; every rule covered by a unit test with a known expected count |

> The blocking/non-blocking split is a business decision, not a technical one.
> 24.9% of rows have no customer id — rejecting them would throw away a quarter
> of the basket evidence to satisfy a rule that only customer analytics cares
> about. Flag them; keep them.

### R3 — Publish a sales model the BI team can also use

| | |
|---|---|
| **Business need** | Merchandising already asked for product/customer/time sales slicing; building a second, incompatible extract for that would be waste |
| **Detail** | Star schema: `fact_sales` (measures + keys only) joined to `dim_product`, `dim_customer`, `dim_date` |
| **Acceptance** | Dimension keys unique; every fact key resolves in its dimension (tested); no descriptive attributes in the fact table; published as Parquet and to the warehouse with indexes on the join keys |

### R4 — Produce ranked recommendations with evidence

| | |
|---|---|
| **Business need** | A recommendation nobody can interrogate will not be trusted or used |
| **Detail** | Pairwise co-purchase within an invoice; expose support, confidence and lift; rank by lift; store the top 5 per product |
| **Acceptance** | Each row carries basket count, support, confidence and lift; lift computed as confidence ÷ baseline frequency (unit-tested against a hand-worked example); pairs below the configured minimum basket count excluded; rules stored in both directions, since A→B and B→A are different recommendations |

> Rank by **lift**, not raw co-occurrence. Ranking by count makes the top sellers
> the recommendation for everything, which is worse than useless — it is
> confidently useless.

### R5 — Cover the long tail

| | |
|---|---|
| **Business need** | 70% of the catalogue would otherwise show an empty panel, and users generalise from the first empty screen they see |
| **Detail** | For products with no qualifying co-purchase rule, fall back to TF-IDF similarity over the product description text |
| **Acceptance** | ≥90% of catalogue carries at least one recommendation; every row labelled with the `method` that produced it so the two signals are never conflated in the UI |

### R6 — Orchestrate nightly with a quality gate

| | |
|---|---|
| **Business need** | Planograms are reviewed in the morning; data must be there before that, and wrong data must not be |
| **Detail** | Scheduled 04:00 daily; stages as separate tasks with retries; quality gate stops the run before load |
| **Acceptance** | Per-stage runtime and failure visible in the scheduler UI; a failure in the quality gate leaves the previous night's published data untouched; alert on failure |

### R7 — Instrument adoption

| | |
|---|---|
| **Business need** | The steering group will ask "is anyone using it" at the first review, and "I think so" is not an answer |
| **Detail** | Capture view / apply / export / feedback events with user, team and timestamp; compute reach, activation, action rate, stickiness and CSAT; publish weekly and by team |
| **Acceptance** | Metric definitions written down once and referenced by both the code and the comms; metrics land in the warehouse alongside the sales tables; dashboard regenerated by the same pipeline run |

## Non-functional

| | |
|---|---|
| Runtime | Full refresh well inside the overnight window (currently ~25 s) |
| Configuration | All paths, thresholds and rule parameters in `config.yaml`; no magic numbers in code |
| Testability | Every quality rule and the lift calculation unit-tested; tests run without the source data present |
| Observability | Per-run metrics written to a machine-readable file; a human-readable data quality report per run |

## Out of scope — confirm before building

Personalised customer-level models, automated pricing or promotion actions,
real-time serving. Each was considered and deferred; the reasoning is in the
stakeholder brief so nobody re-litigates it in a stand-up.

## Open questions for the team

1. Is a 30-day rolling window better than all-history for the co-purchase counts?
   All-history is more stable; a window reacts to seasonality. Suggest measuring
   both once the A/B test gives us something to measure against.
2. `min_support_count` is set at 30 baskets from judgement, not from evidence.
   Once we have action-rate data per rule we can tune it against actual usefulness.
3. Do we need product hierarchy (department / category) in `dim_product`? Not
   required by this release, but every conversation with Merchandising drifts
   toward "show me this by category" within five minutes.
