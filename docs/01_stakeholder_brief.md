# Stakeholder brief — Frequently Bought Together

**Audience:** Merchandising Director, Head of Category Management, Data & AI lead
**Status:** delivered, in adoption phase
**Author:** AI Engineering (business engagement)
**Last updated:** 26 June 2026

---

## The problem in one paragraph

Category managers decide product adjacencies, bundle promotions and "customers also
bought" placements largely from experience and last season's plan. That works for
the top 200 lines and breaks down across a 3,800-line catalogue: nobody can hold
1.4 million potential product pairings in their head. The result is that
cross-sell decisions concentrate on products people already know sell well,
and genuine affinities in the long tail are never found.

## What we built

A nightly pipeline that reads the transaction feed, enforces data quality, and
publishes a ranked "frequently bought together" list for every product in the
catalogue, surfaced in the existing merchandising report.

It is **not** a black box. Every recommendation carries the evidence behind it:
how many baskets contained both products, how often one followed the other, and
how much more likely the pairing is than chance.

## Scope

**In scope**

- Nightly refresh of sales facts (products, customers, dates) from the transaction feed
- Automated data quality checks with a hard gate before anything is published
- Ranked product-to-product recommendations for the full catalogue
- Adoption measurement and a monthly report to the steering group

**Explicitly out of scope for this release**

- Personalised, customer-level recommendations. 24.9% of transactions have no
  customer identifier, so a per-customer model would be built on three-quarters
  of the picture and quietly biased toward loyalty-card holders. Basket-level
  affinity has no such gap.
- Automated price or promotion decisions. The output is a ranked suggestion for
  a human to accept or reject; nothing changes in a trading system automatically.
- Real-time / on-site recommendations. The refresh is nightly, which matches how
  often planograms and promotional slots actually change.

## What it cost and what it delivers

| | |
|---|---|
| Build | One engineer, three weeks, on existing infrastructure |
| Run | ~25 seconds per nightly refresh; no new licences |
| Coverage | 3,800 of 3,803 products carry a recommendation (99.9%) |
| Evidence base | 522,566 clean transaction lines across 19,773 baskets |

## How we know it works

Two checks, one technical and one human.

**Technical.** Recommendations are ranked by *lift* — how much more likely two
products appear together than they would by chance — not by raw co-occurrence.
That distinction matters: raw counts simply re-rank the best sellers, so the
"recommendation" for every product would be the same handful of top lines.

**Human.** The strongest pairings the model finds are ones a category manager
would immediately recognise: *Landmark Frame Covent Garden → Landmark Frame Oxford
Street*, *Childs Garden Spade Blue → Childs Garden Spade Pink*, *Kids Rain Mac
Blue → Kids Rain Mac Pink*. A model that surfaces the obvious pairs correctly
earns the right to be believed on the non-obvious ones.

## Success measures

Agreed with the Merchandising Director at kick-off. Measured continuously — see
`06_adoption_and_comms.md` for definitions and the current dashboard.

| Measure | Target | Latest (12 weeks post-launch) |
|---|---|---|
| Reach — licensed users active in the last 4 weeks | 70% | **75.8%** ✅ |
| Action rate — recommendations acted on / viewed | 25% | **22.6%** ⚠️ |
| Feedback score (1–5) | 4.0 | **4.18** ✅ |
| Catalogue coverage | 90% | **99.9%** ✅ |

Deliberately *not* a success measure at this stage: incremental revenue. We
cannot attribute a basket to a recommendation without a controlled test, and
claiming a revenue number we cannot defend would cost more credibility than it
buys. A held-out category A/B test is the first item on the roadmap.

## Risks and how they are handled

| Risk | Mitigation |
|---|---|
| Bad upstream data silently publishes bad recommendations | The pipeline refuses to publish if the share of rejected rows exceeds 30%; rejected rows go to a quarantine table with reasons, not to `/dev/null` |
| Users treat a statistical association as a causal claim | The user guide and the workshop both lead with "this is what people did, not what you should do"; every recommendation shows its basket count |
| Long-tail products get no recommendation and the tool looks incomplete | Products with no behavioural rule fall back to description similarity, flagged with a `method` column so users know which signal they are looking at |
| Adoption stalls after launch novelty | Adoption is measured weekly by team, not just in aggregate; the two teams below target have named owners and scheduled sessions |

## Decisions needed

1. **Approve the A/B test** — one category, four weeks, recommendations on vs off,
   to produce a defensible incremental-revenue number. Needs Merchandising sign-off
   on the holdout category.
2. **Store Ops sponsorship** — reach there is 50% against a 70% target and it is
   the only team below the feedback target (3.43). This is a fit problem, not a
   training problem (see the monthly update); it needs 30 minutes with the Store
   Ops lead to decide whether to adapt the view or descope the team.

## Contacts

| Need | Who |
|---|---|
| Business questions, access, training | AI Engineering — business engagement |
| Data or pipeline issue | Data & AI team, via the service desk queue |
| Escalation | Business Engagement Lead |
