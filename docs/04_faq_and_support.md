# FAQ, support and change management

**Owner:** AI Engineering — business engagement
**Purpose:** the first point of contact for anything about the recommendations
solution. If a question comes up twice, it belongs in this file.

---

## Support routes

| I need | Go to | Response |
|---|---|---|
| Access to the report | Service desk, "Merchandising report access" | 1 working day |
| A recommendation looks wrong | Feedback button in the report | Reviewed weekly; you get a reply if you leave a comment |
| The data looks stale or the report is broken | Service desk, P3 | Same working day |
| A walkthrough for me or my team | Business engagement contact | Booked within a week |
| A new feature or a different cut of the data | Business engagement contact — it goes on the roadmap, and you'll be told where | Discussed at the monthly review |

**Escalation:** Business Engagement Lead. Escalate if something is blocking
trading decisions, not if it is merely annoying — that distinction keeps
escalation meaningful.

---

## Frequently asked

### "Where does this come from? Is it just guessing?"

No. It counts real baskets. If two products are shown as a pair, that pair
appeared in the number of baskets shown in the Baskets column. Nothing is
generated or inferred by a language model. It is arithmetic over your own
transaction history, and every number on screen can be traced back to it.

### "Why does it recommend something obviously silly?"

Three usual causes, in order of likelihood:

1. **Low basket count.** A pairing built on 30 baskets can be a coincidence.
   Check the Baskets column.
2. **It's a `content_tfidf` row.** That row came from description text, not
   behaviour. See the user guide.
3. **A promotion.** If the two products were in the same offer, they will look
   associated for as long as that period is in the data.

If none of those explain it, report it — that is a genuine finding for us.

### "Can it tell me what a *specific customer* will buy?"

Not in this release, and not by accident. A quarter of transactions have no
customer identifier, so a customer-level model would be built on the
loyalty-card population and quietly generalised to everyone. Basket-level
affinity has no such gap. Personalisation is on the roadmap behind the A/B test.

### "Is the AI going to make pricing or promo decisions?"

No. The output is a ranked suggestion. Nothing writes to a trading system, and
nothing changes unless a person accepts it. That is a deliberate design decision,
not a limitation we are working around.

### "Does it use my data / is anything sent outside the business?"

No. It runs on internal infrastructure over the internal transaction feed. The
usage telemetry behind the adoption dashboard records which report pages were
opened and by whom — the same information the reporting platform already logs —
and is reported at team level, never as individual performance data.

### "Why does my colleague see different numbers?"

Almost always a refresh timing difference — the data refreshes overnight, so a
report left open from yesterday shows yesterday. Refresh and compare. If they
still differ, tell us; that would be a real defect.

### "The recommendations barely changed since last month. Is it broken?"

Probably not. Co-purchase patterns over a full trading history are stable by
design — that is the point. Fast-changing recommendations would mean the model
was chasing noise. New products and seasonal lines are where you should expect
movement.

### "I don't have time for a workshop."

The user guide is a six-minute read and covers 90% of it. The workshop exists for
the other 10% and for the questions people don't like asking in writing. If it is
genuinely not worth an hour of your time, that is fair — but tell us why, because
that is useful information about the product.

---

## Change management

### What changed for whom

| Group | What changes | What we did about it |
|---|---|---|
| Category managers | A new panel in an existing report; no new tool, no new login | Guide + workshop; the panel sits where the sales view already was |
| Merchandising | Adjacency and bundle decisions now start from a candidate list rather than a blank page | Ran the first two planogram reviews alongside the team, using the tool live |
| Online trading | "Customers also bought" slots can be filled from a ranked list | Provided a filtered export |
| Store Ops | Least direct benefit — the view is built around catalogue decisions they don't own | See below |

### Handling the group it doesn't fit

Store Ops sits at 50% reach against a 70% target and is the only team below the
feedback target (3.43 against 4.0, from 7 responses). Reading that as a training
gap and booking another session would be the wrong response — the comments say
the view is organised around catalogue decisions Store Ops does not make.

The honest options are (a) build a store-level cut, (b) descope the team from
the rollout and stop counting them against the target. Both are legitimate;
what is not legitimate is leaving them nominally in scope and reporting a
diluted number every month. This is on the agenda with the Store Ops lead.

### Principles we work to

- **Nobody finds out about a change from the change itself.** Release notes go
  out before the release, not after.
- **Say what it can't do, first.** Every session opens with the limitations. It
  costs nothing and it is the whole reason people believe the rest.
- **Low adoption is a product signal, not a user failing.** If a team isn't
  using it, the first question is what we built, not who needs retraining.
- **No metric without a definition.** Every number in the monthly pack has a
  written definition that has not changed since launch.
