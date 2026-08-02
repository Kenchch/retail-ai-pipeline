# Frequently Bought Together — user guide

**For:** category managers, merchandisers, online trading, marketing
**Reading time:** 6 minutes
**You need:** access to the Merchandising report. Ask the AI Engineering team if you don't have it.

---

## What this is

For any product you look up, this shows you **the products most often bought in
the same basket**, ranked by how much stronger that pairing is than chance.

It is built from 522,566 real transaction lines across 19,773 baskets. It
refreshes overnight, so what you see each morning includes yesterday's trading.

## What this is not

It tells you **what customers did**. It does not tell you what they will do, and
it does not tell you why. Two products appearing together can mean they are
genuinely complementary, or that they sat next to each other on a page, or that
they were in the same promotion last month. You still bring the judgement — this
just makes sure you are judging the right candidates.

---

## Reading a recommendation

Every row shows four things. Here is a real one:

| Product | Recommended | Baskets | Confidence | Lift |
|---|---|---|---|---|
| CHILDS GARDEN SPADE PINK | CHILDS GARDEN SPADE BLUE | 40 | 0.85 | 234 |

**Baskets — 40.** How many actual baskets contained both. This is your sample
size. A lift of 300 built on 8 baskets is noise; the same lift on 200 baskets is
a finding. **Always look at this number first.**

**Confidence — 0.85.** Of the baskets containing the pink spade, 85% also
contained the blue one. Read it as "when someone buys A, how often do they also
buy B".

**Lift — 234.** The pairing is 234× more likely than if the two products were
unrelated. This is the ranking column.

### How to read lift

| Lift | Means |
|---|---|
| Around 1 | No relationship — they co-occur about as often as chance would predict |
| 2–5 | A real but mild association |
| 10+ | A strong association — usually a genuine complement or a colour/size variant |
| 100+ | Almost always a variant pair, a set, or two halves of the same purchase occasion |

Confidence on its own is misleading, which is why it is not the ranking column:
a product that appears in a third of all baskets will show high confidence
against everything. Lift corrects for that by asking "compared to how often this
product shows up anyway".

---

## The `method` column — read this one

Two different signals feed the list, and they are not equally strong.

**`co_purchase`** — built from what people actually bought together. This is the
real signal. Trust it in proportion to the basket count.

**`content_tfidf`** — built from the *product description text*, not from
behaviour. You will see this on newer or slow-moving lines that have not yet
appeared in enough baskets to produce a reliable pairing. It is a reasonable
starting point ("these two products are described similarly") but it is **not
evidence that anyone ever bought them together**.

About 30% of the catalogue has behavioural recommendations; the rest is
description similarity. Check the column before you build a plan on a row.

---

## Three things it will get wrong

Knowing the failure modes is what separates using this well from using it badly.

**1. It will recommend the free gift.** If a promotional item went in every
basket for a fortnight, it will show up as a strong pairing with everything from
that period. Sense-check against what was running.

**2. It cannot see the reason.** Bunting and paper plates co-occur because both
are party purchases, not because bunting causes plate demand. Placing them
together is sensible; concluding that discounting bunting will lift plate sales
is not.

**3. It is blind to what you did not stock.** If a product was out of stock for
six weeks, its pairings are built from the weeks it was available. Long
out-of-stock periods make a product look less connected than it is.

---

## How to use it — four things it is good for

**Adjacency and planogram decisions.** Filter to your category, sort by lift,
look for pairs sitting in different aisles. That gap is the opportunity.

**Bundle candidates.** High confidence *and* high basket count means people are
already buying the bundle themselves. You are formalising an existing behaviour,
which is a much easier sell than creating a new one.

**Online "customers also bought".** The top 3 by lift, filtered to in-stock
lines.

**Long-tail discovery.** Pick a slow-moving line and see what it pairs with.
This is where the tool earns its keep — nobody has the head-space to do this
manually across 3,800 products.

---

## When something looks wrong

Use the **feedback button** in the report. It takes two clicks and a score out of
5. We read every one — the last round of feedback is what drove the filtering
change in the June release.

If a recommendation looks actively wrong, say so, and include the two product
codes. That is more useful to us than a low score with no comment.

**Questions, access, or a walkthrough:** contact the AI Engineering team's
business engagement contact — see `04_faq_and_support.md`. There is no such thing
as a question too basic; if something in this guide is unclear, that is our
problem to fix.
