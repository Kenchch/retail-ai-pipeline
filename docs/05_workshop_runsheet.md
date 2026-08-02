# Workshop runsheet — "Frequently Bought Together, and what AI can't do"

**Length:** 60 minutes
**Audience:** 8–15 people, mixed technical confidence, no prerequisites
**Facilitator:** AI Engineering — business engagement
**Deck:** `workshop/ai_literacy_workshop.pptx`
**Run:** twice so far — week 3 and week 8 of the rollout

---

## Objective

By the end, everyone can (1) read a recommendation and say whether to trust it,
(2) name two things it cannot do, and (3) knows how to give feedback. That is
all. A workshop with three objectives that lands beats one with eight that
doesn't.

**Deliberately not an objective:** explaining how the model works internally.
Nobody in the room has to build one.

---

## Runsheet

| Time | Segment | Facilitator does | Room does |
|---|---|---|---|
| 0:00 | **Open with the failure** (5 min) | Put up a genuinely bad recommendation from the live tool. Ask: "would you have shipped this?" | Reacts |
| 0:05 | **Why it happened** (10 min) | Walk back from that failure to the basket-count and promotion causes | Follows |
| 0:15 | **Reading a recommendation** (10 min) | Baskets, confidence, lift — one real row on screen, one number at a time | Asks |
| 0:25 | **Hands-on: judge five rows** (15 min) | Five real recommendations on a handout, mixed quality. Pairs decide trust / don't trust / need more info | Works in pairs |
| 0:40 | **Debrief** (10 min) | Take answers, especially the disagreements. The three rows that split the room are the whole workshop | Argues |
| 0:50 | **Limits and feedback** (7 min) | What it cannot do; where the feedback button is; what happens to feedback | Listens |
| 0:57 | **Close** (3 min) | One ask: use it once this week on a real decision and tell us what happened | Commits |

---

## Facilitator notes

**Open with a failure, not a demo.** Starting with a polished success puts the
room in evaluation mode — they spend the hour deciding whether to believe you.
Starting with a real bad output puts them in diagnosis mode, and answers the
question they are actually holding ("is this thing going to embarrass me?")
in the first five minutes rather than never.

**Do not explain the algorithm.** Nobody needs it. What they need is when to
trust the output, which is a different skill and the one that determines whether
the tool gets used. If someone asks, answer honestly and briefly, then return to
the decision.

**The hands-on segment is the workshop.** Everything before it is setup and
everything after is admin. Protect the 15 minutes; if you are running late, cut
the limits segment and put it in an email — never cut the exercise.

**Let disagreements run.** When half the room trusts a row and half doesn't, that
is exactly the judgement being built. Resist resolving it too quickly; ask each
side what number moved them.

**Say what it can't do, early and unprompted.** The credibility of everything
else depends on it. The two that always land: it cannot tell you *why*, and it
cannot see products you didn't stock.

---

## Hands-on exercise — the five rows

Real rows from the live tool. Answers are for the facilitator.

| # | Pair | Baskets | Conf. | Lift | Verdict |
|---|---|---|---|---|---|
| 1 | Childs Garden Spade Pink → Blue | 40 | 0.85 | 234 | **Trust.** Variant pair, strong on every measure |
| 2 | Landmark Frame Covent Garden → Oxford Street | 30 | 0.77 | 323 | **Trust, with a note.** Highest lift in the catalogue but only 30 baskets — right for a page module, thin for a category-wide decision |
| 3 | A `content_tfidf` row on a new line | 0 | — | — | **Don't trust as evidence.** Nobody has bought these together; the descriptions merely look alike |
| 4 | Silk Fan → Lunch Bag Vintage Doily | 34 | 0.12 | 2.4 | **Need more info.** Real but mild. Fine as a "you might also like"; not a planogram change |
| 5 | Popular seasonal item → almost everything | high | high | ~1 | **Don't trust.** Lift near 1 means chance. This is the row that catches people who read confidence and stop |

Row 5 is the one that matters. Every group so far has had someone trust it on
confidence alone, and that moment teaches more than any slide.

---

## Demo script (3 minutes, if the room wants to see it live)

1. Open the report on a product everyone knows.
2. Point at the Baskets column **before** anything else. "This is the number that
   decides whether the rest of the row means anything."
3. Sort by lift. Show the top pair. Ask the room whether it makes sense.
4. Filter to `method = content_tfidf`. "These have never been bought together.
   They just read alike. Useful as a starting point, not as evidence."
5. Click feedback, submit a score, and say what happens to it. Two clicks. People
   use a feedback channel they have watched someone use.

---

## After the session

- Send the guide and the deck within the day, while it is still live for them.
- Note every question that came up — repeated questions become FAQ entries
  (three of the current entries came from these sessions).
- Watch the adoption dashboard for the following fortnight.

**What actually happened:** weekly reach went 19.4% → 41.9% after the week-3
session and 35.5% → 46.8% after the week-8 session. Both bumps held for the
following weeks rather than decaying, which is the part worth reporting — a
spike that decays means people looked once, and that is not adoption.
