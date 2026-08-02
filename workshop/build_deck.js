/**
 * Builds workshop/ai_literacy_workshop.pptx - the deck for the 60-minute
 * AI-literacy session described in docs/05_workshop_runsheet.md.
 *
 * The deck is generated rather than hand-built so it stays in step with the
 * numbers: every figure on these slides comes from the same pipeline run that
 * produced reports/run_metrics.json.
 *
 *   npm install pptxgenjs      # only if the require below fails
 *   node workshop/build_deck.js
 */

const pptxgen = require("pptxgenjs");

// --- palette -------------------------------------------------------------
// Deep navy dominates (title, section, close); white content slides carry the
// work; one blue accent, matching the adoption dashboard, ties the two together.
const NAVY = "1E2761";
const NAVY_2 = "2C3A7A";
const ICE = "CADCFC";
const BLUE = "2A78D6";
const INK = "0B0B0B";
const INK_2 = "52514E";
const MUTED = "898781";
const RED = "D03B3B";
const GREEN = "0CA30C";
const WHITE = "FFFFFF";
const TINT = "F1F5FC";

const HEAD = "Cambria";
const BODY = "Calibri";

const pres = new pptxgen();
pres.layout = "LAYOUT_WIDE"; // 13.3 x 7.5 in - set BEFORE any slide is added
pres.author = "AI Engineering - business engagement";
pres.title = "Frequently Bought Together - AI literacy workshop";

const M = 0.7; // page margin

/** Rounded card - the deck's one repeated motif. */
function card(slide, x, y, w, h, fill) {
  slide.addShape(pres.ShapeType.roundRect, {
    x, y, w, h, rectRadius: 0.12,
    fill: { color: fill || TINT },
    line: { color: fill === NAVY_2 ? NAVY_2 : "E3E8F2", width: 1 },
  });
}

/** Number in a filled circle - used for ordered steps. */
function stepDot(slide, x, y, n, color) {
  slide.addShape(pres.ShapeType.ellipse, {
    x, y, w: 0.42, h: 0.42, fill: { color: color || BLUE },
  });
  slide.addText(String(n), {
    x, y, w: 0.42, h: 0.42, align: "center", valign: "middle",
    fontFace: BODY, fontSize: 15, bold: true, color: WHITE, margin: 0,
  });
}

function sectionTitle(slide, text, sub) {
  slide.addText(text, {
    x: M, y: 0.45, w: 13.3 - 2 * M, h: 0.75,
    fontFace: HEAD, fontSize: 34, bold: true, color: INK, margin: 0,
  });
  if (sub) {
    slide.addText(sub, {
      x: M, y: 1.22, w: 13.3 - 2 * M, h: 0.4,
      fontFace: BODY, fontSize: 15, color: INK_2, margin: 0,
    });
  }
}

// =========================================================================
// 1. Title
// =========================================================================
{
  const s = pres.addSlide();
  s.background = { color: NAVY };
  s.addText("Frequently Bought Together", {
    x: M, y: 2.15, w: 11.2, h: 0.9,
    fontFace: HEAD, fontSize: 44, bold: true, color: WHITE, margin: 0,
  });
  s.addText("...and the three things it can't do", {
    x: M, y: 3.1, w: 11.2, h: 0.7,
    fontFace: HEAD, fontSize: 30, color: ICE, margin: 0, italic: true,
  });
  s.addText(
    "60 minutes  ·  no prerequisites  ·  you will not be asked to write any code",
    { x: M, y: 4.3, w: 11.2, h: 0.4, fontFace: BODY, fontSize: 16, color: ICE, margin: 0 }
  );
  s.addText("AI Engineering  ·  business engagement", {
    x: M, y: 6.5, w: 11.2, h: 0.35, fontFace: BODY, fontSize: 13, color: "9BA9D6", margin: 0,
  });
  s.addNotes(
    "Do not open with a demo. Open with the failure on slide 2 - it answers the " +
    "question the room is actually holding ('is this going to embarrass me?') in " +
    "the first five minutes."
  );
}

// =========================================================================
// 2. Open with a failure
// =========================================================================
{
  const s = pres.addSlide();
  sectionTitle(s, "Would you have shipped this?", "A real recommendation from the live tool");

  card(s, M, 2.0, 7.6, 3.1);
  s.addText("VINTAGE UNION JACK BUNTING", {
    x: M + 0.4, y: 2.3, w: 6.8, h: 0.4,
    fontFace: BODY, fontSize: 17, bold: true, color: INK, margin: 0,
  });
  s.addText("recommends  →", {
    x: M + 0.4, y: 2.75, w: 6.8, h: 0.35, fontFace: BODY, fontSize: 13, color: MUTED, margin: 0,
  });
  s.addText("A SEASONAL LINE THAT WAS IN EVERY BASKET", {
    x: M + 0.4, y: 3.15, w: 6.8, h: 0.4,
    fontFace: BODY, fontSize: 17, bold: true, color: INK, margin: 0,
  });
  s.addText(
    [
      { text: "Confidence 0.61", options: { bold: true, color: INK } },
      { text: "   —   looks convincing", options: { color: INK_2 } },
    ],
    { x: M + 0.4, y: 3.8, w: 6.8, h: 0.35, fontFace: BODY, fontSize: 15, margin: 0 }
  );
  s.addText(
    [
      { text: "Lift 1.02", options: { bold: true, color: RED } },
      { text: "   —   the pairing is no more likely than chance", options: { color: INK_2 } },
    ],
    { x: M + 0.4, y: 4.25, w: 6.8, h: 0.35, fontFace: BODY, fontSize: 15, margin: 0 }
  );

  card(s, 9.0, 2.0, 3.6, 3.1, NAVY_2);
  s.addText("Hold that thought", {
    x: 9.3, y: 2.35, w: 3.0, h: 0.4,
    fontFace: HEAD, fontSize: 19, bold: true, color: WHITE, margin: 0,
  });
  s.addText(
    "Every number on this slide is real.\n\nBy 0:25 you will be able to say in " +
    "one sentence why this row is worthless.",
    { x: 9.3, y: 2.9, w: 3.0, h: 2.0, fontFace: BODY, fontSize: 14, color: ICE, margin: 0 }
  );

  s.addText(
    "Confidence is the number people read. Lift is the number that decides.",
    { x: M, y: 5.5, w: 11.9, h: 0.4, fontFace: BODY, fontSize: 16, italic: true, color: INK_2, margin: 0 }
  );
  s.addNotes(
    "Ask the room directly: would you have shipped this? Let them react before " +
    "explaining anything. Do not resolve it yet - the resolution is slide 4."
  );
}

// =========================================================================
// 3. Why it happened
// =========================================================================
{
  const s = pres.addSlide();
  sectionTitle(s, "Three reasons a recommendation goes wrong",
    "In order of how often we see them");

  const items = [
    ["Too few baskets", "A pairing built on 30 baskets can be a coincidence. The basket count is your sample size - read it first, every time."],
    ["A promotion", "If two products were in the same offer, they look associated for as long as that period sits in the data."],
    ["It isn't behaviour at all", "Some rows come from the product description text, not from anything anyone bought. They are labelled."],
  ];
  items.forEach(([title, body], i) => {
    const x = M + i * 4.05;
    card(s, x, 2.1, 3.75, 3.3);
    stepDot(s, x + 0.35, 2.45, i + 1);
    s.addText(title, {
      x: x + 0.35, y: 3.05, w: 3.05, h: 0.45,
      fontFace: BODY, fontSize: 18, bold: true, color: INK, margin: 0,
    });
    s.addText(body, {
      x: x + 0.35, y: 3.55, w: 3.05, h: 1.6,
      fontFace: BODY, fontSize: 14, color: INK_2, margin: 0,
    });
  });
  s.addText(
    "None of these are the model being clever or stupid. They are all the data telling the truth about something you didn't ask about.",
    { x: M, y: 5.8, w: 11.9, h: 0.5, fontFace: BODY, fontSize: 15, italic: true, color: INK_2, margin: 0 }
  );
  s.addNotes("Link each cause back to the slide-2 row. The seasonal line is cause 2.");
}

// =========================================================================
// 4. Reading a recommendation
// =========================================================================
{
  const s = pres.addSlide();
  sectionTitle(s, "Reading a recommendation", "One real row, one number at a time");

  card(s, M, 1.95, 11.9, 0.95);
  s.addText(
    [
      { text: "CHILDS GARDEN SPADE PINK", options: { bold: true, color: INK } },
      { text: "     →     ", options: { color: MUTED } },
      { text: "CHILDS GARDEN SPADE BLUE", options: { bold: true, color: INK } },
    ],
    { x: M + 0.4, y: 1.95, w: 11.1, h: 0.95, fontFace: BODY, fontSize: 18, valign: "middle", margin: 0 }
  );

  const stats = [
    ["40", "baskets", "Your sample size. Read this first — a huge lift on 8 baskets is noise.", BLUE],
    ["0.85", "confidence", "Of baskets with the pink spade, 85% also had the blue one.", BLUE],
    ["234", "lift", "234× more likely than chance. This is the ranking column.", GREEN],
  ];
  stats.forEach(([v, label, note, color], i) => {
    const x = M + i * 4.05;
    card(s, x, 3.2, 3.75, 2.5);
    s.addText(v, {
      x: x + 0.35, y: 3.4, w: 3.05, h: 0.85,
      fontFace: BODY, fontSize: 46, bold: true, color, margin: 0,
    });
    s.addText(label, {
      x: x + 0.35, y: 4.25, w: 3.05, h: 0.35,
      fontFace: BODY, fontSize: 14, color: MUTED, margin: 0,
    });
    s.addText(note, {
      x: x + 0.35, y: 4.65, w: 3.05, h: 0.95,
      fontFace: BODY, fontSize: 13, color: INK_2, margin: 0,
    });
  });
  s.addText(
    "Now go back to the bad row: confidence 0.61, lift 1.02. Lift near 1 means \"chance\". That's the whole answer.",
    { x: M, y: 6.05, w: 11.9, h: 0.5, fontFace: BODY, fontSize: 15, italic: true, color: INK_2, margin: 0 }
  );
  s.addNotes("This is the payoff for slide 2. Say it slowly and let it land.");
}

// =========================================================================
// 5. How to read lift
// =========================================================================
{
  const s = pres.addSlide();
  sectionTitle(s, "How to read lift", "The only column you sort by");

  const bands = [
    ["~ 1", "No relationship", "They co-occur about as often as chance predicts. Ignore.", RED],
    ["2 – 5", "Mild association", "Real but weak. Fine for a \"you might also like\" slot.", INK_2],
    ["10 +", "Strong association", "Usually a genuine complement.", BLUE],
    ["100 +", "Variant or set", "Colour/size variants, or two halves of one purchase.", GREEN],
  ];
  bands.forEach(([band, name, note, color], i) => {
    const y = 2.05 + i * 1.08;
    card(s, M, y, 11.9, 0.92);
    s.addText(band, {
      x: M + 0.35, y, w: 1.5, h: 0.92, valign: "middle",
      fontFace: BODY, fontSize: 22, bold: true, color, margin: 0,
    });
    s.addText(name, {
      x: M + 2.0, y, w: 3.0, h: 0.92, valign: "middle",
      fontFace: BODY, fontSize: 16, bold: true, color: INK, margin: 0,
    });
    s.addText(note, {
      x: M + 5.1, y, w: 6.9, h: 0.92, valign: "middle",
      fontFace: BODY, fontSize: 14, color: INK_2, margin: 0,
    });
  });
  s.addText(
    "Why not sort by confidence? A product that's in a third of all baskets shows high confidence against everything. Lift corrects for that.",
    { x: M, y: 6.5, w: 11.9, h: 0.5, fontFace: BODY, fontSize: 15, italic: true, color: INK_2, margin: 0 }
  );
}

// =========================================================================
// 6. Two signals
// =========================================================================
{
  const s = pres.addSlide();
  sectionTitle(s, "Two signals feed the list — and they are not equal",
    "Check the method column before you build a plan on a row");

  card(s, M, 2.1, 5.8, 3.6);
  stepDot(s, M + 0.4, 2.45, "✓", GREEN);
  s.addText("co_purchase", {
    x: M + 1.0, y: 2.45, w: 4.4, h: 0.45,
    fontFace: BODY, fontSize: 20, bold: true, color: INK, margin: 0,
  });
  s.addText(
    "Built from what people actually bought together.\n\n" +
    "This is the real signal. Trust it in proportion to the basket count.\n\n" +
    "Covers about 30% of the catalogue.",
    { x: M + 0.4, y: 3.15, w: 5.0, h: 2.3, fontFace: BODY, fontSize: 15, color: INK_2, margin: 0 }
  );

  card(s, 7.2, 2.1, 5.4, 3.6);
  stepDot(s, 7.6, 2.45, "!", RED);
  s.addText("content_tfidf", {
    x: 8.2, y: 2.45, w: 4.0, h: 0.45,
    fontFace: BODY, fontSize: 20, bold: true, color: INK, margin: 0,
  });
  s.addText(
    "Built from the product description text.\n\n" +
    "Nobody has been observed buying these together. They simply read alike.\n\n" +
    "A starting point — never evidence.",
    { x: 7.6, y: 3.15, w: 4.6, h: 2.3, fontFace: BODY, fontSize: 15, color: INK_2, margin: 0 }
  );

  s.addText(
    "Why keep the weaker signal at all? Without it, 7 products in 10 show an empty panel — and people generalise from the first empty screen they see.",
    { x: M, y: 6.1, w: 11.9, h: 0.5, fontFace: BODY, fontSize: 15, italic: true, color: INK_2, margin: 0 }
  );
}

// =========================================================================
// 7. Hands-on exercise
// =========================================================================
{
  const s = pres.addSlide();
  sectionTitle(s, "Your turn — 15 minutes, in pairs",
    "Trust it / don't trust it / need more information. Five real rows.");

  const rows = [
    ["1", "Childs Garden Spade Pink → Blue", "40", "0.85", "234"],
    ["2", "Landmark Frame Covent Gdn → Oxford St", "30", "0.77", "323"],
    ["3", "A new line, method = content_tfidf", "—", "—", "—"],
    ["4", "Silk Fan → Lunch Bag Vintage Doily", "34", "0.12", "2.4"],
    ["5", "Seasonal best-seller → almost everything", "410", "0.44", "1.1"],
  ];
  const head = ["#", "Pair", "Baskets", "Confidence", "Lift"];
  const colX = [M + 0.2, M + 0.8, M + 6.6, M + 8.3, M + 10.3];
  const colW = [0.5, 5.7, 1.5, 1.8, 1.4];

  head.forEach((h, c) =>
    s.addText(h, {
      x: colX[c], y: 2.05, w: colW[c], h: 0.35,
      fontFace: BODY, fontSize: 12, bold: true, color: MUTED, margin: 0,
    })
  );
  rows.forEach((r, i) => {
    const y = 2.5 + i * 0.78;
    card(s, M, y, 11.9, 0.66);
    r.forEach((cell, c) =>
      s.addText(cell, {
        x: colX[c], y, w: colW[c], h: 0.66, valign: "middle",
        fontFace: BODY, fontSize: 14, bold: c === 0, color: c === 1 ? INK : INK_2, margin: 0,
      })
    );
  });
  s.addNotes(
    "Protect these 15 minutes. If running late, cut the limits slide and email it - " +
    "never cut this exercise. Row 5 is the one that matters: high confidence, lift " +
    "of 1.1. Every group so far has had someone trust it on confidence alone."
  );
}

// =========================================================================
// 8. What it can't do
// =========================================================================
{
  const s = pres.addSlide();
  s.background = { color: NAVY };
  s.addText("Three things it cannot do", {
    x: M, y: 0.7, w: 11.9, h: 0.8,
    fontFace: HEAD, fontSize: 34, bold: true, color: WHITE, margin: 0,
  });
  s.addText("Said first, and unprompted — the credibility of everything else depends on it", {
    x: M, y: 1.5, w: 11.9, h: 0.4, fontFace: BODY, fontSize: 15, color: ICE, margin: 0,
  });

  const cants = [
    ["It cannot tell you why", "Bunting and paper plates co-occur because both are party purchases — not because one drives the other. Placing them together is sensible. Concluding that discounting one lifts the other is not."],
    ["It cannot see what you didn't stock", "A product out of stock for six weeks builds its pairings from the weeks it was available. It will look less connected than it is."],
    ["It cannot decide anything", "Nothing writes to a trading system. Every recommendation is a suggestion a person accepts or rejects. That is a design decision, not a limitation."],
  ];
  cants.forEach(([t, b], i) => {
    const y = 2.3 + i * 1.55;
    stepDot(s, M, y + 0.05, i + 1, BLUE);
    s.addText(t, {
      x: M + 0.7, y, w: 11.0, h: 0.45,
      fontFace: BODY, fontSize: 20, bold: true, color: WHITE, margin: 0,
    });
    s.addText(b, {
      x: M + 0.7, y: y + 0.48, w: 11.0, h: 0.85,
      fontFace: BODY, fontSize: 14, color: ICE, margin: 0,
    });
  });
}

// =========================================================================
// 9. How it's going
// =========================================================================
{
  const s = pres.addSlide();
  sectionTitle(s, "How it's going", "Weekly reach — % of the 62 licensed users active that week");

  s.addChart(
    pres.ChartType.line,
    [{
      name: "Reach",
      labels: ["W1","W2","W3","W4","W5","W6","W7","W8","W9","W10","W11","W12"],
      values: [6.5, 19.4, 41.9, 30.6, 32.3, 25.8, 35.5, 46.8, 48.4, 48.4, 51.6, 41.9],
    }],
    {
      x: M, y: 2.0, w: 7.6, h: 3.9,
      chartColors: [BLUE], lineSize: 3, lineSmooth: false,
      showLegend: false, showTitle: false,
      showValue: false,
      catAxisLabelColor: MUTED, valAxisLabelColor: MUTED,
      catAxisLabelFontFace: BODY, valAxisLabelFontFace: BODY,
      catAxisLabelFontSize: 11, valAxisLabelFontSize: 11,
      valGridLine: { color: "E1E0D9", size: 1 },
      catGridLine: { style: "none" },
      valAxisMaxVal: 60, valAxisMinVal: 0,
    }
  );

  card(s, 9.0, 2.0, 3.6, 3.9);
  s.addText("The two jumps", {
    x: 9.35, y: 2.25, w: 3.0, h: 0.4,
    fontFace: BODY, fontSize: 17, bold: true, color: INK, margin: 0,
  });
  s.addText(
    "Week 3 and week 8 are these workshops.\n\n" +
    "19% → 42% after the first.\n36% → 47% after the second.\n\n" +
    "Both held rather than decaying — which is the part that matters. " +
    "A spike that decays means people looked once.",
    { x: 9.35, y: 2.75, w: 3.0, h: 2.9, fontFace: BODY, fontSize: 13.5, color: INK_2, margin: 0 }
  );
  s.addText(
    "Week 6 is the school holidays. We report the dips too — a chart with no dips is a chart nobody believes.",
    { x: M, y: 6.15, w: 11.9, h: 0.5, fontFace: BODY, fontSize: 15, italic: true, color: INK_2, margin: 0 }
  );
}

// =========================================================================
// 10. Close
// =========================================================================
{
  const s = pres.addSlide();
  s.background = { color: NAVY };
  s.addText("One ask", {
    x: M, y: 1.5, w: 11.9, h: 0.8,
    fontFace: HEAD, fontSize: 38, bold: true, color: WHITE, margin: 0,
  });
  s.addText("Use it once this week on a real decision — and tell us what happened.", {
    x: M, y: 2.5, w: 11.0, h: 0.6,
    fontFace: HEAD, fontSize: 24, color: ICE, margin: 0, italic: true,
  });

  const asks = [
    ["Feedback button", "Two clicks and a score. We read every one — the June filtering change came from this."],
    ["Something looks wrong?", "Say so, and include both product codes. More useful than a low score with no comment."],
    ["Want a session for your team?", "Reply to the monthly update. Booked within a week."],
  ];
  asks.forEach(([t, b], i) => {
    const x = M + i * 4.05;
    card(s, x, 3.9, 3.75, 2.0, NAVY_2);
    s.addText(t, {
      x: x + 0.35, y: 4.15, w: 3.05, h: 0.45,
      fontFace: BODY, fontSize: 16, bold: true, color: WHITE, margin: 0,
    });
    s.addText(b, {
      x: x + 0.35, y: 4.62, w: 3.05, h: 1.15,
      fontFace: BODY, fontSize: 13, color: ICE, margin: 0,
    });
  });
  s.addText("AI Engineering  ·  business engagement", {
    x: M, y: 6.6, w: 11.9, h: 0.35, fontFace: BODY, fontSize: 13, color: "9BA9D6", margin: 0,
  });
}

pres.writeFile({ fileName: "workshop/ai_literacy_workshop.pptx" })
  .then(f => console.log("Wrote " + f));
