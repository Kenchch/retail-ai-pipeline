"""The workshop deck and its PDF preview, checked against the report they quote.

Both are binary files that a person rebuilds by hand, and nothing read them.
When the recommender's population changed, `reports/doc_figures.json` and the
two audience documents moved, the tests on those documents went red until they
were fixed, and the deck stayed at confidence 0.19 and lift 95 without anything
disagreeing with it.

A PPTX is a zip of XML, so its text is read here without python-pptx. The PDF
is pictures, so it is held to the fingerprint `scripts/build_workshop.py`
writes into it.
"""

import html
import re
import zipfile
from pathlib import Path

from scripts.build_workshop import (
    FINGERPRINT_LABEL,
    figure_texts,
    figures_fingerprint,
    load_figures,
)

ROOT = Path(__file__).resolve().parents[1]
DECK = ROOT / "workshop/ai_literacy_workshop.pptx"
REBUILD = (
    "Rebuild it: python scripts/build_workshop.py, render the six slides to "
    "PNG, then run it again with --rendered-slides DIR."
)


def _slide_runs() -> dict[int, set[str]]:
    """Text runs per slide, in presentation order rather than file-name order."""
    with zipfile.ZipFile(DECK) as deck:
        rels = deck.read("ppt/_rels/presentation.xml.rels").decode("utf-8")
        targets = {
            tag_id: target
            for tag in re.findall(r"<Relationship\b[^>]*>", rels)
            for tag_id, target in [
                (
                    re.search(r'\bId="([^"]+)"', tag).group(1),
                    re.search(r'\bTarget="([^"]+)"', tag).group(1),
                )
            ]
        }
        order = re.findall(
            r'<p:sldId\b[^>]*\br:id="([^"]+)"',
            deck.read("ppt/presentation.xml").decode("utf-8"),
        )
        runs = {}
        for number, rel_id in enumerate(order, start=1):
            xml = deck.read("ppt/" + targets[rel_id].lstrip("/")).decode("utf-8")
            runs[number] = {
                html.unescape(text) for text in re.findall(r"<a:t>([^<]*)</a:t>", xml)
            }
    return runs


def test_the_deck_has_the_six_slides_the_build_script_expects():
    assert sorted(_slide_runs()) == [1, 2, 3, 4, 5, 6]


def test_every_figure_on_the_slides_is_the_reports_current_figure():
    runs = _slide_runs()
    expected = figure_texts(load_figures())
    assert len(expected) == 8, "a figure text was added or dropped; update this count"
    stale = [
        f"slide {slide}: {text!r}"
        for slide, _, text, _, _ in expected
        if text not in runs[slide]
    ]
    assert not stale, (
        "The workshop PPTX does not carry the figures in reports/doc_figures.json:\n  "
        + "\n  ".join(stale)
        + f"\n{REBUILD}"
    )


def test_the_pdf_preview_was_built_from_the_current_figures():
    pdf = DECK.with_suffix(".pdf").read_bytes()
    # Pillow writes the field as a UTF-16 string behind a byte-order mark. The
    # label and a hex digest hold no bracket or backslash, so the literal ends
    # at the first closing bracket.
    field = re.search(rb"/Keywords \(([^)]*)\)", pdf)
    keywords = ""
    if field:
        raw = field.group(1)
        keywords = raw.decode(
            "utf-16" if raw[:2] in (b"\xfe\xff", b"\xff\xfe") else "latin-1"
        )
    recorded = re.search(re.escape(FINGERPRINT_LABEL) + r" ([0-9a-f]{64})", keywords)
    assert recorded, f"The workshop PDF records no figures fingerprint. {REBUILD}"
    assert recorded.group(1) == figures_fingerprint(load_figures()), (
        "The workshop PDF was rendered from different figures than "
        f"reports/doc_figures.json now holds. {REBUILD}"
    )


def test_the_fingerprint_ignores_formatting_and_follows_values():
    figures = load_figures()
    reordered = dict(reversed(list(figures.items())))
    assert figures_fingerprint(reordered) == figures_fingerprint(figures)
    changed = {**figures, "t_light_products": figures["t_light_products"] + 1}
    assert figures_fingerprint(changed) != figures_fingerprint(figures)
