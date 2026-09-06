import json
from pathlib import Path

import pandas as pd

from retail_pipeline.doc_figures import document_figures

ROOT = Path(__file__).resolve().parents[1]


def test_workshop_direction_is_pink_to_blue():
    rows = pd.DataFrame(
        [
            dict(
                stock_code="P",
                description="CHILDS GARDEN SPADE PINK",
                recommended_description="CHILDS GARDEN SPADE BLUE",
                method="co_purchase",
                pair_baskets=40,
                confidence=0.48,
                lift=95.11,
            ),
            dict(
                stock_code="B",
                description="CHILDS GARDEN SPADE BLUE",
                recommended_description="CHILDS GARDEN SPADE PINK",
                method="co_purchase",
                pair_baskets=40,
                confidence=0.40,
                lift=95.11,
            ),
        ]
    )
    assert document_figures(rows, 2)["pairs"]["spade"]["confidence"] == 0.48


def test_documented_examples_match_committed_figures():
    figures = json.loads((ROOT / "reports/doc_figures.json").read_text())
    guide = (ROOT / "docs/02_user_guide.md").read_text(encoding="utf-8")
    comms = (ROOT / "docs/03_adoption_and_comms.md").read_text(encoding="utf-8")
    lantern = figures["pairs"]["lantern"]
    assert (
        f"{lantern['baskets']} baskets, confidence {lantern['confidence']:.2f}, lift {lantern['lift']:.2f}"
        in comms
    )
    assert f"{figures['t_light_products']} different products" in guide
    assert f"median lift of {figures['t_light_median_lift']:.1f}" in guide
