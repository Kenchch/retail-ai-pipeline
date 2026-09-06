"""Recompute documentation figures from the pinned full dataset without publishing."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from retail_pipeline.doc_figures import document_figures
from retail_pipeline.pipeline import check_quality, extract, load_config, transform
from retail_pipeline.recommend import recommend
from scripts.get_data import EXPECTED_SHA256, _sha256

if __name__ == "__main__":
    cfg = load_config()
    if _sha256(cfg["paths"]["raw"]) != EXPECTED_SHA256:
        raise ValueError("The source does not match the pinned retail dataset")
    raw = extract(cfg)
    clean, quarantine, results = check_quality(raw, cfg)
    tables = transform(clean)
    figures = document_figures(recommend(tables, cfg), len(tables["dim_product"]))
    path = cfg["paths"]["reports"] / "doc_figures.json"
    path.write_text(json.dumps(figures, indent=2, allow_nan=False), encoding="utf-8")
    print(path.read_text(encoding="utf-8"))
