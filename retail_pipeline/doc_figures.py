"""Documentation figures computed from the same recommendation table as publication."""

import pandas as pd


def document_figures(recs: pd.DataFrame, n_products: int) -> dict:
    pairs = {}
    for key, source, target in (
        ("lantern", "WHITE METAL LANTERN", "WHITE HANGING HEART T-LIGHT HOLDER"),
        ("spade", "CHILDS GARDEN SPADE PINK", "CHILDS GARDEN SPADE BLUE"),
    ):
        if {"description", "recommended_description"}.issubset(recs.columns):
            rows = recs.loc[
                (recs.description == source) & (recs.recommended_description == target)
            ]
            rows = rows.loc[rows.method == "co_purchase"] if "method" in rows else rows
            if not rows.empty:
                row = rows.iloc[0]
                pairs[key] = {
                    "source": source,
                    "target": target,
                    "baskets": int(row.pair_baskets),
                    "confidence": round(float(row.confidence), 2),
                    "lift": round(float(row.lift), 2),
                }
    target_rows = recs.loc[
        recs.get("recommended_description", pd.Series(index=recs.index, dtype=str))
        == "WHITE HANGING HEART T-LIGHT HOLDER"
    ]
    return {
        "pairs": pairs,
        "t_light_products": int(target_rows.stock_code.nunique()),
        "t_light_median_lift": (
            round(float(target_rows.lift.median()), 1)
            if not target_rows.empty and target_rows.lift.notna().any()
            else None
        ),
        "catalogue_coverage_pct": (
            round(100 * recs.stock_code.nunique() / n_products, 1)
            if n_products
            else 0.0
        ),
    }
