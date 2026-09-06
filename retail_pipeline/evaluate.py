"""Temporal basket-completion evaluation; this is not a causal sales experiment."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from importlib.metadata import version
from pathlib import Path

import pandas as pd

from retail_pipeline.pipeline import check_quality, extract, load_config, transform
from retail_pipeline.recommend import content_fallback, recommend


def _lookup(rows: pd.DataFrame, k: int) -> dict[str, list[str]]:
    if rows.empty:
        return {}
    return (
        rows.sort_values(["stock_code", "rank"])
        .groupby("stock_code")
        .recommended_stock_code.apply(lambda values: list(values)[:k])
        .to_dict()
    )


def score_baskets(baskets, lookup: dict, k: int, popular: list | None = None) -> dict:
    # At most one source item is removed from the global popularity ranking.
    if popular is not None:
        popular = popular[: k + 1]
    queries = hits = covered = 0
    for basket in baskets:
        items = set(basket)
        if len(items) < 2:
            continue
        for source in items:
            predictions = list(
                dict.fromkeys(
                    item
                    for item in (
                        popular if popular is not None else lookup.get(source, [])
                    )
                    if item != source
                )
            )[:k]
            queries += 1
            covered += bool(predictions)
            hits += bool(set(predictions) & (items - {source}))
    return {
        "queries": queries,
        "hits": hits,
        "hit_rate_at_k": hits / queries if queries else None,
        "query_coverage": covered / queries if queries else None,
    }


def evaluate(
    clean: pd.DataFrame, cfg: dict, cutoff: str = "2011-10-01", k: int = 5
) -> dict:
    if k < 1:
        raise ValueError("k must be positive")
    boundary = pd.Timestamp(cutoff)
    train, test = (
        clean.loc[clean.invoice_ts < boundary],
        clean.loc[clean.invoice_ts >= boundary],
    )
    crossing = set(train.invoice_no) & set(test.invoice_no)
    train = train.loc[~train.invoice_no.isin(crossing)]
    test = test.loc[~test.invoice_no.isin(crossing)]
    if train.empty or test.empty:
        raise ValueError("Both temporal partitions must contain accepted invoice lines")
    config = copy.deepcopy(cfg)
    config["recommend"]["top_n"] = k
    tables = transform(train)
    hybrid = _lookup(recommend(tables, config), k)
    text = _lookup(content_fallback(tables["dim_product"], set(), config), k)
    counts = (
        train.groupby("stock_code").invoice_no.nunique().reset_index(name="baskets")
    )
    popular = counts.sort_values(
        ["baskets", "stock_code"], ascending=[False, True]
    ).stock_code.tolist()
    baskets = test.groupby("invoice_no").stock_code.apply(list).tolist()
    return {
        "cutoff": cutoff,
        "k": k,
        "training_rows": len(train),
        "test_rows": len(test),
        "training_last_timestamp": train.invoice_ts.max().isoformat(),
        "test_first_timestamp": test.invoice_ts.min().isoformat(),
        "cross_boundary_invoices_excluded": len(crossing),
        "definition": "Each product in a held-out basket of at least two distinct products is a query; a hit retrieves any other product in that basket. All models use training data only.",
        "models": {
            "hybrid": score_baskets(baskets, hybrid, k),
            "most_popular": score_baskets(baskets, {}, k, popular),
            "content_tfidf": score_baskets(baskets, text, k),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cutoff", default="2011-10-01")
    parser.add_argument("--k", type=int, default=5)
    parser.add_argument("--output", type=Path, default=Path("reports/evaluation.json"))
    args = parser.parse_args()
    cfg = load_config()
    clean, _, _ = check_quality(extract(cfg), cfg)
    result = evaluate(clean, cfg, args.cutoff, args.k)
    result["source_sha256"] = hashlib.sha256(cfg["paths"]["raw"].read_bytes()).hexdigest()
    result["environment"] = {"pandas": pd.__version__, "scikit-learn": version("scikit-learn")}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, allow_nan=False), encoding="utf-8"
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
