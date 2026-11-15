"""Temporal basket-completion evaluation; this is not a causal sales experiment.

Every product in a held-out basket is a query, and a hit retrieves another
product from the same basket. Two ways of averaging those queries are reported
and they are not interchangeable:

* `basket_hit_rate_at_k` - the headline. Each basket's queries are averaged
  first, then the baskets are averaged, so a basket counts once.
* `hit_rate_at_k` - every query counts once, so a basket counts once per product
  it holds.

The second was the only figure this module used to report, and on this extract
it mostly measures wholesale orders: baskets of more than 60 products are a
small share of the baskets and close to half of the queries, and in a basket
that size five best sellers are a hit for almost any query. It put a model that
knows nothing about the query within a few points of one that does. The
per-size breakdown is kept in the report so that effect stays visible.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import heapq
import json
from collections import Counter, defaultdict
from importlib.metadata import version
from itertools import combinations
from pathlib import Path

import pandas as pd

from retail_pipeline.pipeline import check_quality, extract, load_config, transform
from retail_pipeline.recommend import RANK_KEYS, content_fallback, recommend

# Upper bounds of the basket-size bands, in distinct products. The last band is
# open-ended and starts above the default `max_basket_size`.
SIZE_BANDS = ((5, "2-5"), (10, "6-10"), (20, "11-20"), (60, "21-60"), (None, "61+"))


def _band(size: int) -> str:
    for upper, label in SIZE_BANDS:
        if upper is None or size <= upper:
            return label
    raise AssertionError("the last band is open-ended")


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
    n_baskets = 0
    basket_rates = 0.0
    bands = {label: [0, 0, 0, 0.0] for _, label in SIZE_BANDS}
    for basket in baskets:
        items = set(basket)
        if len(items) < 2:
            continue
        basket_hits = 0
        # Sorted so the float sum below does not depend on set iteration order.
        for source in sorted(items):
            predictions = list(
                dict.fromkeys(
                    item
                    for item in (
                        popular if popular is not None else lookup.get(source, [])
                    )
                    if item != source
                )
            )[:k]
            covered += bool(predictions)
            basket_hits += bool(set(predictions) & (items - {source}))
        queries += len(items)
        hits += basket_hits
        n_baskets += 1
        basket_rates += basket_hits / len(items)
        band = bands[_band(len(items))]
        band[0] += 1
        band[1] += len(items)
        band[2] += basket_hits
        band[3] += basket_hits / len(items)
    return {
        "baskets": n_baskets,
        "queries": queries,
        "hits": hits,
        "basket_hit_rate_at_k": basket_rates / n_baskets if n_baskets else None,
        "hit_rate_at_k": hits / queries if queries else None,
        "query_coverage": covered / queries if queries else None,
        "by_basket_size": {
            label: {
                "baskets": b,
                "queries": q,
                "basket_hit_rate_at_k": rates / b if b else None,
                "hit_rate_at_k": h / q if q else None,
            }
            for label, (b, q, h, rates) in bands.items()
        },
    }


def co_occurrence_lookup(
    train: pd.DataFrame, popular: list[str], k: int, max_basket_size: int
) -> dict[str, list[str]]:
    """The baseline a recommender has to beat: bought together most often.

    No support, confidence or lift threshold and no text: for each product, the
    k products that shared the most training baskets with it, topped up from the
    best sellers when it has fewer than k. The basket-size cap is the one the
    rules use, so the two see the same pairs.
    """
    pairs: Counter = Counter()
    for products in train.groupby("invoice_no").stock_code.apply(
        lambda s: sorted(set(s))
    ):
        if len(products) <= max_basket_size:
            pairs.update(combinations(products, 2))
    neighbours: dict[str, list[tuple[int, str]]] = defaultdict(list)
    for (a, b), count in pairs.items():
        neighbours[a].append((-count, b))
        neighbours[b].append((-count, a))
    lookup = {}
    for code in popular:
        picks = [other for _, other in heapq.nsmallest(k, neighbours.get(code, []))]
        for candidate in popular:
            if len(picks) >= k:
                break
            if candidate != code and candidate not in picks:
                picks.append(candidate)
        lookup[code] = picks
    return lookup


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
    text = _lookup(content_fallback(tables["dim_product"], set(), config), k)
    counts = (
        train.groupby("stock_code").invoice_no.nunique().reset_index(name="baskets")
    )
    popular = counts.sort_values(
        ["baskets", "stock_code"], ascending=[False, True]
    ).stock_code.tolist()
    baskets = test.groupby("invoice_no").stock_code.apply(list).tolist()

    # The published recommender, then the same recommender with each other
    # ranking key, so the choice of key in config.yaml is one that was scored.
    published = config["recommend"].get("rank_by", "lift")
    ranking = {}
    for key in RANK_KEYS:
        variant = copy.deepcopy(config)
        variant["recommend"]["rank_by"] = key
        ranking[key] = score_baskets(baskets, _lookup(recommend(tables, variant), k), k)
    return {
        "cutoff": cutoff,
        "k": k,
        "training_rows": len(train),
        "test_rows": len(test),
        "training_last_timestamp": train.invoice_ts.max().isoformat(),
        "test_first_timestamp": test.invoice_ts.min().isoformat(),
        "cross_boundary_invoices_excluded": len(crossing),
        "definition": "Each product in a held-out basket of at least two distinct products is a query; a hit retrieves any other product in that basket. basket_hit_rate_at_k averages the queries within each basket and then the baskets; hit_rate_at_k weights every query equally, so large baskets dominate it. All models use training data only.",
        "published_rank_by": published,
        "models": {
            "hybrid": ranking[published],
            "co_occurrence": score_baskets(
                baskets,
                co_occurrence_lookup(
                    train, popular, k, config["recommend"]["max_basket_size"]
                ),
                k,
            ),
            "most_popular": score_baskets(baskets, {}, k, popular),
            "content_tfidf": score_baskets(baskets, text, k),
        },
        "hybrid_by_rank_key": {
            key: {
                "basket_hit_rate_at_k": result["basket_hit_rate_at_k"],
                "hit_rate_at_k": result["hit_rate_at_k"],
            }
            for key, result in ranking.items()
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--cutoff", default="2011-10-01")
    parser.add_argument("--k", type=int, default=5)
    parser.add_argument("--output", type=Path, default=Path("reports/evaluation.json"))
    args = parser.parse_args()
    cfg = load_config()
    clean, _, _ = check_quality(extract(cfg), cfg)
    result = evaluate(clean, cfg, args.cutoff, args.k)
    result["source_sha256"] = hashlib.sha256(
        cfg["paths"]["raw"].read_bytes()
    ).hexdigest()
    result["environment"] = {
        "pandas": pd.__version__,
        "scikit-learn": version("scikit-learn"),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, allow_nan=False), encoding="utf-8"
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
