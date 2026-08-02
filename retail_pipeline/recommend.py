"""Stage 4 - "frequently bought together".

Two signals, one structured and one unstructured:

**Co-purchase (structured).** For every product pair in the same invoice:

    support(A,B)    = baskets with both / all baskets
    confidence(A→B) = baskets with both / baskets with A
    lift(A,B)       = confidence(A→B) / (baskets with B / all baskets)

Rules are ranked by **lift**, not by raw co-occurrence, because raw counts just
re-rank the best sellers - a popular product co-occurs with everything, which
makes for recommendations that are confidently useless. `lift > 1` means B is
genuinely more likely in a basket that already holds A than in a random one.

**Description similarity (unstructured).** Long-tail products never reach the
support threshold and would show an empty panel. TF-IDF over the free-text
product description fills those slots. Every row carries a `method` column, so a
behavioural rule is never mistaken for a text-similarity guess.
"""

from __future__ import annotations

import logging
from collections import Counter
from itertools import combinations

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors

log = logging.getLogger("pipeline")

# An empty result is still a table with these columns. A zero-column DataFrame
# is not "no recommendations", it is a shape no consumer can read.
COLUMNS = ["stock_code", "description", "recommended_stock_code",
           "recommended_description", "rank", "method",
           "pair_baskets", "support", "confidence", "lift"]


def co_purchase_rules(fact: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    c = cfg["recommend"]
    baskets = fact.groupby("invoice_no")["stock_code"].apply(lambda s: sorted(set(s)))
    sizes = baskets.str.len()
    baskets = baskets[(sizes >= 2) & (sizes <= c["max_basket_size"])]

    items: Counter = Counter()
    pairs: Counter = Counter()
    for products in baskets:
        items.update(products)
        pairs.update(combinations(products, 2))   # sorted -> stable key

    n = len(baskets)
    rows = []
    for (a, b), count in pairs.items():
        if count < c["min_support_count"]:
            continue
        for src, dst in ((a, b), (b, a)):         # a recommendation is directional
            conf = count / items[src]
            rows.append({"stock_code": src, "recommended_stock_code": dst,
                         "pair_baskets": count, "support": count / n,
                         "confidence": conf, "lift": conf / (items[dst] / n)})

    rules = pd.DataFrame(rows)
    log.info("Baskets %s | pairs %s | rules %s", f"{n:,}", f"{len(pairs):,}", f"{len(rules):,}")
    if rules.empty:
        return rules
    rules = rules[(rules["confidence"] >= c["min_confidence"]) & (rules["lift"] >= c["min_lift"])]
    top = (rules.sort_values(["stock_code", "lift"], ascending=[True, False])
                .groupby("stock_code").head(c["top_n"]).copy())
    top["rank"] = top.groupby("stock_code").cumcount() + 1
    top["method"] = "co_purchase"
    return top


def content_fallback(dim_product: pd.DataFrame, covered: set[str], cfg: dict) -> pd.DataFrame:
    catalogue = dim_product[dim_product["description"] != "UNKNOWN"].reset_index(drop=True)
    cold = catalogue[~catalogue["stock_code"].isin(covered)]
    if cold.empty or len(catalogue) < 5:
        return pd.DataFrame()

    top_n = cfg["recommend"]["top_n"]
    # min_df=2 keeps one-off words out, which is right on a real catalogue and
    # fatal on a small one - if nothing survives pruning the vectoriser raises.
    # This fallback is optional, so it degrades instead of taking down the
    # co-purchase rules that computed fine.
    matrix = None
    for min_df in (2, 1):
        try:
            vec = TfidfVectorizer(stop_words="english", ngram_range=(1, 2), min_df=min_df)
            matrix = vec.fit_transform(catalogue["description"])
            break
        except ValueError:
            continue
    if matrix is None or matrix.shape[1] == 0:
        log.warning("Descriptions carry no usable vocabulary - skipping content fallback")
        return pd.DataFrame()

    nn = NearestNeighbors(n_neighbors=min(top_n + 1, len(catalogue)), metric="cosine").fit(matrix)
    _, idx = nn.kneighbors(matrix[cold.index.to_numpy()])

    rows = []
    for i, neighbours in enumerate(idx):
        src = cold.iloc[i]["stock_code"]
        rank = 0
        for j in neighbours:
            dst = catalogue.iloc[j]["stock_code"]
            if dst == src:
                continue
            rank += 1
            rows.append({"stock_code": src, "recommended_stock_code": dst, "rank": rank,
                         "method": "content_tfidf", "pair_baskets": 0,
                         "support": 0.0, "confidence": 0.0, "lift": 0.0})
            if rank >= top_n:
                break
    log.info("Content fallback covered %s cold-start products", f"{len(cold):,}")
    return pd.DataFrame(rows)


def recommend(tables: dict[str, pd.DataFrame], cfg: dict) -> pd.DataFrame:
    dim_product = tables["dim_product"]
    top = co_purchase_rules(tables["fact_sales"], cfg)
    covered = set(top["stock_code"]) if not top.empty else set()
    cold = content_fallback(dim_product, covered, cfg)

    parts = [f for f in (top, cold) if not f.empty]
    recs = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if recs.empty:
        log.warning("No recommendations produced - loosen the thresholds in config.yaml")
        return pd.DataFrame(columns=COLUMNS)

    # Attach names so the table can go straight to a merchandiser.
    names = dim_product[["stock_code", "description"]]
    recs = (recs.merge(names, on="stock_code", how="left")
                .merge(names.rename(columns={"stock_code": "recommended_stock_code",
                                             "description": "recommended_description"}),
                       on="recommended_stock_code", how="left"))
    for col in ("support", "confidence", "lift"):
        recs[col] = recs[col].round(5)

    log.info("Recommendations %s rows | %.1f%% of the catalogue covered",
             f"{len(recs):,}", 100 * recs["stock_code"].nunique() / len(dim_product))
    return recs[COLUMNS].sort_values(["method", "stock_code", "rank"]).reset_index(drop=True)
