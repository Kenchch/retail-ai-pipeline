"""Stage 5 - "Frequently bought together".

Two complementary signals, both produced from tables the pipeline already built:

1. Co-purchase (structured data).
   For every pair of products that appear in the same invoice we compute
   support, confidence and lift. Lift is the ranking metric because raw
   co-occurrence just re-ranks the best sellers: a popular product co-occurs
   with everything, which makes for useless recommendations.

       support(A,B)    = baskets containing both / all baskets
       confidence(A>B) = baskets containing both / baskets containing A
       lift(A,B)       = confidence(A>B) / (baskets containing B / all baskets)

   lift > 1 means B is more likely in a basket that already contains A than in
   a basket picked at random - i.e. a real association rather than popularity.

2. Description similarity (unstructured data).
   Products that never appear in a qualifying pair - new or long-tail lines -
   get no co-purchase rule at all. For those we fall back to TF-IDF over the
   free-text product description and recommend the nearest neighbours by
   cosine similarity, so the cold-start slots are filled with something
   defensible instead of being left empty.
"""

from __future__ import annotations

from collections import Counter
from itertools import combinations

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors

from .config import Config, get_logger

log = get_logger(__name__)

# The output schema, declared once. An empty result is still a table with these
# columns - a DataFrame with zero columns is not "no recommendations", it is a
# shape no consumer can read, and it produces invalid SQL at the load stage.
RECOMMENDATION_COLUMNS = [
    "stock_code", "description", "recommended_stock_code", "recommended_description",
    "rank", "method", "pair_baskets", "support", "confidence", "lift", "similarity",
]


# --------------------------------------------------------------------------- #
# 1. Co-purchase rules from structured transaction data
# --------------------------------------------------------------------------- #

def build_baskets(fact: pd.DataFrame, cfg: Config) -> pd.Series:
    """One row per invoice holding the set of distinct products bought."""
    max_size = cfg.recommend["max_basket_size"]
    baskets = fact.groupby("invoice_no")["stock_code"].apply(lambda s: sorted(set(s)))
    sizes = baskets.str.len()
    kept = baskets[(sizes >= 2) & (sizes <= max_size)]
    log.info(
        "Baskets: %s total, %s usable (2-%s distinct products), median size %.0f",
        f"{len(baskets):,}", f"{len(kept):,}", max_size, sizes[sizes >= 2].median(),
    )
    return kept


def co_purchase_rules(baskets: pd.Series, cfg: Config) -> pd.DataFrame:
    """Pairwise association rules with support / confidence / lift."""
    n_baskets = len(baskets)
    item_counts: Counter = Counter()
    pair_counts: Counter = Counter()

    for items in baskets:
        item_counts.update(items)
        pair_counts.update(combinations(items, 2))  # items are sorted -> stable key

    log.info("Counted %s distinct co-purchase pairs", f"{len(pair_counts):,}")

    min_count = cfg.recommend["min_support_count"]
    frequent = {p: c for p, c in pair_counts.items() if c >= min_count}
    log.info(
        "%s pairs meet min_support_count=%s (%.2f%% of all pairs)",
        f"{len(frequent):,}", min_count, 100 * len(frequent) / max(len(pair_counts), 1),
    )

    # Emit each pair in both directions - a recommendation is directional.
    rows = []
    for (a, b), pair_n in frequent.items():
        for src, dst in ((a, b), (b, a)):
            conf = pair_n / item_counts[src]
            lift = conf / (item_counts[dst] / n_baskets)
            rows.append(
                {
                    "stock_code": src,
                    "recommended_stock_code": dst,
                    "pair_baskets": pair_n,
                    "support": pair_n / n_baskets,
                    "confidence": conf,
                    "lift": lift,
                }
            )

    rules = pd.DataFrame(rows)
    if rules.empty:
        return rules

    before = len(rules)
    rules = rules[
        (rules["confidence"] >= cfg.recommend["min_confidence"])
        & (rules["lift"] >= cfg.recommend["min_lift"])
    ].copy()
    log.info(
        "%s of %s directional rules pass confidence>=%.2f and lift>=%.2f",
        f"{len(rules):,}", f"{before:,}",
        cfg.recommend["min_confidence"], cfg.recommend["min_lift"],
    )
    return rules


def top_n_per_product(rules: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    if rules.empty:
        return rules
    top_n = cfg.recommend["top_n"]
    out = (
        rules.sort_values(["stock_code", "lift", "confidence"], ascending=[True, False, False])
        .groupby("stock_code")
        .head(top_n)
        .copy()
    )
    out["rank"] = out.groupby("stock_code").cumcount() + 1
    out["method"] = "co_purchase"
    return out


# --------------------------------------------------------------------------- #
# 2. Content fallback from unstructured product descriptions
# --------------------------------------------------------------------------- #

def content_fallback(
    dim_product: pd.DataFrame, covered: set[str], cfg: Config
) -> pd.DataFrame:
    """TF-IDF nearest neighbours for products with no co-purchase rule."""
    catalogue = dim_product[dim_product["description"] != "UNKNOWN"].reset_index(drop=True)
    cold = catalogue[~catalogue["stock_code"].isin(covered)]
    if cold.empty or len(catalogue) < 5:
        log.info("No cold-start products - skipping content fallback")
        return pd.DataFrame()

    top_n = cfg.recommend["cold_start_top_n"]
    # `min_df=2` keeps one-off words out of the vocabulary, which is right on a
    # real catalogue and fatal on a small or unusual one: if no term survives
    # pruning - or the descriptions are nothing but stop words - the vectoriser
    # raises. This fallback is a nice-to-have, so it degrades rather than taking
    # down the co-purchase rules that were computed successfully.
    matrix = None
    for min_df in (2, 1):
        try:
            vec = TfidfVectorizer(
                lowercase=True, stop_words="english", ngram_range=(1, 2), min_df=min_df
            )
            matrix = vec.fit_transform(catalogue["description"])
            if min_df == 1:
                log.warning(
                    "Descriptions share too few terms for min_df=2; retried with min_df=1"
                )
            break
        except ValueError as exc:
            log.warning("TF-IDF with min_df=%s failed: %s", min_df, exc)
    if matrix is None or matrix.shape[1] == 0:
        log.warning(
            "Product descriptions carry no usable vocabulary - skipping the content "
            "fallback. Co-purchase recommendations are unaffected."
        )
        return pd.DataFrame()
    log.info(
        "Cold start: %s products without a rule | TF-IDF vocabulary %s terms",
        f"{len(cold):,}", f"{len(vec.vocabulary_):,}",
    )

    nn = NearestNeighbors(n_neighbors=min(top_n + 1, len(catalogue)), metric="cosine")
    nn.fit(matrix)
    dist, idx = nn.kneighbors(matrix[cold.index.to_numpy()])

    rows = []
    for row_i, (d_row, i_row) in enumerate(zip(dist, idx)):
        src = cold.iloc[row_i]["stock_code"]
        rank = 0
        for d, i in zip(d_row, i_row):
            dst = catalogue.iloc[i]["stock_code"]
            if dst == src:
                continue
            sim = 1 - d
            if sim <= 0:
                continue
            rank += 1
            rows.append(
                {
                    "stock_code": src,
                    "recommended_stock_code": dst,
                    "pair_baskets": 0,
                    "support": 0.0,
                    "confidence": 0.0,
                    "lift": 0.0,
                    "similarity": round(float(sim), 4),
                    "rank": rank,
                    "method": "content_tfidf",
                }
            )
            if rank >= top_n:
                break
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #

def recommend(tables: dict[str, pd.DataFrame], cfg: Config) -> pd.DataFrame:
    fact, dim_product = tables["fact_sales"], tables["dim_product"]

    baskets = build_baskets(fact, cfg)
    rules = co_purchase_rules(baskets, cfg)
    top = top_n_per_product(rules, cfg)

    covered = set(top["stock_code"]) if not top.empty else set()
    cold = content_fallback(dim_product, covered, cfg)

    recs = pd.concat([top, cold], ignore_index=True) if not cold.empty else top
    if recs.empty:
        log.warning("No recommendations produced - loosen the thresholds in config.yaml")
        return pd.DataFrame(columns=RECOMMENDATION_COLUMNS)

    # Attach human-readable names so the table can be handed straight to a
    # business user or a merchandising tool without another join.
    names = dim_product[["stock_code", "description"]]
    recs = (
        recs.merge(names, on="stock_code", how="left")
        .merge(
            names.rename(
                columns={"stock_code": "recommended_stock_code",
                         "description": "recommended_description"}
            ),
            on="recommended_stock_code", how="left",
        )
    )
    for col in ("support", "confidence", "lift"):
        recs[col] = recs[col].round(5)
    if "similarity" not in recs.columns:
        recs["similarity"] = 0.0
    recs["similarity"] = recs["similarity"].fillna(0.0)

    recs = recs[
        ["stock_code", "description", "recommended_stock_code", "recommended_description",
         "rank", "method", "pair_baskets", "support", "confidence", "lift", "similarity"]
    ].sort_values(["method", "stock_code", "rank"]).reset_index(drop=True)

    coverage = recs["stock_code"].nunique() / len(dim_product)
    log.info(
        "Recommendations: %s rows for %s products (%.1f%% catalogue coverage)",
        f"{len(recs):,}", f"{recs['stock_code'].nunique():,}", 100 * coverage,
    )
    return recs
