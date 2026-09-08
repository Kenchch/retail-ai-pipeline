"""Stage 4 - "frequently bought together".

Two signals, one structured and one unstructured:

**Co-purchase (structured).** For every product pair in the same invoice:

    support(A,B)    = baskets with both / all baskets
    confidence(A→B) = baskets with both / baskets with A
    lift(A,B)       = confidence(A→B) / (baskets with B / all baskets)

"All baskets" means all of them - including single-item baskets, which are
evidence against a rule rather than absent from it. Only the numerator's pair
counting skips baskets outside 2..max_basket_size: a one-item basket cannot
form a pair, and a 1,107-item wholesale order would contribute 612k pairs of
things that merely shared a pallet. So the three figures are conservative,
never inflated - a co-purchase inside an oversized basket goes uncounted.

Worked example, three baskets: {A}, {A, B}, {A, B, C}

    all baskets     = 3        A appears in 3, B in 2, C in 1
    A and B together = 2
    support(A,B)     = 2/3 = 0.67
    confidence(A→B)  = 2/3 = 0.67      <- not 2/2, the {A} basket counts
    lift(A,B)        = 0.67 / (2/3) = 1.0

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

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors

log = logging.getLogger("pipeline")

# An empty result is still a table with these columns. A zero-column DataFrame
# is not "no recommendations", it is a shape no consumer can read.
COLUMNS = [
    "stock_code",
    "description",
    "recommended_stock_code",
    "recommended_description",
    "rank",
    "method",
    "pair_baskets",
    "support",
    "confidence",
    "lift",
]

# How many nearest neighbours the content fallback asks for before sorting and
# truncating to top_n. Chosen by measurement, not by feel: under three
# permutations of the real catalogue, top_n + 1 leaves 6.2% of cold-start
# products with different published recommendations, 20 leaves 1.4%, 50 leaves
# 0.45%, and 125 leaves the same 0.45%. See content_fallback.
CANDIDATE_WIDTH = 50


def co_purchase_rules(fact: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    c = cfg["recommend"]
    # No rows is a shape to report, not to crash on. The pipeline's own path
    # cannot reach here empty (transform() raises on an empty frame first, and
    # an empty fact_sales read back from Parquet keeps its `string` dtype and
    # would survive anyway), but a frame carrying no dtype information comes
    # back from the groupby as float64, and .str on that raises an
    # AttributeError naming the dtype rather than the empty input.
    if fact.empty:
        log.warning("fact_sales is empty - no baskets to mine")
        return pd.DataFrame()

    baskets = fact.groupby("invoice_no")["stock_code"].apply(lambda s: sorted(set(s)))
    # .map(len), not .str.len(): .str needs pandas to have inferred a string or
    # object dtype, which it cannot always do from a group result. len() works
    # on the lists regardless.
    sizes = baskets.map(len)

    # The population is every basket, and the size filter applies ONLY to pair
    # generation. These are two different questions and conflating them
    # overstates the result.
    #
    # "Of the baskets containing A, how many also contain B" is a question about
    # all baskets containing A - including the ones where A was bought alone,
    # which are evidence *against* the rule, and including wholesale-scale ones.
    # Counting `items` and `n` over the filtered set instead shrinks every
    # denominator: on this dataset it inflated confidence on 100% of published
    # rules, by 1.47x at the median and 4.2x at the worst (84032A -> 84032B read
    # 0.59 where the true figure is 0.14, because 239 of that product's 313
    # baskets were larger than the cap and silently left the denominator).
    #
    # Pair generation still skips them, for two different reasons: a one-item
    # basket cannot produce a pair at all, and a 1,107-item wholesale order
    # would contribute 612k pairs of things that merely shared a pallet. That
    # makes support/confidence/lift *conservative* - co-occurrences inside
    # oversized baskets go uncounted - which is the safe direction for a number
    # someone is going to act on.
    n = len(baskets)
    items: Counter = Counter()
    for products in baskets:
        items.update(products)

    pairs: Counter = Counter()
    for products in baskets[(sizes >= 2) & (sizes <= c["max_basket_size"])]:
        pairs.update(combinations(products, 2))  # sorted -> stable key
    rows = []
    for (a, b), count in pairs.items():
        if count < c["min_support_count"]:
            continue
        for src, dst in ((a, b), (b, a)):  # a recommendation is directional
            conf = count / items[src]
            rows.append(
                {
                    "stock_code": src,
                    "recommended_stock_code": dst,
                    "pair_baskets": count,
                    "support": count / n,
                    "confidence": conf,
                    "lift": conf / (items[dst] / n),
                }
            )

    rules = pd.DataFrame(rows)
    log.info(
        "Baskets %s | pairs %s | rules %s",
        f"{n:,}",
        f"{len(pairs):,}",
        f"{len(rules):,}",
    )
    if rules.empty:
        return rules
    rules = rules[
        (rules["confidence"] >= c["min_confidence"]) & (rules["lift"] >= c["min_lift"])
    ]
    # recommended_stock_code is a tiebreaker for a tie that exists -- one
    # product of the 1,120 with rules has a lift tie straddling its fifth slot
    # -- but it is not fixing an observed defect, and the comment should not
    # imply otherwise. This path is already order-independent: `groupby` sorts
    # its keys, each basket is `sorted(set(...))`, and pandas sorts multiple
    # columns with a stable lexsort. Shuffling the fact table changed nothing,
    # on the real extract or on fixtures up to 1,024 tied candidates.
    #
    # What the key buys is that the guarantee stops depending on three
    # incidental properties holding at once, none of which this module states
    # or tests. The tied product is decided by the data after this, rather than
    # decided correctly by luck.
    top = (
        rules.sort_values(
            ["stock_code", "lift", "recommended_stock_code"],
            ascending=[True, False, True],
        )
        .groupby("stock_code")
        .head(c["top_n"])
        .copy()
    )
    top["rank"] = top.groupby("stock_code").cumcount() + 1
    top["method"] = "co_purchase"
    return top


def content_fallback(
    dim_product: pd.DataFrame, covered: set[str], cfg: dict
) -> pd.DataFrame:
    catalogue = dim_product[dim_product["description"] != "UNKNOWN"].reset_index(
        drop=True
    )
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
            vec = TfidfVectorizer(
                stop_words="english", ngram_range=(1, 2), min_df=min_df
            )
            matrix = vec.fit_transform(catalogue["description"])
            break
        except ValueError:
            continue
    if matrix is None or matrix.shape[1] == 0:
        log.warning(
            "Descriptions carry no usable vocabulary - skipping content fallback"
        )
        return pd.DataFrame()

    # Cosine distance ties are not an edge case on a giftware catalogue: 22.1%
    # of the cold-start neighbour lists contain one, and 216 of them contain
    # more than one distance of exactly zero -- products whose descriptions are
    # identical text. Which of a tied group got published was decided by the
    # order the catalogue happened to arrive in, so reordering it changed the
    # recommendations for 15.7% of cold-start products.
    #
    # Two changes, both measured against three catalogue permutations on the
    # real extract. Sorting each list by (distance, stock_code) before
    # truncating takes 15.7% to 6.2%; it fixes the order within the returned
    # set but not which candidates are in it when a tie straddles the boundary.
    # Fetching CANDIDATE_WIDTH rather than top_n + 1 takes it to 0.45%. The
    # residual 12 products have tie groups wider than the fetch: raising the
    # width to 125 leaves the same 12, so it is not a width worth paying for.
    nn = NearestNeighbors(
        n_neighbors=min(CANDIDATE_WIDTH, len(catalogue)), metric="cosine"
    ).fit(matrix)
    distances, idx = nn.kneighbors(matrix[cold.index.to_numpy()])

    rows = []
    for i, neighbours in enumerate(idx):
        src = cold.iloc[i]["stock_code"]
        candidates = sorted(
            (
                # Rounded because two mathematically equal cosine distances can
                # differ in the last bits, which would make the tiebreaker
                # depend on floating-point noise instead of resolving it.
                (round(float(distance), 12), catalogue.iloc[j]["stock_code"])
                for distance, j in zip(distances[i], neighbours, strict=True)
                if catalogue.iloc[j]["stock_code"] != src
            )
        )
        rank = 0
        for _, dst in candidates:
            rank += 1
            # NaN, not 0.0. Support, confidence and lift are co-occurrence
            # statistics and are undefined for a text-similarity match - there
            # is no basket evidence to compute them from. Writing 0.0 states
            # "measured, and the worst possible value", which sorts every
            # content row to the bottom of a lift-ranked view and reads as a
            # failed recommendation rather than a differently-derived one.
            # Same rule the adoption report states: no data shows as blank,
            # never as zero.
            rows.append(
                {
                    "stock_code": src,
                    "recommended_stock_code": dst,
                    "rank": rank,
                    "method": "content_tfidf",
                    "pair_baskets": pd.NA,
                    "support": np.nan,
                    "confidence": np.nan,
                    "lift": np.nan,
                }
            )
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
        log.warning(
            "No recommendations produced - loosen the thresholds in config.yaml"
        )
        return pd.DataFrame(columns=COLUMNS)

    # Attach names so the table can go straight to a merchandiser.
    names = dim_product[["stock_code", "description"]]
    recs = recs.merge(names, on="stock_code", how="left").merge(
        names.rename(
            columns={
                "stock_code": "recommended_stock_code",
                "description": "recommended_description",
            }
        ),
        on="recommended_stock_code",
        how="left",
    )
    for col in ("support", "confidence", "lift"):
        recs[col] = recs[col].round(5)

    # Coverage has two routes and a product can miss both: no co-purchase rule
    # AND a description the content fallback cannot use (it drops "UNKNOWN").
    # This run happens to reach 100%, which makes the gap invisible unless it
    # is counted - and the acceptance criterion in the engineering brief is
    # >=90%, so silence here would be indistinguishable from success.
    uncovered = set(dim_product["stock_code"]) - set(recs["stock_code"])
    if uncovered:
        blank = set(
            dim_product.loc[dim_product["description"] == "UNKNOWN", "stock_code"]
        )
        log.warning(
            "%s of %s products have no recommendation by either route "
            "(%s of them also have no usable description)",
            f"{len(uncovered):,}",
            f"{len(dim_product):,}",
            f"{len(uncovered & blank):,}",
        )

    log.info(
        "Recommendations %s rows | %.1f%% of the catalogue covered",
        f"{len(recs):,}",
        100 * recs["stock_code"].nunique() / len(dim_product),
    )
    return (
        recs[COLUMNS]
        .sort_values(["method", "stock_code", "rank"])
        .reset_index(drop=True)
    )
