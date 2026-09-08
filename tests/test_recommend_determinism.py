"""Two products with equal evidence must not be ordered by luck.

Both ranking paths truncate: co-purchase keeps the top `top_n` rules by lift,
and the content fallback keeps the `top_n` nearest descriptions. Where the sort
key ties, something has to break it.

The two paths were in different states, and it is worth being precise about
which:

**The content fallback was order-dependent.** Permuting the catalogue changed
the published recommendations for 15.7% of cold-start products, because 22.1%
of neighbour lists contain a distance tie and 216 contain more than one
distance of exactly zero -- products whose descriptions are identical text.
End to end, with the fact table shuffled too, 986-1,006 of 17,083 published
rows changed, about 5.8%. After the fix: 33 rows, 0.19%.

**The co-purchase path was not.** Shuffling the fact table changed nothing, on
the real extract or on fixtures up to 1,024 tied candidates over ten seeds.
`groupby` sorts its keys, each basket is sorted, and pandas sorts multiple
columns with a stable lexsort, so the order was already fixed -- by three
incidental properties rather than by a stated sort key. Its test below passed
before the tiebreaker was added and is a characterisation test, not a
regression test. Said plainly because a test that passes either way proves
nothing on its own.

Running the same input twice always agreed, before and after, on both paths.
The committed reports were reproducible; what was wrong was a ranking that
partly encoded row order rather than evidence.
"""

import numpy as np
import pandas as pd

from retail_pipeline import recommend as recommender

CFG = {
    "recommend": {
        "top_n": 3,
        "min_support_count": 2,
        "min_confidence": 0.0,
        "min_lift": 0.0,
        "max_basket_size": 50,
    }
}


def _fact(pairs: list[tuple[str, list[str]]]) -> pd.DataFrame:
    rows = []
    for invoice, codes in pairs:
        for code in codes:
            rows.append(
                {
                    "invoice_no": invoice,
                    "stock_code": code,
                    "quantity": 1,
                    "unit_price": 1.0,
                    "revenue": 1.0,
                }
            )
    return pd.DataFrame(rows)


def _dim(codes_to_descriptions: dict[str, str]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"stock_code": code, "description": description}
            for code, description in codes_to_descriptions.items()
        ]
    )


def _published(tables: dict[str, pd.DataFrame]) -> list[tuple]:
    out = recommender.recommend(tables, CFG)
    if out.empty:
        return []
    ordered = out.sort_values(["method", "stock_code", "rank"])
    return list(
        ordered[["method", "stock_code", "rank", "recommended_stock_code"]].itertuples(
            index=False, name=None
        )
    )


def test_tied_lifts_do_not_depend_on_the_order_the_rows_arrive_in():
    """A anchors four candidates that each co-occur with it exactly twice, so
    all four rules carry the same lift and only three can be published.

    This passed before the tiebreaker existed -- see the module docstring. It
    is here to hold the property in place, not to demonstrate a bug.
    """
    baskets = []
    for i, code in enumerate(["W", "X", "Y", "Z"]):
        baskets.append((f"inv{i}a", ["A", code]))
        baskets.append((f"inv{i}b", ["A", code]))
    fact = _fact(baskets)
    dim = _dim({c: f"PRODUCT {c}" for c in ["A", "W", "X", "Y", "Z"]})

    reference = _published({"fact_sales": fact, "dim_product": dim})
    assert reference, "the fixture produced no rules to compare"

    for seed in (1, 2, 3, 4, 5):
        rng = np.random.default_rng(seed)
        shuffled = {
            "fact_sales": fact.sample(frac=1.0, random_state=seed).reset_index(
                drop=True
            ),
            "dim_product": dim.iloc[rng.permutation(len(dim))].reset_index(drop=True),
        }
        assert _published(shuffled) == reference, (
            f"seed {seed} published a different set of tied candidates"
        )


def test_identical_descriptions_do_not_depend_on_catalogue_order():
    """Cosine distance between identical text is exactly zero, so the content
    fallback has to choose between candidates it cannot tell apart."""
    fact = _fact([("inv1", ["HOT", "SELLER"]), ("inv2", ["HOT", "SELLER"])])
    descriptions = {"HOT": "POPULAR THING", "SELLER": "POPULAR THING OTHER"}
    # Six cold-start products, three pairs of identical text.
    for i in range(3):
        descriptions[f"C{i}A"] = f"RED VINTAGE TIN {i}"
        descriptions[f"C{i}B"] = f"RED VINTAGE TIN {i}"
    dim = _dim(descriptions)

    reference = _published({"fact_sales": fact, "dim_product": dim})
    content = [row for row in reference if row[0] == "content_tfidf"]
    assert content, "the fixture produced no content recommendations to compare"

    for seed in (1, 2, 3, 4, 5):
        rng = np.random.default_rng(seed)
        shuffled = {
            "fact_sales": fact,
            "dim_product": dim.iloc[rng.permutation(len(dim))].reset_index(drop=True),
        }
        assert _published(shuffled) == reference, (
            f"seed {seed} recommended different identically-described products"
        )


def test_the_candidate_width_is_wider_than_what_is_published():
    """The constant exists to outrun ties at the truncation boundary; a value
    at or below top_n would make the sort that follows it pointless."""
    assert recommender.CANDIDATE_WIDTH > CFG["recommend"]["top_n"] * 4
