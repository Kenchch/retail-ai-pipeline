"""Two products with equal evidence must not be ordered by luck.

Both ranking paths truncate: co-purchase keeps the top `top_n` rules by lift,
and the content fallback keeps the `top_n` nearest descriptions. Where the sort
key ties, something has to break it.

The two paths were in different states, and it is worth being precise about
which:

**The content fallback was order-dependent.** Permuting the catalogue changed
the published recommendations for 15.7% of cold-start products, because 22.1%
of neighbour lists contain a distance tie and 216 contain more than one
distance of exactly zero. End to end, with the fact table shuffled too,
986-1,006 of 17,083 published rows changed, about 5.8%.

Sorting each shortlist fixed most of it and left 0.19%, which a wider fetch
could not close: no fixed width outruns every tie group, and CI proved the
point by producing 7 more hits on a GitHub runner than on the machine the
reports came from. `_tie_complete_neighbours` widens per product until the
cut-off is unambiguous, and the residual is now 0.00% -- zero of 17,083 rows,
under every permutation tried.

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


def test_a_tie_group_wider_than_the_first_fetch_is_still_resolved(monkeypatch):
    """The case a fixed candidate width cannot reach.

    Every product here carries the same description, so every pairwise cosine
    distance is exactly zero and the tie group is the whole catalogue. With the
    fetch pinned to a width smaller than that group, `kneighbors` returns an
    arbitrary subset -- and which subset it is depends on the machine's BLAS,
    which is how this reached CI as a real difference rather than a theory.
    """
    monkeypatch.setattr(recommender, "CANDIDATE_WIDTH", 4)

    fact = _fact([("inv1", ["HOT", "SELLER"]), ("inv2", ["HOT", "SELLER"])])
    descriptions = {"HOT": "POPULAR THING", "SELLER": "POPULAR THING OTHER"}
    for i in range(30):
        descriptions[f"TIE{i:02d}"] = "IDENTICAL VINTAGE TIN BOX"
    dim = _dim(descriptions)

    reference = _published({"fact_sales": fact, "dim_product": dim})
    content = [row for row in reference if row[0] == "content_tfidf"]
    assert content, "the fixture produced no content recommendations"

    for seed in (1, 2, 3, 4, 5, 6, 7):
        rng = np.random.default_rng(seed)
        shuffled = {
            "fact_sales": fact,
            "dim_product": dim.iloc[rng.permutation(len(dim))].reset_index(drop=True),
        }
        assert _published(shuffled) == reference, (
            f"seed {seed} chose a different member of a 30-wide tie group"
        )


def test_widening_stops_at_the_catalogue_rather_than_looping(monkeypatch):
    """The loop's termination condition, asserted rather than assumed.

    Every candidate is equidistant, so the cut-off is ambiguous at any width
    below the whole catalogue -- which is exactly the shape that would spin
    forever if the loop only widened on ambiguity and never bounded itself.
    """
    monkeypatch.setattr(recommender, "CANDIDATE_WIDTH", 2)

    fact = _fact([("inv1", ["HOT", "SELLER"]), ("inv2", ["HOT", "SELLER"])])
    descriptions = {"HOT": "POPULAR THING", "SELLER": "POPULAR THING OTHER"}
    for i in range(12):
        descriptions[f"SAME{i:02d}"] = "ONE DESCRIPTION FOR ALL OF THEM"
    dim = _dim(descriptions)

    published = _published({"fact_sales": fact, "dim_product": dim})
    assert [row for row in published if row[0] == "content_tfidf"]
