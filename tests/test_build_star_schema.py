"""Regression tests for the Power BI semantic-model builders."""

import pandas as pd
import pytest

from bi import build_star_schema as star


def test_module_import_does_not_resolve_the_published_manifest():
    assert star.OUT.name == "model"


def test_invalid_average_price_uses_not_applicable_band():
    products = pd.DataFrame(
        {
            "stock_code": ["NEGATIVE", "MISSING", "VALID"],
            "description": ["negative", "missing", "valid"],
            "avg_unit_price": [-1.0, float("nan"), 1.5],
        }
    )
    fact = pd.DataFrame(
        {
            "stock_code": ["NEGATIVE", "MISSING", "VALID"],
            "date_key": pd.to_datetime(["2026-01-01"] * 3),
        }
    )
    quarantine = pd.DataFrame({"stock_code": pd.Series(dtype="object")})

    result = star.build_dim_product(products, quarantine, fact)

    bands = result.set_index("StockCode")["PriceBand"]
    assert bands["NEGATIVE"] == "0. Not applicable"
    assert bands["MISSING"] == "0. Not applicable"
    assert bands["VALID"] == "2. GBP 1 - 2.50"
    assert "nan" not in set(bands)


def test_duplicate_dimension_key_raises_without_assert():
    dim_date = pd.DataFrame({"Date": pd.to_datetime(["2026-01-01"])})
    duplicate = pd.DataFrame({"ProductKey": [1, 1]})

    with pytest.raises(ValueError, match=r"dim_product\.ProductKey is not unique"):
        star._validate_dimensions((("dim_product", duplicate, "ProductKey"),), dim_date)


def test_non_contiguous_date_dimension_raises_without_assert():
    dim_date = pd.DataFrame({"Date": pd.to_datetime(["2026-01-01", "2026-01-03"])})

    with pytest.raises(ValueError, match="dim_date is not contiguous"):
        star._validate_dimensions((("dim_date", dim_date, "Date"),), dim_date)
