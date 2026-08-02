"""Stage 3 - Transform / model.

Turns the cleaned transaction rows into a small star schema:

    fact_sales  --> dim_product   (stock_code)
                --> dim_customer  (customer_id)
                --> dim_date      (date_key)

A star schema is used rather than one wide table because the two downstream
consumers want different things: BI wants to slice sales by product / customer /
time, and the recommender only needs invoice + product. Both read the same
conformed dimensions, so a product renamed once is renamed everywhere.
"""

from __future__ import annotations

import pandas as pd

from .config import get_logger

log = get_logger(__name__)


def build_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    """One row per stock code. Descriptions vary between rows for the same code,
    so the most frequently used description wins."""
    desc = (
        df.dropna(subset=["description"])
        .groupby(["stock_code", "description"])
        .size()
        .reset_index(name="n")
        .sort_values(["stock_code", "n"], ascending=[True, False])
        .drop_duplicates("stock_code")[["stock_code", "description"]]
    )
    agg = df.groupby("stock_code").agg(
        n_line_items=("invoice_no", "size"),
        n_invoices=("invoice_no", "nunique"),
        units_sold=("quantity", "sum"),
        avg_unit_price=("unit_price", "mean"),
        first_sold=("invoice_ts", "min"),
        last_sold=("invoice_ts", "max"),
    ).reset_index()

    dim = agg.merge(desc, on="stock_code", how="left")
    dim["description"] = dim["description"].fillna("UNKNOWN")
    dim["avg_unit_price"] = dim["avg_unit_price"].round(4)
    log.info("dim_product: %s products", f"{len(dim):,}")
    return dim[
        ["stock_code", "description", "n_line_items", "n_invoices",
         "units_sold", "avg_unit_price", "first_sold", "last_sold"]
    ]


def build_dim_customer(df: pd.DataFrame) -> pd.DataFrame:
    """One row per identified customer, with the RFM-style facts a marketing or
    AI use case would ask for first."""
    known = df.dropna(subset=["customer_id"])
    dim = known.groupby("customer_id").agg(
        country=("country", lambda s: s.mode().iat[0] if not s.mode().empty else "Unknown"),
        n_invoices=("invoice_no", "nunique"),
        n_line_items=("invoice_no", "size"),
        total_revenue=("revenue", "sum"),
        first_order=("invoice_ts", "min"),
        last_order=("invoice_ts", "max"),
    ).reset_index()
    dim["total_revenue"] = dim["total_revenue"].round(2)
    dim["avg_order_value"] = (dim["total_revenue"] / dim["n_invoices"]).round(2)
    log.info("dim_customer: %s identified customers", f"{len(dim):,}")
    return dim


def build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    dates = pd.to_datetime(df["invoice_ts"].dt.date.unique())
    dim = pd.DataFrame({"date_key": dates}).sort_values("date_key").reset_index(drop=True)
    dim["year"] = dim["date_key"].dt.year
    dim["month"] = dim["date_key"].dt.month
    dim["day"] = dim["date_key"].dt.day
    dim["day_of_week"] = dim["date_key"].dt.day_name()
    dim["iso_week"] = dim["date_key"].dt.isocalendar().week.astype(int)
    dim["is_weekend"] = dim["date_key"].dt.dayofweek >= 5
    log.info("dim_date: %s days", f"{len(dim):,}")
    return dim


def build_fact_sales(df: pd.DataFrame) -> pd.DataFrame:
    """The fact table carries measures and foreign keys only - descriptive
    attributes live in the dimensions, so a description is stored once."""
    fact = df[
        ["invoice_no", "stock_code", "customer_id", "date_key", "invoice_ts",
         "quantity", "unit_price", "revenue", "country"]
    ].reset_index(drop=True)
    log.info(
        "fact_sales: %s line items | %s invoices | revenue %.0f",
        f"{len(fact):,}", f"{fact['invoice_no'].nunique():,}", fact["revenue"].sum(),
    )
    return fact


def transform(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Build the whole star schema from cleaned transactions."""
    enriched = df.copy()
    enriched["date_key"] = pd.to_datetime(enriched["invoice_ts"].dt.date)
    enriched["revenue"] = (enriched["quantity"] * enriched["unit_price"]).round(4)

    return {
        "fact_sales": build_fact_sales(enriched),
        "dim_product": build_dim_product(enriched),
        "dim_customer": build_dim_customer(enriched),
        "dim_date": build_dim_date(enriched),
    }
