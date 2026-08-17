"""Build the Power BI semantic layer from the pipeline's warehouse tables.

    python -m bi.build_star_schema

Reads `data/processed/*.parquet` (produced by `retail_pipeline.pipeline`) and
writes a BI-ready dimensional model to `bi/model/*.csv`.

The pipeline already publishes a star schema. It is a *warehouse* star, not a
*semantic* one, and the difference is the whole point of this module:

* **Surrogate keys.** The warehouse joins on natural keys. A semantic model
  needs a stable integer key so an unknown member can exist at all (see below)
  and so a renamed stock code does not orphan history.
* **Unknown members.** 25.1% of sales rows are guest checkout with no customer
  id, and 3,069 quarantined rows carry 155 distinct stock codes that never
  became products (POST, BANK CHARGES). A null foreign key produces a blank row in the
  dimension, which silently drops from every slicer. Both get an explicit
  member instead: -1 Guest, -1 Non-product.
* **A calendar that supports time intelligence.** The warehouse date dimension
  spans only the dates present in the data (2010-12-01 to 2011-12-09).
  SAMEPERIODLASTYEAR over that returns blank for most of 2011 because 2010 is
  eleven months short. The semantic calendar runs whole years, 2010-01-01 to
  2011-12-31, and carries the sort columns Power BI needs to order month names
  chronologically rather than alphabetically.
* **No aggregates in dimensions.** `dim_product.revenue` and
  `dim_customer.total_revenue` are facts sitting in dimension tables. Left
  there they are additive down one path and not the other, and a user who
  drags `total_revenue` into a visual filtered to March gets the all-time
  number. They are removed; measures compute them from the fact.
* **A second fact.** `fact_quarantine` shares dim_date and dim_product with
  `fact_sales`, so one report can answer "what sold" and "what we rejected and
  why" off the same slicers. The reason list is many-to-many, so it resolves
  through a bridge rather than a comma-separated string nobody can filter on.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from retail_pipeline.pipeline import CHECKS

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "data" / "processed"
OUT = Path(__file__).resolve().parent / "model"

UNKNOWN_KEY = -1

# UK / Europe / Rest of world. The retailer is UK-based, so "domestic vs export"
# is the split a merchandiser actually asks for; continent alone would not
# separate the home market from the rest of Europe.
EU_EEA = {
    "Austria",
    "Belgium",
    "Channel Islands",
    "Cyprus",
    "Czech Republic",
    "Denmark",
    "EIRE",
    "European Community",
    "Finland",
    "France",
    "Germany",
    "Greece",
    "Iceland",
    "Italy",
    "Lithuania",
    "Malta",
    "Netherlands",
    "Norway",
    "Poland",
    "Portugal",
    "Spain",
    "Sweden",
    "Switzerland",
}
DOMESTIC = {"United Kingdom"}

PRICE_BANDS = [
    (0.00, 1.00, "1. Under GBP 1"),
    (1.00, 2.50, "2. GBP 1 - 2.50"),
    (2.50, 5.00, "3. GBP 2.50 - 5"),
    (5.00, 10.00, "4. GBP 5 - 10"),
    (10.00, np.inf, "5. GBP 10+"),
]


# --------------------------------------------------------------------------- #
# Dimensions
# --------------------------------------------------------------------------- #


def build_dim_date(
    first: pd.Timestamp, last: pd.Timestamp, traded: set[pd.Timestamp]
) -> pd.DataFrame:
    """Contiguous whole-year calendar.

    Whole years, not the observed range: time intelligence compares a date to
    the same date last year, and a calendar that starts in December 2010 has
    no last year for eleven months of 2011. Every DAX date function also
    requires the table to be contiguous - one missing day and Power BI refuses
    to mark it as a date table.
    """
    start = pd.Timestamp(year=first.year, month=1, day=1)
    end = pd.Timestamp(year=last.year, month=12, day=31)
    d = pd.DataFrame({"Date": pd.date_range(start, end, freq="D")})

    d["DateKey"] = d["Date"].dt.strftime("%Y%m%d").astype(int)
    d["Year"] = d["Date"].dt.year
    d["QuarterNo"] = d["Date"].dt.quarter
    d["Quarter"] = d["Year"].astype(str) + " Q" + d["QuarterNo"].astype(str)
    d["MonthNo"] = d["Date"].dt.month
    d["MonthName"] = d["Date"].dt.strftime("%b")
    d["YearMonth"] = d["Date"].dt.strftime("%Y-%m")
    # Sort columns. Without them a month slicer reads Apr, Aug, Dec, Feb...
    d["MonthNameSort"] = d["MonthNo"]
    d["YearMonthSort"] = d["Year"] * 100 + d["MonthNo"]
    d["DayOfMonth"] = d["Date"].dt.day
    d["DayName"] = d["Date"].dt.strftime("%a")
    d["DayOfWeekNo"] = d["Date"].dt.dayofweek + 1  # Mon = 1
    d["DayNameSort"] = d["DayOfWeekNo"]
    d["IsWeekend"] = d["DayOfWeekNo"] >= 6
    d["ISOWeek"] = d["Date"].dt.isocalendar().week.astype(int)
    # This retailer is shut on Saturdays and most of the Christmas period.
    # "Did we trade" is an observed fact about the business, not a property of
    # the calendar, so it is a flag on the row rather than a filter on it.
    d["IsTradingDay"] = d["Date"].isin(traded)
    return d


def build_dim_country(countries: pd.Series) -> pd.DataFrame:
    names = sorted(set(countries.dropna().astype(str)))
    rows = []
    for i, name in enumerate(names, start=1):
        if name in DOMESTIC:
            region = "1. United Kingdom"
        elif name in EU_EEA:
            region = "2. Europe"
        elif name == "Unspecified":
            region = "4. Unspecified"
        else:
            region = "3. Rest of world"
        rows.append(
            {
                "CountryKey": i,
                "Country": name,
                "Region": region,
                "IsDomestic": name in DOMESTIC,
            }
        )
    df = pd.DataFrame(rows)
    return pd.concat(
        [
            pd.DataFrame(
                [
                    {
                        "CountryKey": UNKNOWN_KEY,
                        "Country": "Unknown",
                        "Region": "4. Unspecified",
                        "IsDomestic": False,
                    }
                ]
            ),
            df,
        ],
        ignore_index=True,
    )


def build_dim_product(
    dim_product: pd.DataFrame, quarantine: pd.DataFrame, fact: pd.DataFrame
) -> pd.DataFrame:
    """Sellable products plus a non-product member for the quarantine fact.

    Aggregates (revenue, units_sold, n_invoices) are dropped: they belong to
    measures. First/last sold survive because they are dates, not additive
    quantities - a semantic model can safely expose them as attributes.
    """
    p = dim_product[["stock_code", "description", "avg_unit_price"]].copy()
    p = p.sort_values("stock_code").reset_index(drop=True)
    p.insert(0, "ProductKey", np.arange(1, len(p) + 1))

    bands = pd.cut(
        p["avg_unit_price"],
        bins=[b[0] for b in PRICE_BANDS] + [np.inf],
        labels=[b[2] for b in PRICE_BANDS],
        right=False,
        include_lowest=True,
    )
    p["PriceBand"] = bands.astype(str)

    span = (
        fact.groupby("stock_code")["date_key"]
        .agg(FirstSoldDate="min", LastSoldDate="max")
        .reset_index()
    )
    p = p.merge(span, on="stock_code", how="left")

    p = p.rename(
        columns={
            "stock_code": "StockCode",
            "description": "Description",
            "avg_unit_price": "AvgUnitPrice",
        }
    )
    p["IsSellable"] = True

    # Stock codes that only ever appear in quarantined rows - POST, BANK
    # CHARGES, M. They are real rows and the quality report has to slice by
    # them, but they never became products, so they collapse to one member
    # rather than polluting the catalogue with codes nothing can be sold under.
    orphan = sorted(
        set(quarantine["stock_code"].dropna().astype(str)) - set(p["StockCode"])
    )
    unknown = pd.DataFrame(
        [
            {
                "ProductKey": UNKNOWN_KEY,
                "StockCode": "(non-product)",
                "Description": "Non-product stock code (postage, fees, adjustments)",
                "AvgUnitPrice": np.nan,
                "PriceBand": "0. Not applicable",
                "FirstSoldDate": pd.NaT,
                "LastSoldDate": pd.NaT,
                "IsSellable": False,
            }
        ]
    )
    print(
        f"  dim_product: {len(p):,} sellable + 1 non-product member "
        f"(absorbing {len(orphan):,} codes)"
    )
    return pd.concat([unknown, p], ignore_index=True)


def build_dim_customer(
    dim_customer: pd.DataFrame, country: pd.DataFrame
) -> pd.DataFrame:
    """Known customers plus an explicit Guest member.

    131,418 sales rows have no customer id. Leaving the key null makes Power BI
    invent a blank dimension row: it disappears from slicers, so every
    customer-sliced total quietly excludes a quarter of revenue and nothing in
    the report says so. An explicit Guest member keeps the total honest and
    lets a user filter guest traffic in or out deliberately.

    Segment is a Type-1 snapshot attribute recalculated every load, not a slowly
    changing dimension: it answers "who is a top customer today", and the
    history of how someone was banded is not a question this model is asked.
    """
    c = dim_customer.copy()
    c = c.sort_values("customer_id").reset_index(drop=True)
    c.insert(0, "CustomerKey", np.arange(1, len(c) + 1))

    q = c["total_revenue"].rank(pct=True)
    c["CustomerSegment"] = np.select(
        [q > 0.90, q > 0.70, q > 0.40],
        ["1. Top 10%", "2. Next 20%", "3. Middle 30%"],
        default="4. Bottom 40%",
    )

    c = c.merge(
        country[["CountryKey", "Country"]],
        left_on="country",
        right_on="Country",
        how="left",
    )
    c["CountryKey"] = c["CountryKey"].fillna(UNKNOWN_KEY).astype(int)

    c = c.rename(
        columns={
            "customer_id": "CustomerID",
            "first_order": "FirstOrderDate",
            "last_order": "LastOrderDate",
        }
    )
    c["CustomerID"] = c["CustomerID"].astype(str)
    c["IsGuest"] = False
    c = c[
        [
            "CustomerKey",
            "CustomerID",
            "CountryKey",
            "Country",
            "CustomerSegment",
            "FirstOrderDate",
            "LastOrderDate",
            "IsGuest",
        ]
    ]

    guest = pd.DataFrame(
        [
            {
                "CustomerKey": UNKNOWN_KEY,
                "CustomerID": "(guest)",
                "CountryKey": UNKNOWN_KEY,
                "Country": "Unknown",
                "CustomerSegment": "5. Guest checkout",
                "FirstOrderDate": pd.NaT,
                "LastOrderDate": pd.NaT,
                "IsGuest": True,
            }
        ]
    )
    return pd.concat([guest, c], ignore_index=True)


def build_dim_rule() -> pd.DataFrame:
    """One row per quality rule, sourced from the pipeline's own CHECKS list.

    Typed out separately it would drift the first time a rule is renamed; read
    from the code it cannot.
    """
    return pd.DataFrame(
        [
            {
                "RuleKey": i,
                "RuleName": c.name,
                "RuleLabel": c.name.replace("_", " ").capitalize(),
                "QualityDimension": c.dimension.capitalize(),
                "Action": "Quarantine" if c.blocking else "Flag and keep",
                "Rationale": c.why,
            }
            for i, c in enumerate(CHECKS, start=1)
        ]
    )


# --------------------------------------------------------------------------- #
# Facts
# --------------------------------------------------------------------------- #


def build_fact_sales(
    fact: pd.DataFrame,
    product: pd.DataFrame,
    customer: pd.DataFrame,
    country: pd.DataFrame,
) -> pd.DataFrame:
    f = fact.copy()
    f["customer_id"] = (
        f["customer_id"].astype("Int64").astype(str).replace("<NA>", "(guest)")
    )

    f = f.merge(
        product[["ProductKey", "StockCode"]],
        left_on="stock_code",
        right_on="StockCode",
        how="left",
    )
    f = f.merge(
        customer[["CustomerKey", "CustomerID"]],
        left_on="customer_id",
        right_on="CustomerID",
        how="left",
    )
    f = f.merge(
        country[["CountryKey", "Country"]],
        left_on="country",
        right_on="Country",
        how="left",
    )

    for col in ("ProductKey", "CustomerKey", "CountryKey"):
        f[col] = f[col].fillna(UNKNOWN_KEY).astype(int)

    out = pd.DataFrame(
        {
            # Degenerate dimension: invoice number has no attributes of its own, so
            # it lives on the fact rather than in a one-column dimension table.
            "InvoiceNo": f["invoice_no"].astype(str),
            "Date": f["date_key"].dt.normalize(),
            "ProductKey": f["ProductKey"],
            "CustomerKey": f["CustomerKey"],
            "CountryKey": f["CountryKey"],
            # Int64 for symmetry with fact_quarantine; clean rows never hold
            # a null quantity, since that is a blocking rule.
            "Quantity": f["quantity"].astype("Int64"),
            "UnitPrice": f["unit_price"].round(4),
            "Revenue": f["revenue"].round(4),
        }
    )
    assert (out[["ProductKey", "CustomerKey", "CountryKey"]] != 0).all().all()
    return out


def build_fact_quarantine(
    q: pd.DataFrame, product: pd.DataFrame, country: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """The rejected rows, as a fact, plus the bridge to the rules they broke.

    A row can break several rules at once, so rule is a many-to-many
    relationship, not an attribute. Stored as the pipeline's comma-separated
    string it is unfilterable - "show me price outliers" would need a text
    search and would double-count nothing correctly. The bridge makes the
    grain explicit: one row per rejected row per rule broken.
    """
    q = q.reset_index(drop=True).copy()
    q.insert(0, "QuarantineKey", np.arange(1, len(q) + 1))

    q = q.merge(
        product[["ProductKey", "StockCode"]],
        left_on="stock_code",
        right_on="StockCode",
        how="left",
    )
    q = q.merge(
        country[["CountryKey", "Country"]],
        left_on="country",
        right_on="Country",
        how="left",
    )
    q["ProductKey"] = q["ProductKey"].fillna(UNKNOWN_KEY).astype(int)
    q["CountryKey"] = q["CountryKey"].fillna(UNKNOWN_KEY).astype(int)

    fact = pd.DataFrame(
        {
            "QuarantineKey": q["QuarantineKey"],
            "InvoiceNo": q["invoice_no"].astype(str),
            "Date": q["invoice_ts"].dt.normalize(),
            "ProductKey": q["ProductKey"],
            "CountryKey": q["CountryKey"],
            # Int64, not int: a quarantined row can carry a null quantity - that is
            # exactly what `non_positive_quantity` fires on - and .astype(int)
            # raises IntCastingNaNError on it. This extract happens to have none,
            # so the crash is latent rather than absent.
            "Quantity": q["quantity"].astype("Int64"),
            "UnitPrice": q["unit_price"].round(4),
            # Not "Revenue": these rows were rejected, so this is the money that
            # would have been booked had the rules not fired. Naming it Revenue
            # invites someone to add it to the sales measure.
            "RejectedValue": (q["quantity"] * q["unit_price"]).round(4),
        }
    )

    rules = build_dim_rule().set_index("RuleName")["RuleKey"].to_dict()
    pairs = [
        (k, rules[name])
        for k, reasons in zip(q["QuarantineKey"], q["reasons"])
        for name in str(reasons).split(",")
        if name in rules
    ]
    bridge = pd.DataFrame(pairs, columns=["QuarantineKey", "RuleKey"])
    return fact, bridge


def build_security(country: pd.DataFrame) -> pd.DataFrame:
    """Sample row-level-security mapping: user -> the countries they may see.

    SAMPLE DATA. The addresses are placeholders; in production this table is a
    view over the HR or entitlements system, refreshed with the model. It is a
    table rather than a static role filter because a static filter needs a new
    role per market and a redeploy per staff change.

    One member is deliberately given the "*" wildcard: the pattern is only
    complete if head office can see everything through the same mechanism,
    rather than through a second role that bypasses it.
    """
    rows = [
        ("uk.analyst@example.com", "United Kingdom"),
        ("eire.analyst@example.com", "EIRE"),
        ("dach.analyst@example.com", "Germany"),
        ("dach.analyst@example.com", "Austria"),
        ("dach.analyst@example.com", "Switzerland"),
        ("emea.analyst@example.com", "France"),
        ("emea.analyst@example.com", "Spain"),
        ("emea.analyst@example.com", "Netherlands"),
        ("emea.analyst@example.com", "Belgium"),
        ("groupbi@example.com", "*"),
    ]
    lookup = country.set_index("Country")["CountryKey"].to_dict()
    return pd.DataFrame(
        [
            {
                "UserEmail": u,
                "Country": c,
                "CountryKey": lookup.get(c, UNKNOWN_KEY) if c != "*" else 0,
            }
            for u, c in rows
        ]
    )


# --------------------------------------------------------------------------- #


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    print(f"Reading {SRC}")
    fact_src = pd.read_parquet(SRC / "fact_sales.parquet")
    prod_src = pd.read_parquet(SRC / "dim_product.parquet")
    cust_src = pd.read_parquet(SRC / "dim_customer.parquet")
    quar_src = pd.read_parquet(SRC / "quarantine.parquet")

    all_countries = pd.concat([fact_src["country"], quar_src["country"]])
    dim_country = build_dim_country(all_countries)
    dim_product = build_dim_product(prod_src, quar_src, fact_src)
    dim_customer = build_dim_customer(cust_src, dim_country)

    traded = set(fact_src["date_key"].dt.normalize().unique())
    spanned = pd.concat([fact_src["date_key"], quar_src["invoice_ts"]])
    dim_date = build_dim_date(spanned.min(), spanned.max(), traded)
    dim_rule = build_dim_rule()

    fact_sales = build_fact_sales(fact_src, dim_product, dim_customer, dim_country)
    fact_quar, bridge = build_fact_quarantine(quar_src, dim_product, dim_country)
    security = build_security(dim_country)

    tables = {
        "dim_date": dim_date,
        "dim_product": dim_product,
        "dim_customer": dim_customer,
        "dim_country": dim_country,
        "dim_quality_rule": dim_rule,
        "fact_sales": fact_sales,
        "fact_quarantine": fact_quar,
        "bridge_quarantine_rule": bridge,
        "security_user_country": security,
    }

    print("\nReferential integrity")
    checks = [
        ("fact_sales.ProductKey", fact_sales["ProductKey"], dim_product["ProductKey"]),
        (
            "fact_sales.CustomerKey",
            fact_sales["CustomerKey"],
            dim_customer["CustomerKey"],
        ),
        ("fact_sales.CountryKey", fact_sales["CountryKey"], dim_country["CountryKey"]),
        ("fact_sales.Date", fact_sales["Date"], dim_date["Date"]),
        (
            "fact_quarantine.ProductKey",
            fact_quar["ProductKey"],
            dim_product["ProductKey"],
        ),
        # Nulls excluded, not counted as orphans: a row rejected by
        # missing_invoice_key has no timestamp, because the timestamp is what
        # was wrong with it. Same rule the sibling project states for
        # quarantine.view_date - "no data" is not a broken key.
        ("fact_quarantine.Date", fact_quar["Date"].dropna(), dim_date["Date"]),
        ("bridge.RuleKey", bridge["RuleKey"], dim_rule["RuleKey"]),
        ("bridge.QuarantineKey", bridge["QuarantineKey"], fact_quar["QuarantineKey"]),
    ]
    for name, child, parent in checks:
        orphans = int((~child.isin(set(parent))).sum())
        print(f"  {'OK ' if orphans == 0 else 'FAIL'} {name:32s} {orphans} orphans")
        if orphans:
            raise SystemExit(
                f"{name} has {orphans} orphan keys - fix before publishing"
            )

    for name, df, key in (
        ("dim_product", dim_product, "ProductKey"),
        ("dim_customer", dim_customer, "CustomerKey"),
        ("dim_country", dim_country, "CountryKey"),
        ("dim_date", dim_date, "Date"),
        ("dim_quality_rule", dim_rule, "RuleKey"),
    ):
        assert df[key].is_unique, f"{name}.{key} is not unique"
    assert dim_date["Date"].diff().dropna().eq(pd.Timedelta(days=1)).all(), (
        "dim_date is not contiguous - Power BI will refuse to mark it as a date table"
    )

    print("\nWriting", OUT)
    for name, df in tables.items():
        path = OUT / f"{name}.csv"
        df.to_csv(path, index=False, date_format="%Y-%m-%d")
        print(
            f"  {name:24s} {len(df):>9,} rows x {df.shape[1]:>2} cols "
            f"({path.stat().st_size / 1024:,.0f} KB)"
        )

    total = fact_sales["Revenue"].sum()
    print(
        f"\nfact_sales reconciles to GBP {total:,.2f} over "
        f"{fact_sales['InvoiceNo'].nunique():,} invoices"
    )
    print(
        f"guest rows: {(fact_sales['CustomerKey'] == UNKNOWN_KEY).sum():,} "
        f"({(fact_sales['CustomerKey'] == UNKNOWN_KEY).mean():.1%})"
    )


if __name__ == "__main__":
    main()
