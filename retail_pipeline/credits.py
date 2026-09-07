"""Exact, chronological credit matching against quality-accepted sales."""

from collections import defaultdict

import numpy as np
import pandas as pd


def flag_reversed_sales(sales: pd.DataFrame, source: pd.DataFrame) -> pd.DataFrame:
    """Keep gross sales; flag each latest eligible sale consumed by a credit.

    Match customer/product/absolute quantity/price exactly, with sale time <=
    credit time. Credits are processed oldest first (source order breaks ties).
    At equal sale times the last accepted source row wins. Each side is used
    once; partial credits, unknown customers and invalid timestamps cannot match.
    Position IDs avoid relying on unique pandas indexes supplied by callers.
    """
    result = sales.copy()
    result["reversed_by_credit"] = False
    result["matched_credit_invoice"] = pd.Series(
        pd.NA, index=result.index, dtype="string"
    )
    cols = [
        "customer_id",
        "stock_code",
        "quantity",
        "unit_price",
        "invoice_ts",
        "invoice_no",
    ]
    credits = source.loc[
        source["invoice_no"].astype("string").str.startswith("C", na=False)
        & source["quantity"].lt(0),
        cols,
    ].copy()
    credits["quantity"] = -credits["quantity"]
    credits["kind"] = 1
    credits["position"] = np.arange(len(credits))
    accepted = sales[cols].copy()
    accepted["kind"] = 0
    accepted["position"] = np.arange(len(sales))
    events = pd.concat([accepted, credits], ignore_index=True).dropna(subset=cols)
    events = events.loc[(events["quantity"] > 0) & (events["unit_price"] > 0)]
    events = events.sort_values(["invoice_ts", "kind", "position"], kind="stable")
    available = defaultdict(list)
    flag_col = result.columns.get_loc("reversed_by_credit")
    credit_col = result.columns.get_loc("matched_credit_invoice")
    for row in events.itertuples(index=False):
        key = (row.customer_id, row.stock_code, row.quantity, row.unit_price)
        if row.kind == 0:
            available[key].append(row.position)
        elif available[key]:
            pos = available[key].pop()
            result.iat[pos, flag_col] = True
            result.iat[pos, credit_col] = row.invoice_no
    return result
