import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from retail_pipeline.config import Config  # noqa: E402


@pytest.fixture()
def cfg() -> Config:
    """The real config, with the quarantine gate opened up.

    The `sample` fixture below is deliberately two-thirds broken so that every
    rule has something to catch; that is far past the production ceiling, so
    the gate would fire on every test. Tests that care about the gate set the
    threshold themselves.
    """
    c = Config.load(ROOT / "config.yaml")
    c.quality["max_quarantine_rate"] = 1.0
    return c


@pytest.fixture()
def sample() -> pd.DataFrame:
    """A tiny hand-built frame where every row breaks exactly one known rule,
    so the expected outcome of each check is unambiguous."""
    rows = [
        # invoice, code,   description,  qty, ts,                  price, cust,  country
        ("536365", "85123A", "WHITE MUG",   6, "2010-12-01 08:26",  2.55, 17850, "United Kingdom"),  # clean
        ("536365", "71053",  "LANTERN",     2, "2010-12-01 08:26",  3.39, 17850, "United Kingdom"),  # clean
        ("536365", "85123A", "WHITE MUG",   6, "2010-12-01 08:26",  2.55, 17850, "United Kingdom"),  # duplicate
        ("C536379", "22633", "GAME SET",    1, "2010-12-01 09:41",  4.95, 14527, "United Kingdom"),  # cancellation
        ("536380", "22960",  "JAM MAKING", -3, "2010-12-01 10:02",  4.25, 17850, "United Kingdom"),  # negative qty
        ("536381", "22961",  "TEA SET",     1, "2010-12-01 10:05",  0.00, 17850, "United Kingdom"),  # zero price
        ("536382", "22962",  "RARE ITEM",   1, "2010-12-01 10:07", 5000.0, 17850, "United Kingdom"), # price outlier
        ("536383", "POST",   "POSTAGE",     1, "2010-12-01 10:09", 18.00, 12583, "France"),          # non-product
        ("536384", "22963",  None,          4, "2010-12-01 10:11",  1.25, None,  "United Kingdom"),  # no desc, guest
    ]
    df = pd.DataFrame(
        rows,
        columns=["invoice_no", "stock_code", "description", "quantity",
                 "invoice_ts", "unit_price", "customer_id", "country"],
    )
    df["invoice_ts"] = pd.to_datetime(df["invoice_ts"])
    df["invoice_no"] = df["invoice_no"].astype("string")
    df["stock_code"] = df["stock_code"].astype("string")
    df["description"] = df["description"].astype("string")
    df["customer_id"] = df["customer_id"].astype("Int64")
    return df
