"""Tests for the parts that fail silently.

A pipeline breaks loudly when the code is wrong and quietly when the *data* is
unusual, so these concentrate on the second case: a rule that stops firing, a
team that stops appearing, a week with nothing in it. Nothing here crashes if it
regresses — bad rows just start flowing into the warehouse, which is why the
tests exist.
"""

import pandas as pd
import pytest

from retail_pipeline.adoption import headline_metrics, team_metrics, weekly_metrics
from retail_pipeline.pipeline import CHECKS, check_quality, extract, load_config, transform
from retail_pipeline.recommend import COLUMNS, recommend

HEADER = ("InvoiceNo,StockCode,Description,Quantity,InvoiceDate,"
          "UnitPrice,CustomerID,Country\n")


@pytest.fixture()
def cfg():
    c = load_config()
    c["quality"]["max_quarantine_rate"] = 1.0   # the sample below is mostly broken
    return c


@pytest.fixture()
def sample():
    """Nine rows, each breaking exactly one known rule."""
    rows = [
        ("536365", "85123A", "WHITE MUG", 6, "2010-12-01 08:26", 2.55, 17850, "UK"),   # clean
        ("536365", "71053", "LANTERN", 2, "2010-12-01 08:26", 3.39, 17850, "UK"),      # clean
        ("536365", "85123A", "WHITE MUG", 6, "2010-12-01 08:26", 2.55, 17850, "UK"),   # duplicate
        ("C536379", "22633", "GAME SET", 1, "2010-12-01 09:41", 4.95, 14527, "UK"),    # cancelled
        ("536380", "22960", "JAM JAR", -3, "2010-12-01 10:02", 4.25, 17850, "UK"),     # negative
        ("536381", "22961", "TEA SET", 1, "2010-12-01 10:05", 0.00, 17850, "UK"),      # zero price
        ("536382", "22962", "RARE", 1, "2010-12-01 10:07", 5000.0, 17850, "UK"),       # outlier
        ("536383", "POST", "POSTAGE", 1, "2010-12-01 10:09", 18.0, 12583, "FR"),       # not a product
        ("536384", "22963", None, 4, "2010-12-01 10:11", 1.25, None, "UK"),            # guest, no desc
    ]
    df = pd.DataFrame(rows, columns=["invoice_no", "stock_code", "description", "quantity",
                                     "invoice_ts", "unit_price", "customer_id", "country"])
    df["invoice_ts"] = pd.to_datetime(df["invoice_ts"])
    for col in ("invoice_no", "stock_code", "description"):
        df[col] = df[col].astype("string")
    df["customer_id"] = df["customer_id"].astype("Int64")
    return df


@pytest.fixture()
def events():
    """Two users, one week: U1 views 3 / applies 1, U2 views 1 / rates it 5."""
    rows = [
        ("2026-04-06 10:00", "U1", "Category Management", "view", "S1", None),
        ("2026-04-06 10:05", "U1", "Category Management", "view", "S2", None),
        ("2026-04-06 10:12", "U1", "Category Management", "apply", "S2", None),
        ("2026-04-06 14:00", "U1", "Category Management", "view", "S3", None),
        ("2026-04-07 09:00", "U2", "Merchandising", "view", "S1", None),
        ("2026-04-07 09:10", "U2", "Merchandising", "feedback", None, 5),
    ]
    df = pd.DataFrame(rows, columns=["event_ts", "user_id", "team", "event_type",
                                     "stock_code", "feedback_score"])
    df["event_ts"] = pd.to_datetime(df["event_ts"])
    df["week_start"] = df["event_ts"].dt.to_period("W-SUN").dt.start_time
    return df


# --- data quality ---------------------------------------------------------- #

def test_every_rule_fires_exactly_once(sample, cfg):
    _, _, results = check_quality(sample, cfg)
    assert dict(zip(results["check"], results["failed_rows"])) == {
        "duplicate_line_items": 1, "missing_invoice_key": 0, "cancelled_invoice": 1,
        "non_positive_quantity": 1, "non_positive_price": 1, "price_outlier": 1,
        "non_product_stock_code": 1, "missing_description": 1, "missing_customer_id": 1,
    }
    assert len({c.name for c in CHECKS}) == len(CHECKS)


def test_quarantine_keeps_the_reasons_and_the_flagged_rows(sample, cfg):
    clean, quarantine, _ = check_quality(sample, cfg)
    assert len(clean) == 3 and len(quarantine) == 6
    assert "cancelled_invoice" in dict(zip(quarantine["invoice_no"],
                                           quarantine["reasons"]))["C536379"]
    # A guest checkout with a blank description is still a real sale.
    assert "536384" in set(clean["invoice_no"])


def test_the_gate_stops_the_run(sample, cfg):
    cfg["quality"]["max_quarantine_rate"] = 0.01
    with pytest.raises(ValueError, match="refusing to load"):
        check_quality(sample, cfg)


# --- star schema ----------------------------------------------------------- #

def test_star_schema_keys_and_referential_integrity(sample, cfg):
    clean, _, _ = check_quality(sample, cfg)
    t = transform(clean)
    assert t["dim_product"]["stock_code"].is_unique
    assert set(t["fact_sales"]["stock_code"]) <= set(t["dim_product"]["stock_code"])
    assert set(t["fact_sales"]["date_key"]) <= set(t["dim_date"]["date_key"])
    assert "description" not in t["fact_sales"].columns     # attributes live in dimensions


def test_dim_date_is_a_continuous_calendar(sample, cfg):
    """Built from observed dates it would have holes wherever the shop was shut,
    and BI grouping by week would silently get six-day weeks."""
    clean, _, _ = check_quality(sample, cfg)
    clean.loc[clean.index[-1], "invoice_ts"] += pd.Timedelta(days=7)
    dim = transform(clean)["dim_date"]
    assert list(dim["date_key"]) == list(
        pd.date_range(dim["date_key"].min(), dim["date_key"].max(), freq="D"))
    assert (~dim["has_sales"]).any()


def test_a_changed_source_schema_names_itself(cfg, tmp_path):
    src = tmp_path / "s.csv"
    src.write_text("InvoiceNo,StockCode,Quantity\n1,A,2\n")
    with pytest.raises(ValueError, match="missing expected column"):
        extract(cfg, source=src)


# --- recommendations ------------------------------------------------------- #

def _basket_tables():
    """20 baskets of A+B, 20 of C+D. A and B never appear apart."""
    rows = [(f"I{i}", p) for i in range(20) for p in "AB"]
    rows += [(f"J{i}", p) for i in range(20) for p in "CD"]
    fact = pd.DataFrame(rows, columns=["invoice_no", "stock_code"])
    dim = pd.DataFrame({"stock_code": list("ABCD"),
                        "description": ["RED MUG LARGE", "RED MUG SMALL",
                                        "BLUE PLATE ROUND", "BLUE PLATE SQUARE"]})
    return {"fact_sales": fact, "dim_product": dim}


def test_lift_is_computed_correctly(cfg):
    cfg["recommend"]["min_support_count"] = 5
    recs = recommend(_basket_tables(), cfg)
    ab = recs[(recs["stock_code"] == "A") & (recs["recommended_stock_code"] == "B")]
    # A and B share 20 of 40 baskets and never appear apart:
    # confidence = 1.0, support = 0.5, lift = 1.0 / 0.5 = 2.0
    assert ab["confidence"].iat[0] == 1.0 and ab["support"].iat[0] == 0.5
    assert round(ab["lift"].iat[0], 6) == 2.0
    # Products from disjoint baskets never produce a rule.
    assert recs[(recs["stock_code"] == "A")
                & (recs["recommended_stock_code"] == "C")
                & (recs["method"] == "co_purchase")].empty


def test_an_empty_result_still_has_its_schema(cfg):
    """A zero-column frame is not "no recommendations" - it is a shape no
    consumer can read, and it makes SQLite fail on `CREATE TABLE x ()`."""
    cfg["recommend"]["min_support_count"] = 10_000
    tables = _basket_tables()
    tables["dim_product"]["description"] = "UNKNOWN"     # kills the fallback too
    recs = recommend(tables, cfg)
    assert recs.empty and list(recs.columns) == COLUMNS


# --- adoption -------------------------------------------------------------- #

def test_reach_divides_by_the_roster_not_by_who_showed_up(events, cfg):
    cfg["adoption"]["roster"] = {"Category Management": 1, "Merchandising": 4}
    cfg["adoption"]["licensed_users"] = 5
    t = team_metrics(events, cfg).set_index("team")
    assert t.loc["Category Management", "reach_pct"] == 100.0
    assert t.loc["Merchandising", "reach_pct"] == 25.0      # 1 active of 4 licensed


def test_a_team_with_zero_adoption_shows_zero_rather_than_vanishing(events, cfg):
    """Deriving the team list from the event log drops exactly the team the
    report exists to surface."""
    cfg["adoption"]["roster"] = {"Category Management": 1, "Ghost Team": 5}
    cfg["adoption"]["licensed_users"] = 6
    t = team_metrics(events, cfg).set_index("team")
    assert "Ghost Team" in t.index and t.loc["Ghost Team", "reach_pct"] == 0.0


def test_a_week_with_no_activity_stays_in_the_trend(cfg):
    """Dropping it closes the gap and shifts every later week one position left."""
    rows = [("2026-04-06 10:00", "U1", "T", "view", "S1", None),
            ("2026-04-06 10:01", "U1", "T", "apply", "S1", None),
            ("2026-04-20 10:00", "U1", "T", "view", "S1", None)]   # week 2 empty
    df = pd.DataFrame(rows, columns=["event_ts", "user_id", "team", "event_type",
                                     "stock_code", "feedback_score"])
    df["event_ts"] = pd.to_datetime(df["event_ts"])
    w = weekly_metrics(df, cfg)
    assert list(w["week_no"]) == [1, 2, 3]
    assert w.iloc[1]["active_users"] == 0 and w.iloc[1]["reach_pct"] == 0.0
    assert w.iloc[2]["week_start"] == pd.Timestamp("2026-04-20")
    # 0 applies out of 0 views is undefined, not a 0% action rate.
    assert pd.isna(w.iloc[1]["action_rate_pct"])
    assert w.iloc[0]["action_rate_pct"] == 100.0


def test_a_metric_with_no_data_is_not_a_failing_metric(events, cfg):
    """Nobody answering the survey is not everybody being unhappy."""
    h = headline_metrics(events[events["event_type"] != "feedback"], cfg).set_index("metric")
    assert pd.isna(h.loc["csat", "value"]) and h.loc["csat", "status"] == "no data"


def test_no_usage_at_all_reports_zeros_rather_than_failing(cfg):
    empty = pd.DataFrame(columns=["event_ts", "user_id", "team", "event_type",
                                  "stock_code", "feedback_score", "week_start"])
    empty["event_ts"] = pd.to_datetime(empty["event_ts"])
    h = headline_metrics(empty, cfg).set_index("metric")
    t = team_metrics(empty, cfg)
    assert h.loc["reach_pct", "value"] == 0.0
    assert len(t) == len(cfg["adoption"]["roster"])   # every team still listed
    assert weekly_metrics(empty, cfg).empty
