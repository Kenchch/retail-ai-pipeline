"""Tests for the stages at the edges of the pipeline: extract and load.

These two had no tests at all until a coverage run made that visible, and the
gap was not harmless - both failed on realistic inputs with errors that named
the wrong thing. Read together with `test_degenerate_inputs.py`, the pattern is
consistent: the defects were never in the algorithms, they were in the seams.
"""

import sqlite3

import pandas as pd
import pytest

from retail_pipeline.extract import extract
from retail_pipeline.load import load, to_warehouse
from retail_pipeline.recommend import RECOMMENDATION_COLUMNS

HEADER = ("InvoiceNo,StockCode,Description,Quantity,InvoiceDate,"
          "UnitPrice,CustomerID,Country\n")
ROW = "536365,85123A,WHITE MUG,6,12/1/2010 8:26,2.55,17850,United Kingdom\n"


def _csv(tmp_path, text, name="src.csv"):
    p = tmp_path / name
    p.write_text(text, encoding="utf-8")
    return p


# --------------------------------------------------------------------------- #
# extract
# --------------------------------------------------------------------------- #

def test_extract_reads_and_standardises(cfg, tmp_path):
    df = extract(cfg, source=_csv(tmp_path, HEADER + ROW))
    assert list(df.columns) == list(cfg.extract["column_map"].values())
    assert df["invoice_ts"].iloc[0] == pd.Timestamp("2010-12-01 08:26")
    assert df["customer_id"].iloc[0] == 17850
    assert df["stock_code"].iloc[0] == "85123A"


def test_missing_source_columns_name_themselves(cfg, tmp_path):
    """The defect: a dropped column surfaced three lines later as
    `KeyError: 'invoice_ts'` - an internal name the reader has never seen, in
    the wrong file. A changed source schema should say it changed."""
    src = _csv(tmp_path, "InvoiceNo,StockCode,Quantity\n1,A,2\n")
    with pytest.raises(ValueError) as e:
        extract(cfg, source=src)
    assert "missing expected column" in str(e.value)
    assert "invoice_ts" in str(e.value)
    assert "column_map" in str(e.value)     # points at the fix


def test_a_different_edition_of_the_dataset_is_diagnosed(cfg, tmp_path):
    """Online Retail *II* names the same fields Invoice / Price / Customer ID.
    Pointing the pipeline at it is a plausible mistake and must be legible."""
    src = _csv(
        tmp_path,
        "Invoice,StockCode,Description,Quantity,InvoiceDate,Price,Customer ID,Country\n"
        "1,A,X,2,12/1/2010 8:26,1.5,17850,UK\n",
    )
    with pytest.raises(ValueError, match="schema has most likely changed"):
        extract(cfg, source=src)


def test_an_empty_source_file_is_rejected(cfg, tmp_path):
    with pytest.raises(ValueError, match="no rows"):
        extract(cfg, source=_csv(tmp_path, HEADER))


def test_a_wholly_unparseable_date_column_blames_the_format(cfg, tmp_path):
    """Every timestamp becoming NaT means the source date format changed - it
    does not mean the business had no sales."""
    bad = HEADER + "536365,85123A,WHITE MUG,6,2010-12-01T08:26:00,2.55,17850,UK\n"
    with pytest.raises(ValueError, match="date_format"):
        extract(cfg, source=_csv(tmp_path, bad))


def test_partially_unparseable_dates_warn_but_proceed(cfg, tmp_path, caplog):
    mixed = HEADER + ROW + "536366,71053,LANTERN,2,2010-13-45 99:99,3.39,17850,UK\n"
    df = extract(cfg, source=_csv(tmp_path, mixed))
    assert len(df) == 2
    assert df["invoice_ts"].isna().sum() == 1
    assert "did not match" in caplog.text


# --------------------------------------------------------------------------- #
# load
# --------------------------------------------------------------------------- #

def _cfg_to(tmp_path, cfg):
    cfg.paths["processed"] = tmp_path
    cfg.paths["warehouse"] = tmp_path / "retail.db"
    return cfg


def test_load_writes_both_layers_and_they_agree(cfg, tmp_path):
    cfg = _cfg_to(tmp_path, cfg)
    df = pd.DataFrame({"stock_code": ["A", "B"], "traded": [True, False], "n": [1, 2]})
    load({"dim_thing": df}, cfg)

    assert (tmp_path / "dim_thing.parquet").exists()
    parquet = pd.read_parquet(tmp_path / "dim_thing.parquet")
    with sqlite3.connect(cfg.paths["warehouse"]) as conn:
        rows = conn.execute("SELECT COUNT(*) FROM dim_thing").fetchone()[0]
    assert len(parquet) == rows == 2


def test_an_empty_but_shaped_table_round_trips(cfg, tmp_path):
    """A run that legitimately produces nothing must still publish the table,
    so downstream queries return zero rows instead of failing on a missing one."""
    cfg = _cfg_to(tmp_path, cfg)
    load({"recommendations": pd.DataFrame(columns=RECOMMENDATION_COLUMNS)}, cfg)

    with sqlite3.connect(cfg.paths["warehouse"]) as conn:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(recommendations)")]
    assert cols == RECOMMENDATION_COLUMNS


def test_a_table_with_no_columns_is_refused_by_name(cfg, tmp_path, caplog):
    """The defect: `CREATE TABLE x ()` is invalid SQL, so a zero-column frame
    crashed the load stage with `near ")": syntax error` - a message that names
    a bracket rather than the stage that produced a shapeless table."""
    cfg = _cfg_to(tmp_path, cfg)
    to_warehouse({"broken": pd.DataFrame()}, cfg)      # must not raise
    assert "has no columns" in caplog.text
    assert "broken" in caplog.text
