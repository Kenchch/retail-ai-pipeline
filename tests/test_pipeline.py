"""Tests for the parts that fail silently.

A pipeline breaks loudly when the code is wrong and quietly when the *data* is
unusual, so these concentrate on the second case: a rule that stops firing, a
team that stops appearing, a week with nothing in it. Nothing here crashes if it
regresses — bad rows just start flowing into the warehouse, which is why the
tests exist.
"""

import os
import pathlib

import pandas as pd
import pytest

from retail_pipeline import adoption as adoption_mod
from retail_pipeline import pipeline as P
from retail_pipeline.adoption import headline_metrics, team_metrics, weekly_metrics
from retail_pipeline.pipeline import (
    CHECKS,
    REPORT_NAMES,
    _input_fingerprint,
    check_quality,
    current_run_id,
    extract,
    finalize_reports,
    load_config,
    mark_published,
    published_reports,
    reports_dir,
    transform,
)
from retail_pipeline.recommend import COLUMNS, recommend

HEADER = (
    "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,"
    "UnitPrice,CustomerID,Country\n"
)


@pytest.fixture()
def cfg():
    c = load_config()
    c["quality"]["max_quarantine_rate"] = 1.0  # the sample below is mostly broken
    # The synthetic event frames here are dated around the launch, not around
    # config's pinned analysis date, so they measure back from their own newest
    # event. test_analysis_as_of_is_honoured_and_shared covers the pinned path.
    c["adoption"].pop("analysis_as_of", None)
    return c


@pytest.fixture()
def sample():
    """Nine rows, each breaking exactly one known rule."""
    rows = [
        (
            "536365",
            "85123A",
            "WHITE MUG",
            6,
            "2010-12-01 08:26",
            2.55,
            17850,
            "UK",
        ),  # clean
        (
            "536365",
            "71053",
            "LANTERN",
            2,
            "2010-12-01 08:26",
            3.39,
            17850,
            "UK",
        ),  # clean
        (
            "536365",
            "85123A",
            "WHITE MUG",
            6,
            "2010-12-01 08:26",
            2.55,
            17850,
            "UK",
        ),  # duplicate
        (
            "C536379",
            "22633",
            "GAME SET",
            1,
            "2010-12-01 09:41",
            4.95,
            14527,
            "UK",
        ),  # cancelled
        (
            "536380",
            "22960",
            "JAM JAR",
            -3,
            "2010-12-01 10:02",
            4.25,
            17850,
            "UK",
        ),  # negative
        (
            "536381",
            "22961",
            "TEA SET",
            1,
            "2010-12-01 10:05",
            0.00,
            17850,
            "UK",
        ),  # zero price
        (
            "536382",
            "22962",
            "RARE",
            1,
            "2010-12-01 10:07",
            5000.0,
            17850,
            "UK",
        ),  # outlier
        (
            "536383",
            "POST",
            "POSTAGE",
            1,
            "2010-12-01 10:09",
            18.0,
            12583,
            "FR",
        ),  # not a product
        (
            "536384",
            "22963",
            None,
            4,
            "2010-12-01 10:11",
            1.25,
            None,
            "UK",
        ),  # guest, no desc
    ]
    df = pd.DataFrame(
        rows,
        columns=[
            "invoice_no",
            "stock_code",
            "description",
            "quantity",
            "invoice_ts",
            "unit_price",
            "customer_id",
            "country",
        ],
    )
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
    df = pd.DataFrame(
        rows,
        columns=[
            "event_ts",
            "user_id",
            "team",
            "event_type",
            "stock_code",
            "feedback_score",
        ],
    )
    df["event_ts"] = pd.to_datetime(df["event_ts"])
    df["week_start"] = df["event_ts"].dt.to_period("W-SUN").dt.start_time
    return df


# --- data quality ---------------------------------------------------------- #


def test_every_rule_fires_exactly_once(sample, cfg):
    _, _, results = check_quality(sample, cfg)
    assert dict(zip(results["check"], results["failed_rows"], strict=True)) == {
        "duplicate_line_items": 1,
        "missing_invoice_key": 0,
        "cancelled_invoice": 1,
        "non_positive_quantity": 1,
        "non_positive_price": 1,
        "price_outlier": 1,
        "non_product_stock_code": 1,
        "missing_description": 1,
        "missing_customer_id": 1,
    }
    assert len({c.name for c in CHECKS}) == len(CHECKS)


def test_quarantine_keeps_the_reasons_and_the_flagged_rows(sample, cfg):
    clean, quarantine, _ = check_quality(sample, cfg)
    assert len(clean) == 3 and len(quarantine) == 6
    assert (
        "cancelled_invoice"
        in dict(zip(quarantine["invoice_no"], quarantine["reasons"], strict=True))[
            "C536379"
        ]
    )
    # A guest checkout with a blank description is still a real sale.
    assert "536384" in set(clean["invoice_no"])


def test_the_gate_stops_the_run(sample, cfg, tmp_path):
    cfg["quality"]["max_quarantine_rate"] = 0.01
    cfg["paths"] = dict(cfg["paths"], reports=tmp_path)
    with pytest.raises(ValueError, match="refusing to load"):
        check_quality(sample, cfg)


def test_a_failed_gate_still_writes_the_quality_report(sample, cfg, tmp_path):
    """The gate's message says "investigate the source extract", and this report
    is what an investigator opens to do that. Raising before writing it left the
    previous run's green numbers in place, so the one run that needed the report
    was the only run that never produced it."""
    cfg["quality"]["max_quarantine_rate"] = 0.01
    cfg["paths"] = dict(cfg["paths"], reports=tmp_path)

    with pytest.raises(ValueError, match="refusing to load"):
        check_quality(sample, cfg)

    text = (tmp_path / "data_quality_report.md").read_text(encoding="utf-8")
    assert "GATE FAILED" in text and "NOTHING WAS PUBLISHED" in text
    assert "Would have loaded" in text  # not reported as published


# --- the publication boundary ---------------------------------------------- #


def _staged_cfg(cfg, tmp_path):
    cfg["paths"] = dict(
        cfg["paths"], reports=tmp_path / "reports", staging=tmp_path / "staging"
    )
    cfg["paths"]["reports"].mkdir(parents=True)
    return cfg


def _fill_version(cfg, run_id, marker="v"):
    """A complete report version: all three files, none of them published."""
    version = reports_dir(cfg, run_id)
    for name in REPORT_NAMES:
        (version / name).write_text(f"{marker} {name}\n", encoding="utf-8")
    return version


def test_a_version_is_published_by_one_atomic_write(cfg, tmp_path):
    """reports/ describes what is IN the warehouse.

    Every report used to be written straight to reports/ by the task that
    computed it, and every one of those tasks runs before `publish`, so a run
    that failed to publish left last night's warehouse beside tonight's
    reports.

    Staging them and moving them across afterwards was not enough: three
    separate os.replace() calls can half-succeed, and on Windows they routinely
    do - replacing a file another process holds open raises PermissionError,
    which leaves one new report beside two old ones. A version is therefore a
    directory, and publishing it changes exactly one file.
    """
    cfg = _staged_cfg(cfg, tmp_path)
    _fill_version(cfg, "run_a", marker="old")
    P.publish_version(cfg, "run_a")
    _fill_version(cfg, "run_b", marker="new")

    # Built, not published. The pointer has not moved, so a reader still sees
    # the version whose data is in the warehouse.
    assert current_run_id(cfg) == "run_a"
    assert published_reports(cfg).name == "run_a"

    P.publish_version(cfg, "run_b")
    assert current_run_id(cfg) == "run_b"
    for name in REPORT_NAMES:
        assert (
            (published_reports(cfg) / name)
            .read_text(encoding="utf-8")
            .startswith("new")
        )


def test_an_incomplete_version_is_refused(cfg, tmp_path):
    """Pointing at a version before it is complete is the exact failure the
    pointer exists to prevent, so it is refused rather than published."""
    cfg = _staged_cfg(cfg, tmp_path)
    _fill_version(cfg, "run_a", marker="old")
    P.publish_version(cfg, "run_a")

    version = reports_dir(cfg, "run_b")
    (version / "data_quality_report.md").write_text("partial", encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="adoption_report.md"):
        P.publish_version(cfg, "run_b")
    assert current_run_id(cfg) == "run_a"


def test_a_failed_snapshot_copy_does_not_unpublish_the_version(
    cfg, tmp_path, monkeypatch
):
    """FAULT INJECTION: the second convenience copy fails.

    The top-level reports/*.md files are a convenience for GitHub readers, and
    copying them is three separate writes - exactly the operation that used to
    be the publish itself, and exactly the one that can half-succeed. It is now
    downstream of the pointer, so a failure there leaves the published version
    correct and complete; only the copies lag, and each report names its own
    run_id so the lag is visible rather than silent.
    """
    cfg = _staged_cfg(cfg, tmp_path)
    _fill_version(cfg, "run_b", marker="new")

    calls = []
    real = P.shutil.copyfile

    def flaky(src, dst):
        calls.append(pathlib.Path(dst).name)
        if len(calls) == 2:
            raise PermissionError("another process has this file open")
        return real(src, dst)

    monkeypatch.setattr(P.shutil, "copyfile", flaky)

    P.publish_version(cfg, "run_b")

    assert current_run_id(cfg) == "run_b"
    assert len(calls) == len(REPORT_NAMES), "the copy loop stopped at the failure"
    for name in REPORT_NAMES:
        assert (published_reports(cfg) / name).exists()


def test_a_failed_publish_leaves_the_previous_version_current(cfg, tmp_path):
    """FAULT INJECTION: the metrics file is never produced, so the version is
    never complete and the run never publishes. The previous version has to
    stay current, in full."""
    cfg = _staged_cfg(cfg, tmp_path)
    _fill_version(cfg, "run_a", marker="old")
    P.publish_version(cfg, "run_a")

    version = reports_dir(cfg, "run_b")
    (version / "data_quality_report.md").write_text("new", encoding="utf-8")
    (version / "adoption_report.md").write_text("new", encoding="utf-8")
    # run_metrics.json missing: write_run_metrics raised.

    assert finalize_reports(cfg, "run_b") == "failed"
    assert current_run_id(cfg) == "run_a"
    for name in REPORT_NAMES:
        assert (
            (published_reports(cfg) / name)
            .read_text(encoding="utf-8")
            .startswith("old")
        )
    parked = cfg["paths"]["reports"] / "failed_runs" / "run_b"
    assert (parked / "data_quality_report.md").exists()


def test_the_finaliser_waits_rather_than_racing_the_adoption_branch(cfg, tmp_path):
    """FAULT INJECTION: the gate fails first, adoption finishes afterwards.

    The archiver used to run on `one_failed`, which fires as soon as ANY
    upstream fails without waiting for the others. measure_adoption is a root
    task with no upstream, so it runs in parallel with the gate: the archive
    moved the version out from under it, and the adoption report it then wrote
    was never seen again.

    Here both writers finish before the finaliser is called - which is what
    all_done buys - and both reports end up in the same archived version.
    """
    cfg = _staged_cfg(cfg, tmp_path)
    version = reports_dir(cfg, "run_b")
    (version / "data_quality_report.md").write_text("GATE FAILED", encoding="utf-8")
    (version / "adoption_report.md").write_text("adoption, later", encoding="utf-8")

    assert finalize_reports(cfg, "run_b") == "failed"

    parked = cfg["paths"]["reports"] / "failed_runs" / "run_b"
    assert sorted(p.name for p in parked.iterdir()) == [
        "adoption_report.md",
        "data_quality_report.md",
    ]


def test_the_finaliser_is_idempotent(cfg, tmp_path):
    """Airflow retries tasks, so running this twice must not undo the first
    run - neither by re-archiving a published version nor by failing on one
    that is already gone."""
    cfg = _staged_cfg(cfg, tmp_path)

    _fill_version(cfg, "run_ok")
    mark_published(cfg, "run_ok")
    assert finalize_reports(cfg, "run_ok") == "published"
    assert finalize_reports(cfg, "run_ok") == "noop"
    assert current_run_id(cfg) == "run_ok"

    _fill_version(cfg, "run_bad")
    assert finalize_reports(cfg, "run_bad") == "failed"
    assert finalize_reports(cfg, "run_bad") == "noop"
    assert current_run_id(cfg) == "run_ok", "a retry unpublished the good version"


def test_pruning_never_removes_the_published_version(cfg, tmp_path):
    """A run that fails after a successful one leaves a NEWER directory that is
    not published, so "keep the newest" is not the same as "keep the current
    one"."""
    cfg = _staged_cfg(cfg, tmp_path)
    _fill_version(cfg, "run_00")
    mark_published(cfg, "run_00")
    finalize_reports(cfg, "run_00")
    for i in range(1, 9):
        _fill_version(cfg, f"run_{i:02d}")

    P.prune_report_versions(cfg, keep=3)

    assert current_run_id(cfg) == "run_00"
    assert published_reports(cfg) is not None
    assert (published_reports(cfg) / "run_metrics.json").exists()


def test_the_data_does_not_move_unless_the_version_is_complete(
    sample, cfg, tmp_path, monkeypatch
):
    """FAULT INJECTION: run_metrics.json cannot be written.

    The local entry point used to call load() first and write the metrics
    afterwards, which the DAG had already been fixed not to do. A failure
    writing that one file then left the warehouse holding tonight's data,
    reports/CURRENT still naming last night's version, and tonight's
    incomplete version in failed_runs - the data had moved and every report
    describing it had been thrown away.

    Nothing in run_metrics needs the load to have happened: the counts, the
    frames and the input digests all exist before it. So the rule is the same
    in both entry points - the version is complete before the data moves.
    """
    from retail_pipeline import adoption as adoption_module
    from retail_pipeline import recommend as recommend_module

    cfg = _staged_cfg(cfg, tmp_path)
    cfg["paths"]["processed"] = tmp_path / "processed"
    cfg["paths"]["warehouse"] = tmp_path / "warehouse" / "retail.db"
    _fill_version(cfg, "run_old", marker="old")
    P.publish_version(cfg, "run_old")

    empty = pd.DataFrame({"stock_code": [], "metric": [], "value": []})

    def fake_adoption(c, reports_dest=None, run_id=None):
        (reports_dest / "adoption_report.md").write_text("new", encoding="utf-8")
        return {
            "adoption_headline": pd.DataFrame(
                {"metric": ["reach_pct"], "value": [1.0]}
            ),
            "adoption_weekly": empty,
            "adoption_by_team": empty,
        }

    loaded = []
    monkeypatch.setattr(P, "load_config", lambda *a, **k: cfg)
    monkeypatch.setattr(P, "extract", lambda c: sample)
    monkeypatch.setattr(adoption_module, "measure_adoption", fake_adoption)
    monkeypatch.setattr(
        recommend_module,
        "recommend",
        lambda t, c: pd.DataFrame({"stock_code": ["85123A"], "rec": ["71053"]}),
    )
    monkeypatch.setattr(P, "load", lambda *a, **k: loaded.append(a))

    def no_disk(*a, **k):
        raise OSError("no space left on device")

    monkeypatch.setattr(P, "write_run_metrics", no_disk)

    with pytest.raises(OSError, match="no space"):
        P.run()

    assert loaded == [], "the data was published without a complete report version"
    assert current_run_id(cfg) == "run_old"
    for name in REPORT_NAMES:
        assert (
            (published_reports(cfg) / name)
            .read_text(encoding="utf-8")
            .startswith("old")
        )


def test_the_warehouse_records_which_run_it_holds(cfg, tmp_path):
    """ "Did this run's data reach the warehouse" is answered by the warehouse.

    The finaliser used to read a marker file written just after the commit, so
    a crash in between left the warehouse ahead of reports/CURRENT with nothing
    recording it. The run_id is stamped inside load()'s swap transaction, which
    means it becomes true at the same instant the data does.
    """
    cfg = _staged_cfg(cfg, tmp_path)
    cfg["paths"] = dict(
        cfg["paths"], processed=tmp_path / "processed", warehouse=tmp_path / "w.db"
    )
    cfg["paths"]["processed"].mkdir(parents=True)

    assert P.warehouse_run_id(cfg) is None  # nothing published yet

    frame = pd.DataFrame({"invoice_no": ["A"], "stock_code": ["S"], "date_key": ["d"]})
    P.load({"fact_sales": frame}, cfg, run_id="run_x")
    assert P.warehouse_run_id(cfg) == "run_x"

    # ... and the finaliser publishes on that alone, with no marker file.
    version = _fill_version(cfg, "run_x")
    assert not (version / P.PUBLISHED_MARKER).exists()
    assert finalize_reports(cfg, "run_x") == "published"
    assert current_run_id(cfg) == "run_x"


def test_a_warehouse_on_a_different_run_does_not_publish_this_version(cfg, tmp_path):
    """The stamp has to be checked, not merely present: a warehouse left on
    last night's run must not let tonight's failed version become current."""
    cfg = _staged_cfg(cfg, tmp_path)
    cfg["paths"] = dict(
        cfg["paths"], processed=tmp_path / "processed", warehouse=tmp_path / "w.db"
    )
    cfg["paths"]["processed"].mkdir(parents=True)
    frame = pd.DataFrame({"invoice_no": ["A"], "stock_code": ["S"], "date_key": ["d"]})
    P.load({"fact_sales": frame}, cfg, run_id="run_last_night")

    _fill_version(cfg, "run_tonight")
    assert finalize_reports(cfg, "run_tonight") == "failed"
    assert current_run_id(cfg) is None
    assert P.warehouse_run_id(cfg) == "run_last_night"


def test_a_failed_gate_does_not_overwrite_the_published_report(sample, cfg, tmp_path):
    """The gate report is the sharpest case.

    check_quality writes "GATE FAILED - NOTHING WAS PUBLISHED" before raising.
    Written into reports/, that sentence lands on top of the report describing
    the data the warehouse still holds and still serves, telling every reader
    the current contents were rejected - the reverse of what happened.
    """
    cfg = _staged_cfg(cfg, tmp_path)
    _fill_version(cfg, "run_a", marker="old")
    P.publish_version(cfg, "run_a")

    cfg["quality"]["max_quarantine_rate"] = 0.01
    version = reports_dir(cfg, "run_b")
    with pytest.raises(ValueError, match="refusing to load"):
        check_quality(sample, cfg, reports_dest=version, run_id="run_b")

    assert current_run_id(cfg) == "run_a"
    assert "GATE FAILED" not in (
        published_reports(cfg) / "data_quality_report.md"
    ).read_text(encoding="utf-8")

    assert finalize_reports(cfg, "run_b") == "failed"
    parked = cfg["paths"]["reports"] / "failed_runs" / "run_b"
    text = (parked / "data_quality_report.md").read_text(encoding="utf-8")
    assert "GATE FAILED" in text
    assert "run_id: run_b" in text  # the version says which run it describes
    assert current_run_id(cfg) == "run_a"


# --- star schema ----------------------------------------------------------- #


def test_star_schema_keys_and_referential_integrity(sample, cfg):
    clean, _, _ = check_quality(sample, cfg)
    t = transform(clean)
    assert t["dim_product"]["stock_code"].is_unique
    assert set(t["fact_sales"]["stock_code"]) <= set(t["dim_product"]["stock_code"])
    assert set(t["fact_sales"]["date_key"]) <= set(t["dim_date"]["date_key"])
    assert "description" not in t["fact_sales"].columns  # attributes live in dimensions


def test_dim_date_is_a_continuous_calendar(sample, cfg):
    """Built from observed dates it would have holes wherever the shop was shut,
    and BI grouping by week would silently get six-day weeks."""
    clean, _, _ = check_quality(sample, cfg)
    clean.loc[clean.index[-1], "invoice_ts"] += pd.Timedelta(days=7)
    dim = transform(clean)["dim_date"]
    assert list(dim["date_key"]) == list(
        pd.date_range(dim["date_key"].min(), dim["date_key"].max(), freq="D")
    )
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
    dim = pd.DataFrame(
        {
            "stock_code": list("ABCD"),
            "description": [
                "RED MUG LARGE",
                "RED MUG SMALL",
                "BLUE PLATE ROUND",
                "BLUE PLATE SQUARE",
            ],
        }
    )
    return {"fact_sales": fact, "dim_product": dim}


def test_single_item_baskets_count_in_the_denominator(cfg):
    """Baskets that cannot form a pair are still part of the population.

    Three baskets - {A}, {A,B}, {A,B,C} - worked by hand:

        all baskets      = 3      A in 3, B in 2, C in 1
        A and B together = 2
        support(A,B)     = 2/3
        confidence(A→B)  = 2/3    NOT 2/2
        lift(A,B)        = (2/3) / (2/3) = 1.0

    Counting the population over pair-forming baskets only would give
    confidence 2/2 = 1.0 and lift 1.5: a rule that holds two times in three
    reported as one that never fails. A basket where A was bought alone is
    evidence against A→B, which is exactly why it has to stay in.
    """
    cfg["recommend"].update(min_support_count=1, min_confidence=0.0, min_lift=0.0)
    fact = pd.DataFrame(
        [("I1", "A"), ("I2", "A"), ("I2", "B"), ("I3", "A"), ("I3", "B"), ("I3", "C")],
        columns=["invoice_no", "stock_code"],
    )
    dim = pd.DataFrame({"stock_code": list("ABC"), "description": ["AA", "BB", "CC"]})
    recs = recommend({"fact_sales": fact, "dim_product": dim}, cfg)

    ab = recs[(recs.stock_code == "A") & (recs.recommended_stock_code == "B")]
    assert round(ab["support"].iat[0], 4) == round(2 / 3, 4)
    assert round(ab["confidence"].iat[0], 4) == round(2 / 3, 4)
    assert round(ab["lift"].iat[0], 4) == 1.0


def test_oversized_baskets_count_in_the_denominator(cfg):
    """The size cap is a pair-generation guard, not a population definition.

    A product sold in wholesale-scale baskets had those baskets dropped from
    its own denominator, which is what inflated confidence 4.2x on the worst
    real rule. Here A appears in 3 baskets, one of them over the cap, and
    pairs with B in one of the two countable ones.
    """
    cfg["recommend"].update(
        min_support_count=1, min_confidence=0.0, min_lift=0.0, max_basket_size=3
    )
    rows = [("I1", "A"), ("I2", "A"), ("I2", "B")]
    rows += [("I3", p) for p in ("A", "B", "C", "D", "E")]  # 5 items > cap 3
    fact = pd.DataFrame(rows, columns=["invoice_no", "stock_code"])
    dim = pd.DataFrame(
        {"stock_code": list("ABCDE"), "description": [f"D{x}" for x in "ABCDE"]}
    )
    recs = recommend({"fact_sales": fact, "dim_product": dim}, cfg)

    ab = recs[(recs.stock_code == "A") & (recs.recommended_stock_code == "B")]
    # A is in all 3 baskets; the oversized one is excluded from pairing only.
    assert round(ab["confidence"].iat[0], 4) == round(1 / 3, 4)
    assert round(ab["support"].iat[0], 4) == round(1 / 3, 4)


def test_lift_is_computed_correctly(cfg):
    cfg["recommend"]["min_support_count"] = 5
    recs = recommend(_basket_tables(), cfg)
    ab = recs[(recs["stock_code"] == "A") & (recs["recommended_stock_code"] == "B")]
    # A and B share 20 of 40 baskets and never appear apart:
    # confidence = 1.0, support = 0.5, lift = 1.0 / 0.5 = 2.0
    assert ab["confidence"].iat[0] == 1.0 and ab["support"].iat[0] == 0.5
    assert round(ab["lift"].iat[0], 6) == 2.0
    # Products from disjoint baskets never produce a rule.
    assert recs[
        (recs["stock_code"] == "A")
        & (recs["recommended_stock_code"] == "C")
        & (recs["method"] == "co_purchase")
    ].empty


def test_an_empty_fact_table_returns_the_schema_rather_than_raising(cfg):
    """`test_an_empty_result_still_has_its_schema` below covers "baskets exist
    but no rule cleared the thresholds". This covers the other empty: no rows.

    The failure this guards against is dtype-dependent, which is what makes it
    worth a test rather than a comment. An empty fact_sales read back from
    Parquet keeps its `string` dtype and works fine, so the pipeline's own path
    is safe. But an empty frame that carries no dtype information - the shape a
    hand-built fixture, a hand-written recovery script, or a non-Parquet
    hand-off produces - comes back from the groupby as float64, and the .str
    accessor then raises an AttributeError naming that dtype instead of naming
    the empty input.
    """
    empty = pd.DataFrame({"invoice_no": [], "stock_code": []})  # -> float64
    assert empty["stock_code"].dtype == "float64", "fixture must be dtype-less"
    recs = recommend(
        {
            "fact_sales": empty,
            "dim_product": pd.DataFrame({"stock_code": [], "description": []}),
        },
        cfg,
    )
    assert recs.empty and list(recs.columns) == COLUMNS


def test_an_empty_result_still_has_its_schema(cfg):
    """A zero-column frame is not "no recommendations" - it is a shape no
    consumer can read, and it makes SQLite fail on `CREATE TABLE x ()`."""
    cfg["recommend"]["min_support_count"] = 10_000
    tables = _basket_tables()
    tables["dim_product"]["description"] = "UNKNOWN"  # kills the fallback too
    recs = recommend(tables, cfg)
    assert recs.empty and list(recs.columns) == COLUMNS


# --- adoption -------------------------------------------------------------- #


def test_reach_divides_by_the_roster_not_by_who_showed_up(events, cfg):
    cfg["adoption"]["roster"] = {"Category Management": 1, "Merchandising": 4}
    cfg["adoption"]["licensed_users"] = 5
    t = team_metrics(events, cfg).set_index("team")
    assert t.loc["Category Management", "reach_pct"] == 100.0
    assert t.loc["Merchandising", "reach_pct"] == 25.0  # 1 active of 4 licensed


def test_analysis_as_of_is_honoured_and_shared(events, cfg):
    """Every window measures back from one date, and that date can be pinned.

    Defaulting to the newest event is right for a fixed extract and wrong on a
    live feed: the window slides with the data, so a telemetry feed that stopped
    a month ago still reports full reach. Pinning it also stops the three tables
    disagreeing about which day "the last four weeks" ends on.
    """
    cfg["adoption"]["roster"] = {"Category Management": 1, "Merchandising": 1}
    cfg["adoption"]["licensed_users"] = 2

    # Events are dated 2026-04-06/07. An as_of far past them puts everything
    # outside the four-week window, so reach must be 0 rather than following
    # the data forward.
    cfg["adoption"]["analysis_as_of"] = "2026-08-01"
    as_of = adoption_mod.resolve_as_of(events, cfg)
    # A bare date means the END of that day, or "as of 1 August" would drop
    # everything that happened on 1 August.
    assert as_of == pd.Timestamp("2026-08-01 23:59:59.999999999")
    assert (
        headline_metrics(events, cfg, as_of)
        .set_index("metric")
        .loc["reach_pct", "value"]
        == 0.0
    )
    assert team_metrics(events, cfg, as_of)["active_users"].sum() == 0

    # Pinned to the launch week, the same events are inside it for both tables -
    # including U2's 09:00 event on the as_of date itself.
    cfg["adoption"]["analysis_as_of"] = "2026-04-07"
    as_of = adoption_mod.resolve_as_of(events, cfg)
    h = (
        headline_metrics(events, cfg, as_of)
        .set_index("metric")
        .loc["reach_pct", "value"]
    )
    t = team_metrics(events, cfg, as_of)
    assert h == 100.0 and t["active_users"].sum() == 2

    # Unset, it falls back to the newest event rather than to today.
    cfg["adoption"].pop("analysis_as_of")
    assert adoption_mod.resolve_as_of(events, cfg) == events["event_ts"].max()


def test_events_after_as_of_change_nothing(events, cfg):
    """as_of is a snapshot, not just a window edge.

    It used to bound only the four-week reach window, so activation, action
    rate, CSAT, the weekly trend and every team row still read the whole log.
    With as_of at 7 April, two events dated 20 May moved action rate 50 -> 100,
    CSAT from no-data to 1.0, activation 50 -> 100, and the weekly table from
    one row to seven - under a heading that said "four weeks ending 7 April".
    """
    cfg["adoption"]["roster"] = {"Category Management": 1, "Merchandising": 1}
    cfg["adoption"]["licensed_users"] = 2
    as_of = pd.Timestamp("2026-04-07 23:59:59.999999999")

    later = pd.DataFrame(
        [
            ("2026-05-20 10:00", "U2", "Merchandising", "apply", "S1", None),
            ("2026-05-20 10:05", "U2", "Merchandising", "feedback", None, 1.0),
        ],
        columns=[
            "event_ts",
            "user_id",
            "team",
            "event_type",
            "stock_code",
            "feedback_score",
        ],
    )
    later["event_ts"] = pd.to_datetime(later["event_ts"])
    contaminated = pd.concat(
        [events.drop(columns=["week_start"]), later], ignore_index=True
    )

    def snapshot(ev):
        h = headline_metrics(ev, cfg, as_of).set_index("metric")["value"]
        t = team_metrics(ev, cfg, as_of)
        return (
            h["reach_pct"],
            h["activation_pct"],
            h["action_rate_pct"],
            pd.isna(h["csat"]),
            len(weekly_metrics(ev, cfg, as_of)),
            int(t["applies"].sum()),
            int(t["active_users"].sum()),
        )

    assert snapshot(contaminated) == snapshot(events.drop(columns=["week_start"]))


def test_csat_outside_the_scale_and_blank_teams_are_rejected(cfg, tmp_path):
    """CSAT is a mean, so one out-of-range score moves the headline without ever
    looking wrong; an event with no team counts in the totals and in no team's
    row, so the parts stop summing to the whole."""
    header = "event_ts,user_id,team,event_type,stock_code,feedback_score\n"
    good = "2026-04-06 10:00:00,U1,Merchandising,view,S1,\n"

    p = tmp_path / "bad_score.csv"
    p.write_text(header + good + "2026-04-06 10:05:00,U1,Merchandising,feedback,,9\n")
    cfg["paths"] = dict(cfg["paths"], usage_events=p)
    with pytest.raises(ValueError, match="outside 1-5"):
        adoption_mod.load_events(cfg)

    p2 = tmp_path / "blank_team.csv"
    p2.write_text(header + good + "2026-04-06 10:05:00,U2,,view,S1,\n")
    cfg["paths"] = dict(cfg["paths"], usage_events=p2)
    with pytest.raises(ValueError, match="no team"):
        adoption_mod.load_events(cfg)


def test_a_team_with_zero_adoption_shows_zero_rather_than_vanishing(events, cfg):
    """Deriving the team list from the event log drops exactly the team the
    report exists to surface."""
    cfg["adoption"]["roster"] = {"Category Management": 1, "Ghost Team": 5}
    cfg["adoption"]["licensed_users"] = 6
    t = team_metrics(events, cfg).set_index("team")
    assert "Ghost Team" in t.index and t.loc["Ghost Team", "reach_pct"] == 0.0


def test_a_week_with_no_activity_stays_in_the_trend(cfg):
    """Dropping it closes the gap and shifts every later week one position left."""
    rows = [
        ("2026-04-06 10:00", "U1", "T", "view", "S1", None),
        ("2026-04-06 10:01", "U1", "T", "apply", "S1", None),
        ("2026-04-20 10:00", "U1", "T", "view", "S1", None),
    ]  # week 2 empty
    df = pd.DataFrame(
        rows,
        columns=[
            "event_ts",
            "user_id",
            "team",
            "event_type",
            "stock_code",
            "feedback_score",
        ],
    )
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
    h = headline_metrics(events[events["event_type"] != "feedback"], cfg).set_index(
        "metric"
    )
    assert pd.isna(h.loc["csat", "value"]) and h.loc["csat", "status"] == "no data"


def test_no_usage_at_all_reports_zeros_rather_than_failing(cfg):
    empty = pd.DataFrame(
        columns=[
            "event_ts",
            "user_id",
            "team",
            "event_type",
            "stock_code",
            "feedback_score",
            "week_start",
        ]
    )
    empty["event_ts"] = pd.to_datetime(empty["event_ts"])
    h = headline_metrics(empty, cfg).set_index("metric")
    t = team_metrics(empty, cfg)
    assert h.loc["reach_pct", "value"] == 0.0
    assert len(t) == len(cfg["adoption"]["roster"])  # every team still listed
    assert weekly_metrics(empty, cfg).empty


# --- provenance ------------------------------------------------------------ #


def test_the_input_digest_is_the_same_on_windows_and_linux(cfg, tmp_path):
    """The digest exists to answer "could this input have produced different
    numbers?". It has to be portable to answer that at all.

    to_csv defaults its lineterminator to os.linesep - for a returned string as
    much as for a written file - so without an explicit terminator the same
    telemetry hashes one way on Windows (CRLF) and another on Linux (LF). A dev
    box and a CI runner then disagree permanently on byte-identical data, and
    the check reads as "the input changed" when only the OS did.
    """
    events = tmp_path / "usage_events.csv"
    events.write_text(
        "event_ts,user_id,team,event_type,stock_code,feedback_score\n"
        "2026-04-06 10:00:00,U1,Category Management,view,S1,\n"
        "2026-04-06 10:05:00,U1,Category Management,apply,S1,\n"
        "2026-04-07 09:10:00,U2,Merchandising,feedback,,5\n",
        encoding="utf-8",
    )
    cfg["paths"] = dict(cfg["paths"], usage_events=events, raw=tmp_path / "absent.csv")

    real = os.linesep
    try:
        digests = {}
        for label, sep in (("LF", "\n"), ("CRLF", "\r\n")):
            os.linesep = sep
            digests[label] = _input_fingerprint(cfg)["usage_events.csv"]["sha256"]
    finally:
        os.linesep = real

    assert digests["LF"] == digests["CRLF"], (
        f"digest depends on os.linesep: LF gave {digests['LF']}, "
        f"CRLF gave {digests['CRLF']}"
    )


def test_headline_and_team_reach_share_one_denominator(cfg):
    """A team using the solution but missing from the roster used to be counted
    in headline's numerator and excluded from its denominator, while the by-team
    table repaired its own copy - so the two tables divided by different totals
    and headline could exceed 100%."""
    rows = [
        ("2026-04-06 10:00", f"U{i}", "Ghost Team", "view", "S1", None)
        for i in range(20)
    ]
    rows += [("2026-04-06 10:00", "K1", "Category Management", "view", "S1", None)]
    df = pd.DataFrame(
        rows,
        columns=[
            "event_ts",
            "user_id",
            "team",
            "event_type",
            "stock_code",
            "feedback_score",
        ],
    )
    df["event_ts"] = pd.to_datetime(df["event_ts"])
    cfg["adoption"]["roster"] = {"Category Management": 1}
    cfg["adoption"]["licensed_users"] = 1  # the stale configured total

    h = headline_metrics(df, cfg).set_index("metric")
    t = team_metrics(df, cfg)

    assert h.loc["reach_pct", "value"] <= 100.0
    assert int(t["licensed_users"].sum()) == 21  # 1 configured + 20 observed
    assert h.loc["reach_pct", "value"] == round(100 * 21 / 21, 1)
    assert set(t["team"]) == {"Category Management", "Ghost Team"}


def test_load_publishes_neither_layer_when_sqlite_fails(cfg, tmp_path, monkeypatch):
    """pandas commits after every to_sql, so wrapping the loop in `with conn:`
    never made the multi-table replace atomic: a mid-load failure left fact_sales
    from tonight beside dim_product from last night, in both Parquet and SQLite."""
    import pandas as _pd

    from retail_pipeline import pipeline as P

    cfg["paths"] = dict(
        cfg["paths"],
        processed=tmp_path / "processed",
        warehouse=tmp_path / "wh" / "retail.db",
    )
    old = {
        "dim_product": _pd.DataFrame(
            {"stock_code": ["OLD"], "description": ["last night"]}
        ),
        "fact_sales": _pd.DataFrame(
            {"stock_code": ["OLD"], "date_key": ["2026-01-01"]}
        ),
    }
    P.load(old, cfg)

    real = _pd.DataFrame.to_sql

    def boom(self, name, con, **kw):
        if name.startswith("dim_product"):
            raise RuntimeError("worker killed mid-load")
        return real(self, name, con, **kw)

    monkeypatch.setattr(_pd.DataFrame, "to_sql", boom)

    new = {
        "dim_product": _pd.DataFrame(
            {"stock_code": ["NEW"], "description": ["tonight"]}
        ),
        "fact_sales": _pd.DataFrame(
            {"stock_code": ["NEW"], "date_key": ["2026-02-02"]}
        ),
    }
    with pytest.raises(RuntimeError, match="worker killed"):
        P.load(new, cfg)

    import sqlite3

    with sqlite3.connect(cfg["paths"]["warehouse"]) as conn:
        assert conn.execute("SELECT stock_code FROM fact_sales").fetchall() == [
            ("OLD",)
        ]
        assert conn.execute("SELECT stock_code FROM dim_product").fetchall() == [
            ("OLD",)
        ]
    for name in ("fact_sales", "dim_product"):
        got = _pd.read_parquet(cfg["paths"]["processed"] / f"{name}.parquet")
        assert list(got["stock_code"]) == ["OLD"], (
            f"{name} parquet was published anyway"
        )
    assert not list(cfg["paths"]["processed"].glob("*.tmp"))  # no sidecars left behind


def test_an_index_failure_leaves_both_layers_on_the_previous_run(cfg, tmp_path):
    """Indexes are built inside the swap transaction, not after it.

    Built after the COMMIT, a failure there left the warehouse holding tonight's
    tables while the error path deleted the staged Parquet - so SQLite was the
    new run, Parquet was entirely the old one, and load() raised, telling the
    caller nothing had been published. Reproduced by pointing an index at a
    column that does not exist: sqlite 7 rows against parquet 522,566.
    """
    import sqlite3

    from retail_pipeline import pipeline as P

    cfg["paths"] = dict(cfg["paths"], processed=tmp_path, warehouse=tmp_path / "w.db")
    # date_key and stock_code exist because the real _INDEXES reference them.
    first = {
        "fact_sales": pd.DataFrame(
            {"invoice_no": ["A"], "stock_code": ["S"], "date_key": ["2011-01-01"]}
        )
    }
    P.load(first, cfg)

    second = {
        "fact_sales": pd.DataFrame({"invoice_no": ["B"] * 5, "stock_code": ["T"] * 5})
    }
    bad = ("CREATE INDEX ix_boom ON fact_sales(no_such_column)",)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(P, "_INDEXES", bad)
        with pytest.raises(sqlite3.OperationalError):
            P.load(second, cfg)

    with sqlite3.connect(cfg["paths"]["warehouse"]) as conn:
        rows = conn.execute("SELECT count(*) FROM fact_sales").fetchone()[0]
        leftover = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%__new'"
            )
        ]
    assert rows == 1, "SQLite moved to the failed run"
    assert len(pd.read_parquet(tmp_path / "fact_sales.parquet")) == 1
    # pandas commits <name>__new outside the swap transaction, so a rollback
    # would otherwise leave a partial run's rows sitting in the database.
    assert leftover == [], f"staging tables leaked: {leftover}"


def test_a_table_that_stops_being_published_is_retired(cfg, tmp_path):
    """The warehouse accumulated tables no code writes any more - dq_results and
    adoption_top_products sat there for weeks, still answering queries with
    whatever the last version that wrote them produced. A stale table that
    answers is worse than a missing one that errors."""
    import sqlite3

    from retail_pipeline import pipeline as P

    cfg["paths"] = dict(cfg["paths"], processed=tmp_path, warehouse=tmp_path / "w.db")
    frame = pd.DataFrame({"a": [1]})
    P.load({"keep": frame, "retire_me": frame}, cfg)
    assert (tmp_path / "retire_me.parquet").exists()

    P.load({"keep": frame}, cfg)

    with sqlite3.connect(cfg["paths"]["warehouse"]) as conn:
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    assert "keep" in tables and "retire_me" not in tables
    assert not (tmp_path / "retire_me.parquet").exists(), "Parquet outlived its table"


def test_a_warehouse_predating_the_manifest_still_retires_its_legacy_tables(
    cfg, tmp_path
):
    """Manifest-based retirement only reaches tables a previous run recorded.

    A warehouse built before the manifest existed has none, so dq_results and
    adoption_top_products survived every upgrade - which is the state that
    prompted the manifest in the first place. Verified against a database with
    the legacy tables and no _published: both were still there after a load.

    The migration is an explicit allowlist. A table someone else put in this
    database is not ours to drop, so an unknown one must survive.
    """
    import sqlite3

    from retail_pipeline import pipeline as P

    cfg["paths"] = dict(cfg["paths"], processed=tmp_path, warehouse=tmp_path / "w.db")
    with sqlite3.connect(tmp_path / "w.db") as conn:
        for name in (*P.LEGACY_RETIRED_TABLES, "someone_elses_table"):
            conn.execute(f'CREATE TABLE "{name}" (a INTEGER)')
            conn.execute(f'INSERT INTO "{name}" VALUES (1)')
        conn.commit()

    P.load(
        {
            "fact_sales": pd.DataFrame(
                {"invoice_no": ["A"], "stock_code": ["S"], "date_key": ["2011-01-01"]}
            )
        },
        cfg,
    )

    with sqlite3.connect(tmp_path / "w.db") as conn:
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    for gone in P.LEGACY_RETIRED_TABLES:
        assert gone not in tables, f"{gone} survived the migration"
    assert "someone_elses_table" in tables, (
        "the migration dropped a table it does not own"
    )
    assert "fact_sales" in tables


def test_a_legacy_parquet_is_retired_even_when_its_table_is_already_gone(cfg, tmp_path):
    """The two layers can disagree about what is still there.

    Retirement intersected the "no longer published" set with the tables SQLite
    currently holds, so SQLite's state decided the Parquet layer's. On a
    warehouse whose database had been rebuilt or deleted - a routine recovery -
    dq_results was absent from SQLite, so it was never retired, so
    dq_results.parquet was never removed. Power BI reads that directory, not
    the database, so the report kept serving a table the warehouse no longer
    had, indefinitely.

    Here: no _published, no legacy SQLite table, only the orphaned Parquet.
    """
    from retail_pipeline import pipeline as P

    cfg["paths"] = dict(cfg["paths"], processed=tmp_path, warehouse=tmp_path / "w.db")
    orphan = tmp_path / "dq_results.parquet"
    pd.DataFrame({"check": ["cancelled_invoice"], "failed_rows": [9999]}).to_parquet(
        orphan, index=False
    )
    assert "dq_results" in P.LEGACY_RETIRED_TABLES

    P.load(
        {
            "fact_sales": pd.DataFrame(
                {"invoice_no": ["A"], "stock_code": ["S"], "date_key": ["2011-01-01"]}
            )
        },
        cfg,
    )

    assert not orphan.exists(), "an orphaned legacy Parquet outlived the migration"
