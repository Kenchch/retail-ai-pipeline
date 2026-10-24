"""Run every stage in pipeline order and log how the data changes shape.

    python scripts/trace_pipeline.py

The trace is a project directory of its own, data/trace/<run_id>/: a copy of
config.yaml with every output pointed there, and a copy of the dbt project. The
stages find it through RETAIL_CONFIG, the way tests/test_dag_run.py points the
DAG at a fixture, so data/runs/, published/, reports/ and dbt/ are never touched
and data/raw/ is only read. At the end the trace's reports are compared with the
committed ones, and its tables with the published data version when there is
one, so the log doubles as a reproducibility check. The dbt step needs
requirements-dbt.txt and is skipped without it. About 2 minutes.

    trace.log          full log: per step, input and output shape, columns,
                       dtypes, nulls, ranges, sample rows, what changed
    dbt_build.log      dbt's own output for the dbt step
    data_lineage.md    one table, one row per step
"""

import contextlib
import datetime as dt
import hashlib
import io
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import textwrap
import time
from importlib.metadata import version as package_version
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bi import build_star_schema
from retail_pipeline import pipeline, publish
from retail_pipeline.adoption import (
    eligible_events,
    load_events,
    measure_adoption,
    resolve_as_of,
)
from retail_pipeline.evaluate import evaluate
from retail_pipeline.recommend import co_purchase_rules, content_fallback, recommend
from scripts import run_dbt
from scripts.check_committed_reports import _committed, _normalise

TOTAL = 11
ANSI = re.compile(r"\x1b\[[0-9;]*m")  # dbt colours its output even when piped

log = logging.getLogger("trace")
lineage = []


# ---------------------------------------------------------------- formatting


def _mark(record):
    record.mark = "" if record.name == "trace" else f"[{record.name}] "
    return True


def setup(out):
    if hasattr(sys.stdout, "reconfigure"):  # a cp1252 console cannot print the log
        sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
    out.mkdir(parents=True)
    fmt = logging.Formatter("%(asctime)s  %(mark)s%(message)s", "%H:%M:%S")
    handlers = (
        logging.FileHandler(out / "trace.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    )
    for h in handlers:
        h.setFormatter(fmt)
        h.addFilter(_mark)
    # The stages log through "pipeline". Sending it to the same handlers puts
    # what each stage reported into the trace, marked and in order; without
    # propagate=False, pipeline.py's basicConfig would print it a second time.
    for name in ("trace", "pipeline"):
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        for h in handlers:
            logger.addHandler(h)


def header(n, total, name, module, what):
    log.info("")
    log.info("=" * 78)
    log.info(f"[{n}/{total}] {name}   ({module})   {what}")
    log.info("=" * 78)


def frame(label, df, sample=3, max_cols=10):
    """Shape, memory, per-column dtype / nulls / range, and a few rows."""
    mem = df.memory_usage(deep=True).sum() / 2**20
    log.info(f"  {label}: {len(df):,} 行 x {df.shape[1]} 列, 内存 {mem:,.1f} MB")
    for c in df.columns:
        col = df[c]
        info = f"{str(col.dtype):<22} 空值 {int(col.isna().sum()):>8,}"
        if pd.api.types.is_bool_dtype(col):
            info += f"  True {int(col.fillna(False).sum()):,}"
        elif pd.api.types.is_numeric_dtype(col) or pd.api.types.is_datetime64_any_dtype(
            col
        ):
            if col.notna().any():
                lo, hi = col.min(), col.max()
                if pd.api.types.is_integer_dtype(col):  # ids and years in full
                    lo, hi = int(lo), int(hi)
                elif pd.api.types.is_numeric_dtype(col):
                    lo, hi = f"{lo:,.4g}", f"{hi:,.4g}"
                info += f"  范围 [{lo}, {hi}]"
        else:
            info += f"  {col.nunique():,} 种取值"
        log.info(f"      {str(c):<24} {info}")
    shown = list(df.columns[:max_cols])
    extra = (
        f"  (另有 {df.shape[1] - len(shown)} 列未显示)"
        if df.shape[1] > max_cols
        else ""
    )
    log.info(f"      前 {sample} 行{extra}:")
    log.info(
        textwrap.indent(df[shown].head(sample).to_string(max_colwidth=22), "      | ")
    )


def table(df):
    """A small frame in full: rule results, metrics."""
    log.info(textwrap.indent(df.to_string(index=False, max_colwidth=40), "      | "))


def change(text):
    log.info(f"  -> 变化: {text}")


def record(step, module, inp, out, rows, what):
    lineage.append(
        {
            "step": step,
            "module": module,
            "input": inp,
            "output": out,
            "rows": rows,
            "change": what,
        }
    )


def timed(fn, *args, **kw):
    t0 = time.perf_counter()
    result = fn(*args, **kw)
    log.info(f"  耗时 {time.perf_counter() - t0:,.1f} s")
    return result


def git_tag():
    """Short commit, -dirty when the tree has changes: the code this trace ran."""

    def git(*args):
        return subprocess.run(
            ["git", *args], cwd=ROOT, capture_output=True, text=True, check=False
        ).stdout.strip()

    return (git("rev-parse", "--short", "HEAD") or "nogit") + (
        "-dirty" if git("status", "--porcelain") else ""
    )


def make_project(out):
    """The real config with its outputs under `out` and its inputs left where
    they are, plus a copy of the dbt project: the directory tests/test_dag_run.py
    builds around a fixture feed, built around the real extract instead."""
    real = pipeline.load_config(ROOT / "config.yaml")
    cfg = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    for key in ("raw", "usage_events"):
        cfg["paths"][key] = real["paths"][key].as_posix()
    path = out / "config.yaml"
    path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
    shutil.copytree(
        ROOT / "dbt",
        out / "dbt",
        ignore=shutil.ignore_patterns(
            "target", "logs", "runs", "dbt_packages", "*.duckdb*", "CURRENT.json"
        ),
    )
    os.environ["RETAIL_CONFIG"] = str(path)
    cfg = pipeline.load_config()
    for key in ("processed", "reports"):  # as run() does
        cfg["paths"][key].mkdir(parents=True, exist_ok=True)
    return cfg


# ---------------------------------------------------------------- steps


def step_config(cfg):
    header(
        0,
        TOTAL,
        "config",
        "config.yaml",
        "所有阶段共用的参数; 输出路径已指向 trace 目录",
    )
    for k, v in cfg["paths"].items():
        log.info(f"  paths.{k:<14} {v}")
    for k, v in cfg.items():
        if k != "paths":
            log.info(f"  {k}: {v}")


def step_extract(cfg):
    header(1, TOTAL, "extract", "pipeline.py", "读 CSV, 统一列名和类型, 一行不删")
    path = cfg["paths"]["raw"]
    src = pd.read_csv(path, dtype=str)
    frame(f"原始 {path.name} (全部按字符串读)", src)
    raw = timed(pipeline.extract, cfg)
    frame("extract() 输出", raw)
    code = src["StockCode"]
    renamed = code.notna() & (code.str.strip().str.upper() != code)
    log.info(
        f"  时间解析失败 (-> NaT) {int(raw.invoice_ts.isna().sum()):,} 行; "
        f"customer_id 为空 {int(raw.customer_id.isna().sum()):,} 行; "
        f"stock_code 去空格转大写后改变 {int(renamed.sum()):,} 行"
    )
    change(
        f"{len(src):,} 行原样保留。列按 column_map 改名; invoice_ts 字符串 -> "
        "datetime64; quantity / unit_price -> 数值; customer_id -> Int64 (可空整数); "
        "invoice_no / stock_code / description 成为 string 类型"
    )
    record(
        "extract",
        "pipeline.py",
        f"{path.name} {len(src):,} 行 x {src.shape[1]} 列",
        f"raw {len(raw):,} 行 x {raw.shape[1]} 列",
        "x1 (不删行)",
        "列改名; 字符串 -> 时间 / 数值 / Int64; stock_code 规范化",
    )
    return raw


def step_quality(cfg, raw, version, run_id):
    header(
        2,
        TOTAL,
        "data_quality_gate",
        "pipeline.py + credits.py",
        f"{len(pipeline.CHECKS)} 条规则, 阻断的隔离不删除; 退货单配对只打标记",
    )
    clean, quarantine, results = timed(
        pipeline.check_quality, raw, cfg, reports_dest=version, run_id=run_id
    )
    pipeline.write_quality_report(
        results,
        cfg,
        len(raw),
        len(clean),
        len(quarantine),
        dest=version,
        run_id=run_id,
    )
    log.info("  每条规则的结果 (一行可以违反多条, 所以不能相加):")
    table(results.drop(columns="why"))
    frame("clean (通过全部阻断规则)", clean, max_cols=13)
    frame("quarantine (多一列 reasons)", quarantine)
    multi = int(quarantine.reasons.str.contains(",").sum())
    log.info(
        f"  隔离原因 (前 6 种组合): {quarantine.reasons.value_counts().head(6).to_dict()}; "
        f"同时违反多条的 {multi:,} 行"
    )
    rate, ceiling = len(quarantine) / len(raw), cfg["quality"]["max_quarantine_rate"]
    log.info(
        f"  闸门: 隔离率 {rate:.2%} vs 上限 {ceiling:.0%} -> "
        f"{'通过' if rate <= ceiling else '失败'}"
    )
    total = len(clean) + len(quarantine)
    log.info(
        f"  核对: clean {len(clean):,} + quarantine {len(quarantine):,} = {total:,} "
        f"vs raw {len(raw):,} -> {'一致' if total == len(raw) else '不一致'}"
    )
    flagged = clean[clean.reversed_by_credit]
    log.info(
        f"  退货单配对: {len(flagged):,} 行销售被 "
        f"{flagged.matched_credit_invoice.nunique():,} 张退货单冲掉, 金额 GBP "
        f"{(flagged.quantity * flagged.unit_price).sum():,.2f}; 仍在 clean 里, 只打标记"
    )
    log.info(
        f"  不阻断的规则: clean 里没有 customer_id 的 "
        f"{int(clean.customer_id.isna().sum()):,} 行 (游客结账) 照样进入后面的步骤"
    )
    change(
        f"{len(raw):,} 行分成 clean {len(clean):,} + quarantine {len(quarantine):,}。"
        "clean 多两列 (reversed_by_credit, matched_credit_invoice): 被退货单冲掉的销售"
        "只标记不删; quarantine 多一列 reasons, 记下违反的阻断规则"
    )
    record(
        "data_quality_gate",
        "pipeline.py + credits.py",
        f"raw {len(raw):,} 行",
        f"clean {len(clean):,} 行, quarantine {len(quarantine):,} 行",
        f"隔离 {rate:.2%}",
        f"{len(pipeline.CHECKS)} 条规则; 隔离不删除; 退货单配对打标记",
    )
    return clean, quarantine


def step_transform(clean):
    header(3, TOTAL, "transform", "pipeline.py", "星型模型: 1 张事实表 + 3 张维度表")
    tables = timed(pipeline.transform, clean)
    for name, df in tables.items():
        frame(name, df, max_cols=11)
    fact, dim_date = tables["fact_sales"], tables["dim_date"]
    closed = dim_date.loc[~dim_date.has_sales, "day_of_week"].value_counts().to_dict()
    log.info(
        f"  核对: fact_sales {len(fact):,} 行 vs clean {len(clean):,} 行 -> "
        f"{'一致' if len(fact) == len(clean) else '不一致'}; 收入合计 GBP "
        f"{fact.revenue.sum():,.2f}; 游客行 {int(fact.customer_id.isna().sum()):,}"
    )
    log.info(
        f"  dim_date 是 {len(dim_date):,} 天的连续日历, 没有交易的 "
        f"{int((~dim_date.has_sales).sum())} 天: {closed}"
    )
    change(
        "一张交易表拆成 1 事实 + 3 维度。fact_sales 仍是一行一个销售行, 新增 date_key 和 "
        "revenue = quantity x unit_price, description 移出; dim_product 按 stock_code "
        "汇总, 描述取最常用的那个; dim_customer 只含有 ID 的顾客; dim_date 是连续日历, "
        "没交易的日子也有一行"
    )
    record(
        "transform",
        "pipeline.py",
        f"clean {len(clean):,} 行",
        ", ".join(f"{n} {len(df):,}" for n, df in tables.items()),
        "1 -> 4 张表",
        "加 date_key, revenue; 描述移到维度; 维度按键汇总; 连续日历",
    )
    return tables


def step_recommend(cfg, tables):
    header(
        4,
        TOTAL,
        "build_recommendations",
        "recommend.py",
        "共购规则 (按 lift 排序) + 冷门商品用描述 TF-IDF 补位",
    )
    fact, products, c = tables["fact_sales"], tables["dim_product"], cfg["recommend"]
    sizes = fact.groupby("invoice_no")["stock_code"].nunique()
    cap = c["max_basket_size"]
    log.info(
        f"  购物篮 (一张发票一个): {len(sizes):,} 个; 1 件 {int((sizes == 1).sum()):,}, "
        f"2..{cap} 件 {int(sizes.between(2, cap).sum()):,} (只有这些参与配对), "
        f"超过 {cap} 件 {int((sizes > cap).sum()):,}; 分母用全部购物篮"
    )
    top = timed(co_purchase_rules, fact, cfg)
    frame(
        f"co_purchase_rules(): 至少 {c['min_support_count']} 个篮子, confidence >= "
        f"{c['min_confidence']}, lift >= {c['min_lift']}, 每个商品取前 {c['top_n']}",
        top,
    )
    cold = timed(content_fallback, products, set(top.stock_code), cfg)
    usable = int((products.description != "UNKNOWN").sum())
    frame(f"content_fallback(): 有描述的 {usable:,} 个商品里没有共购规则的", cold)
    log.info("  recommend() 内部再算一遍上面两步, 所以下面的 [pipeline] 行会重复:")
    recs = timed(recommend, tables, cfg)
    frame("recommend() 合并, 加上两边的商品名", recs)
    by_method = recs.groupby("method").stock_code.nunique().to_dict()
    covered = recs.stock_code.nunique()
    log.info(
        f"  每种方法覆盖的商品数 {by_method}; 覆盖目录 {covered / len(products):.1%} "
        f"({covered:,} / {len(products):,})"
    )
    change(
        f"fact {len(fact):,} 行 -> {len(sizes):,} 个购物篮 (发票 -> 商品集合) -> 商品对"
        f"计数 -> {top.stock_code.nunique():,} 个商品有共购规则; 其余 "
        f"{cold.stock_code.nunique() if len(cold) else 0:,} 个用描述的余弦近邻补; "
        f"合并成 {len(recs):,} 行, 每行一个 (商品, 推荐商品, 名次)"
    )
    record(
        "build_recommendations",
        "recommend.py",
        f"fact_sales {len(fact):,} 行 + dim_product {len(products):,} 行",
        f"recommendations {len(recs):,} 行",
        f"{len(sizes):,} 篮 -> {len(recs):,} 条推荐",
        f"共购规则 + TF-IDF 补位; 覆盖 {covered / len(products):.1%}",
    )
    return recs


def step_adoption(cfg, version, run_id):
    header(
        5,
        TOTAL,
        "measure_adoption",
        "adoption.py",
        "使用遥测 -> 覆盖率 / 激活率 / 行动率 / CSAT",
    )
    events = load_events(cfg)
    frame(cfg["paths"]["usage_events"].name, events)
    as_of = resolve_as_of(events, cfg)
    eligible = eligible_events(events, as_of)
    log.info(
        f"  as_of {as_of} (config 里 analysis_as_of 当天结束); 在它之前的事件 "
        f"{len(eligible):,} / {len(events):,}; 类型 "
        f"{eligible.event_type.value_counts().to_dict()}"
    )
    tables = timed(measure_adoption, cfg, reports_dest=version, run_id=run_id)
    for name, df in tables.items():
        frame(name, df, sample=min(len(df), 12), max_cols=12)
    weekly, teams = tables["adoption_weekly"], tables["adoption_by_team"]
    change(
        f"{len(events):,} 条事件 (一行一次操作) -> 截到 as_of -> 三张汇总表: headline "
        f"{len(tables['adoption_headline'])} 行 (指标, 目标, 状态), weekly {len(weekly)} "
        f"行 (连续周历, 没人用的周也有一行), by_team {len(teams)} 行 (按花名册, "
        "没有事件的团队也有一行)"
    )
    record(
        "measure_adoption",
        "adoption.py",
        f"usage_events {len(events):,} 行",
        ", ".join(f"{n} {len(df)}" for n, df in tables.items()),
        f"{len(events):,} -> {sum(len(df) for df in tables.values())}",
        "事件 -> 指标; 连续周历; 按花名册列团队",
    )
    return tables


def step_run_metrics(
    cfg, raw, clean, quarantine, tables, recs, adoption, version, run_id, started
):
    header(
        6,
        TOTAL,
        "write_run_metrics",
        "pipeline.py + doc_figures.py",
        "报告版本的最后一个文件, 写在发布之前",
    )
    metrics = pipeline.write_run_metrics(
        cfg,
        n_raw=len(raw),
        n_quarantined=len(quarantine),
        n_clean=len(clean),
        n_products=len(tables["dim_product"]),
        recs=recs,
        adoption_headline=adoption["adoption_headline"],
        # Includes the trace's own logging; the comparison excuses it.
        compute_seconds=time.perf_counter() - started,
        dest=version,
        run_id=run_id,
    )
    log.info("  run_metrics.json:")
    log.info(textwrap.indent(json.dumps(metrics, indent=2, default=str), "      | "))
    figures = json.loads((version / "doc_figures.json").read_text(encoding="utf-8"))
    log.info(f"  doc_figures.json: {figures}")
    change(
        "不产生新表。前面几步的行数、推荐覆盖率、采用率和两个输入文件的指纹写进 "
        "run_metrics.json; 到这里报告版本才完整, 之后数据才允许发布"
    )
    record(
        "write_run_metrics",
        "pipeline.py + doc_figures.py",
        "前 5 步的结果",
        "run_metrics.json, doc_figures.json",
        "-",
        "计数 + 输入指纹; 报告完整后才发布数据",
    )


def step_publish(cfg, tables, quarantine, recs, adoption, run_id):
    header(
        7,
        TOTAL,
        "publish",
        "pipeline.py + publish.py",
        "唯一写仓库的一步: 一个版本目录, Parquet + SQLite, 最后换指针",
    )
    payload = {**tables, "quarantine": quarantine, "recommendations": recs, **adoption}
    timed(pipeline.load, payload, cfg, run_id=run_id)
    pipeline.mark_published(cfg, run_id)
    data = publish.published_data_dir(cfg)
    db = data / publish.WAREHOUSE_FILE
    with contextlib.closing(sqlite3.connect(db)) as conn:
        names = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        ]
        counts = {
            n: conn.execute(
                f"SELECT count(*) FROM {pipeline.quote_ident(n)}"
            ).fetchone()[0]
            for n in names
        }
        stamp = conn.execute("SELECT run_id, published_at FROM _publication").fetchone()
    log.info(
        f"  数据版本 {data.relative_to(cfg['paths']['data_runs'].parent)} (data/CURRENT 指向它):"
    )
    for name in sorted(payload):
        p = data / f"{name}.parquet"
        rows = pq.ParquetFile(p).metadata.num_rows
        ok = rows == counts.get(name) == len(payload[name])
        log.info(
            f"      {name:<20} parquet {rows:>8,} 行 {p.stat().st_size / 2**20:>6,.1f} MB"
            f" | sqlite {counts.get(name, 0):>8,} 行 | {'一致' if ok else '不一致'}"
        )
    log.info(
        f"  retail.db {db.stat().st_size / 2**20:,.1f} MB; 另有元数据表 "
        f"{[n for n in names if n not in payload]}; _publication = {stamp}"
    )
    change(
        f"{len(payload)} 张表在同一个版本目录里各写一份 Parquet 和 SQLite, SQLite 在一个事务"
        "里换表, 再记下发布清单 (_published) 和 run_id 戳 (_publication); 校验通过后改 "
        "data/CURRENT 这一个指针"
    )
    record(
        "publish",
        "pipeline.py + publish.py",
        f"{len(payload)} 张表 (内存)",
        f"data/runs/{data.name}/: {len(payload)} 个 parquet + retail.db",
        "不变",
        "Parquet + SQLite 同一版本; 一个事务; 换指针",
    )


def step_finalize(cfg, run_id):
    header(
        8,
        TOTAL,
        "finalize_reports",
        "publish.py",
        "报告版本设为当前, 和数据版本绑在一起",
    )
    outcome = publish.finalize_reports(cfg, run_id)
    publish.prune_report_versions(cfg)
    log.info(
        f"  finalize_reports: {outcome} | warehouse run {publish.warehouse_run_id(cfg)} "
        f"| reports run {publish.current_run_id(cfg)}"
    )
    log.info(f"  published/CURRENT.json: {publish.published_manifest(cfg)}")
    for p in sorted(publish.published_reports(cfg).iterdir()):
        log.info(f"      {p.name:<26} {p.stat().st_size:>8,} 字节")
    change(
        "数据不动。reports/CURRENT 指向这一版报告 (并抄一份到 reports/ 顶层), "
        "published/CURRENT.json 同时记下数据版本和报告版本, 下游 (dbt, Power BI) 只认它"
    )
    record(
        "finalize_reports",
        "publish.py",
        f"reports/runs/{run_id}/",
        "published/CURRENT.json",
        "-",
        "一个文件绑定数据版本和报告版本",
    )


def step_dbt(cfg, run_id, out):
    header(
        9,
        TOTAL,
        "dbt_build",
        "scripts/run_dbt.py + dbt/",
        "从已发布的版本建 DuckDB mart, 跑数据测试",
    )
    try:
        run_dbt._dbt_executable()
    except FileNotFoundError:
        log.info("  dbt 没装 (pip install -r requirements-dbt.txt), 跳过")
        record("dbt_build", "scripts/run_dbt.py", "-", "-", "-", "跳过: dbt 未安装")
        return

    real_run = subprocess.run
    lines = []

    def spy(cmd, check=False, **kw):
        res = real_run(
            cmd,
            **kw,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        text = ANSI.sub("", res.stdout + res.stderr)
        (out / "dbt_build.log").write_text(text, encoding="utf-8")
        lines.extend(text.splitlines())
        if check and res.returncode:
            for line in lines[-15:]:
                log.info(f"      | {line}")
            res.check_returncode()
        return res

    run_dbt.subprocess.run = spy
    try:
        timed(run_dbt.main, expected_run_id=run_id)
    finally:
        run_dbt.subprocess.run = real_run
    done = next((ln for ln in reversed(lines) if "Done." in ln), "")
    models = sum("OK created" in ln for ln in lines)
    tests = sum(" PASS " in ln for ln in lines)
    log.info(f"  dbt: {models} 个模型建成, {tests} 个测试通过; {done.strip()}")
    log.info(f"  完整输出在 {out / 'dbt_build.log'}")

    import duckdb

    pointer = json.loads((cfg["paths"]["dbt"] / "CURRENT.json").read_text("utf-8"))
    log.info(f"  dbt/CURRENT.json: {pointer}")
    marts = {}
    with contextlib.closing(
        duckdb.connect(str(cfg["paths"]["dbt"] / pointer["database"]), read_only=True)
    ) as con:
        objects = con.execute(
            "SELECT table_schema, table_name, table_type FROM information_schema.tables "
            "ORDER BY table_name"
        ).fetchall()
        for schema, name, kind in objects:
            df = con.execute(f'SELECT * FROM "{schema}"."{name}"').df()
            log.info(f"      {name:<24} {kind:<10} {len(df):>8,} 行 x {df.shape[1]} 列")
            if name.startswith("mart_"):
                marts[name] = df
    for name, df in marts.items():
        frame(name, df, sample=2, max_cols=12)
    change(
        "dbt 按 published/CURRENT.json 找到这次发布的 Parquet: staging 视图改名和选列 -> "
        "intermediate 算度量 -> 4 张 mart 表 (按天 / 商品 / 顾客汇总, 推荐), 写进新的 "
        "DuckDB 文件; 测试全过才改 dbt/CURRENT.json"
    )
    record(
        "dbt_build",
        "scripts/run_dbt.py + dbt/",
        "已发布的 Parquet + 报告",
        ", ".join(f"{n} {len(df):,}" for n, df in marts.items()),
        "汇总",
        f"{models} 个模型, {tests} 个测试",
    )


def step_bi(out, tables):
    header(
        10,
        TOTAL,
        "bi semantic model",
        "bi/build_star_schema.py",
        "仓库星型 -> Power BI 语义模型: 代理键, 未知成员, 整年日历",
    )
    model = out / "bi" / "model"
    real_out, build_star_schema.OUT = build_star_schema.OUT, model
    printed = io.StringIO()
    try:
        with contextlib.redirect_stdout(printed):
            timed(build_star_schema.main)
    finally:
        build_star_schema.OUT = real_out
    log.info("  build_star_schema 自己的输出:")
    log.info(textwrap.indent(printed.getvalue().strip(), "      | ", lambda _: True))
    built = {p.stem: pd.read_csv(p) for p in sorted(model.glob("*.csv"))}
    for name, df in built.items():
        frame(f"{name}.csv", df, sample=2, max_cols=12)
    log.info(
        f"  对照仓库: fact_sales {len(tables['fact_sales']):,} -> "
        f"{len(built['fact_sales']):,} 行; dim_customer {len(tables['dim_customer']):,} "
        f"-> {len(built['dim_customer']):,} (加 -1 Guest); dim_product "
        f"{len(tables['dim_product']):,} -> {len(built['dim_product']):,} (加 -1 "
        f"Non-product, 隔离表里不是商品的代码都归到它); dim_date {len(tables['dim_date']):,} -> "
        f"{len(built['dim_date']):,} 天 (补成整年)"
    )
    change(
        "自然键换成整数代理键; 缺失的外键指向 -1 未知成员而不是空值; 日历补成整年, 加"
        "排序列; 维度里的聚合列去掉; 多一张 fact_quarantine 和规则桥表; 写成 CSV 给 "
        "Power BI"
    )
    record(
        "bi semantic model",
        "bi/build_star_schema.py",
        "已发布的 Parquet",
        f"{len(built)} 个 CSV",
        f"fact_sales {len(built['fact_sales']):,} 行",
        "代理键; -1 未知成员; 整年日历; 去掉维度里的聚合",
    )


def step_evaluate(cfg, clean, out):
    header(11, TOTAL, "evaluate", "evaluate.py", "离线评估: 按时间切开, 测 hit-rate@5")
    result = timed(evaluate, clean, cfg)
    # The two fields evaluate.main() adds, so the file compares with the committed one
    result["source_sha256"] = hashlib.sha256(
        cfg["paths"]["raw"].read_bytes()
    ).hexdigest()
    result["environment"] = {
        "pandas": pd.__version__,
        "scikit-learn": package_version("scikit-learn"),
    }
    (out / "evaluation.json").write_text(
        json.dumps(result, indent=2, allow_nan=False), encoding="utf-8"
    )
    log.info(
        f"  训练 {result['training_rows']:,} 行 (到 {result['training_last_timestamp']}), "
        f"测试 {result['test_rows']:,} 行 (从 {result['test_first_timestamp']}); 跨切点"
        f"的发票 {result['cross_boundary_invoices_excluded']} 张两边都不用"
    )
    models = pd.DataFrame.from_dict(result["models"], orient="index")
    table(models.rename_axis("model").reset_index())
    hybrid = result["models"]["hybrid"]
    change(
        f"clean 按 {result['cutoff']} 切开; 训练部分重新跑 transform + recommend, 测试"
        f"部分按发票变成购物篮; 篮子里每个商品当一次查询 ({hybrid['queries']:,} 次), "
        f"看前 {result['k']} 个推荐有没有命中同篮的其他商品; 和最热门商品基线对比"
    )
    record(
        "evaluate",
        "evaluate.py",
        f"clean {len(clean):,} 行",
        "evaluation.json",
        f"训练 {result['training_rows']:,} / 测试 {result['test_rows']:,}",
        f"hit-rate@{result['k']} {hybrid['hit_rate_at_k']:.1%} vs 最热门 "
        f"{result['models']['most_popular']['hit_rate_at_k']:.1%}",
    )


def step_not_run():
    log.info("")
    log.info("=" * 78)
    log.info("没有运行的脚本 (不改变数据)")
    log.info("=" * 78)
    for script, why in (
        ("check_cancellations.py", "退货单配对对账; 配对数量已在第 2 步记录"),
        ("check_revenue_bridge.py", "和 R 版本的正向销售口径对比"),
        ("verify_doc_figures.py", "重算 doc_figures.json; 第 6 步已经写了一份"),
        ("build_workshop.py", "生成培训幻灯片"),
    ):
        log.info(f"  scripts/{script}: {why}")


# ---------------------------------------------------------------- checks


def _first_difference(a, b):
    for i, (x, y) in enumerate(zip(a, b)):
        if x != y:
            return i, x, y
    i = min(len(a), len(b))
    return i, (a[i:] or ["<结束>"])[0], (b[i:] or ["<结束>"])[0]


def compare_with_committed(cfg, out):
    log.info("")
    log.info("=" * 78)
    log.info("复现检查: trace 输出 vs 已提交的报告和已发布的数据")
    log.info("=" * 78)
    version = publish.published_reports(cfg)
    pairs = [(version / n, n) for n in (*publish.REPORT_NAMES, "doc_figures.json")]
    pairs.append((out / "evaluation.json", "evaluation.json"))
    for new, name in pairs:
        committed = _committed(ROOT / "reports" / name)
        if committed is None:
            log.info(f"  reports/{name}: HEAD 里没有这个文件")
            continue
        a = _normalise(new.read_text(encoding="utf-8")).splitlines()
        b = _normalise(committed).splitlines()
        if a == b:
            log.info(f"  reports/{name}: 一致 (run_id 和 compute_seconds 除外)")
        else:
            i, x, y = _first_difference(a, b)
            log.info(
                f"  reports/{name}: 不一致, 第 {i + 1} 行: trace {x!r} vs 已提交 {y!r}"
            )

    real = publish.published_data_dir(pipeline.load_config(ROOT / "config.yaml"))
    if real is None:
        log.info(
            "  data/ 下还没有已发布的数据版本, 跳过表的比对 "
            "(python -m retail_pipeline.pipeline 之后再跑)"
        )
        return
    log.info(f"  已发布的数据版本: {real.name}")
    for new in sorted(publish.published_data_dir(cfg).glob("*.parquet")):
        old = real / new.name
        if not old.exists():
            log.info(f"  {new.name}: 已发布的版本里没有")
            continue
        a, b = pd.read_parquet(new), pd.read_parquet(old)
        if a.equals(b):
            log.info(f"  {new.name}: 完全相同 ({len(a):,} 行)")
            continue
        try:
            pd.testing.assert_frame_equal(a, b, check_exact=False, rtol=1e-9)
            log.info(f"  {new.name}: 一致 ({len(a):,} 行)")
        except AssertionError as e:
            log.info(f"  {new.name}: 不一致: {str(e).splitlines()[0]}")


def write_lineage(out):
    lines = [
        "# Data lineage (trace run)",
        "",
        "| 步骤 | 模块 | 输入 | 输出 | 行数变化 | 结构上发生了什么 |",
        "|---|---|---|---|---|---|",
    ]
    lines += [
        f"| {r['step']} | `{r['module']}` | {r['input']} | {r['output']} | "
        f"{r['rows']} | {r['change']} |"
        for r in lineage
    ]
    (out / "data_lineage.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"trace_{stamp}_{git_tag()}"
    out = ROOT / "data" / "trace" / run_id
    setup(out)
    log.info(f"trace run {run_id}; 所有输出写在 {out}")

    t0 = time.perf_counter()
    cfg = make_project(out)
    step_config(cfg)
    version = publish.reports_dir(cfg, run_id)
    raw = step_extract(cfg)
    clean, quarantine = step_quality(cfg, raw, version, run_id)
    tables = step_transform(clean)
    recs = step_recommend(cfg, tables)
    adoption = step_adoption(cfg, version, run_id)
    step_run_metrics(
        cfg, raw, clean, quarantine, tables, recs, adoption, version, run_id, t0
    )
    step_publish(cfg, tables, quarantine, recs, adoption, run_id)
    step_finalize(cfg, run_id)
    step_dbt(cfg, run_id, out)
    step_bi(out, tables)
    step_evaluate(cfg, clean, out)
    step_not_run()
    write_lineage(out)
    compare_with_committed(cfg, out)
    log.info("")
    log.info(f"完成, 共 {time.perf_counter() - t0:,.0f} s; 日志 {out / 'trace.log'}")


if __name__ == "__main__":
    main()
