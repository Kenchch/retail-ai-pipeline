"""Stage 2 - Data quality.

Each rule is a small pure function that takes the dataframe and returns a
boolean mask of *failing* rows. Rules are declared in one table so adding a new
check is a one-line change, and every run produces the same report shape - which
is what makes the results comparable over time.

Rules are either:
  * blocking      - failing rows are quarantined and never reach the warehouse
  * non-blocking  - failing rows are counted and reported, but still loaded
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd

from .config import Config, get_logger

log = get_logger(__name__)

Rule = Callable[[pd.DataFrame, Config], pd.Series]


@dataclass(frozen=True)
class Check:
    name: str
    dimension: str  # uniqueness | completeness | validity | consistency
    blocking: bool
    description: str
    fn: Rule


# --------------------------------------------------------------------------- #
# Rule implementations - each returns True for rows that FAIL the rule
# --------------------------------------------------------------------------- #

def _duplicate_rows(df: pd.DataFrame, cfg: Config) -> pd.Series:
    key = ["invoice_no", "stock_code", "quantity", "unit_price", "invoice_ts"]
    return df.duplicated(subset=key, keep="first")


def _missing_customer_id(df: pd.DataFrame, cfg: Config) -> pd.Series:
    return df["customer_id"].isna()


def _missing_description(df: pd.DataFrame, cfg: Config) -> pd.Series:
    return df["description"].isna() | (df["description"].fillna("").str.len() == 0)


def _missing_invoice_key(df: pd.DataFrame, cfg: Config) -> pd.Series:
    return df["invoice_no"].isna() | df["stock_code"].isna() | df["invoice_ts"].isna()


def _cancelled_invoice(df: pd.DataFrame, cfg: Config) -> pd.Series:
    return df["invoice_no"].fillna("").str.upper().str.startswith("C")


def _non_positive_quantity(df: pd.DataFrame, cfg: Config) -> pd.Series:
    return df["quantity"].isna() | (df["quantity"] <= 0)


def _non_positive_price(df: pd.DataFrame, cfg: Config) -> pd.Series:
    return df["unit_price"].isna() | (df["unit_price"] < cfg.quality["min_unit_price"])


def _price_outlier(df: pd.DataFrame, cfg: Config) -> pd.Series:
    return df["unit_price"] > cfg.quality["max_unit_price"]


def _non_product_code(df: pd.DataFrame, cfg: Config) -> pd.Series:
    codes = {c.upper() for c in cfg.quality["non_product_codes"]}
    return df["stock_code"].fillna("").str.upper().isin(codes)


CHECKS: list[Check] = [
    Check("duplicate_line_items", "uniqueness", True,
          "Identical invoice / product / qty / price / timestamp row appearing more than once",
          _duplicate_rows),
    Check("missing_invoice_key", "completeness", True,
          "Invoice number, stock code or timestamp is null - row cannot be modelled",
          _missing_invoice_key),
    Check("cancelled_invoice", "validity", True,
          "Invoice number prefixed with 'C' - a cancellation, not a sale",
          _cancelled_invoice),
    Check("non_positive_quantity", "validity", True,
          "Quantity is null, zero or negative (returns and stock adjustments)",
          _non_positive_quantity),
    Check("non_positive_price", "validity", True,
          "Unit price is null or below the minimum sellable price",
          _non_positive_price),
    Check("price_outlier", "validity", True,
          "Unit price above the configured cap - almost always an adjustment line",
          _price_outlier),
    Check("non_product_stock_code", "consistency", True,
          "Stock code is postage / bank charge / manual adjustment, not a sellable product",
          _non_product_code),
    Check("missing_description", "completeness", False,
          "Product description is blank - degrades the recommender but not the sales facts",
          _missing_description),
    Check("missing_customer_id", "completeness", False,
          "No customer id - guest checkout; usable for basket analysis, not for customer analytics",
          _missing_customer_id),
]


def run_checks(df: pd.DataFrame, cfg: Config) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (results table, per-row failure flags)."""
    flags = pd.DataFrame(index=df.index)
    rows = []
    for check in CHECKS:
        mask = check.fn(df, cfg).fillna(True)
        flags[check.name] = mask
        rows.append(
            {
                "check": check.name,
                "dimension": check.dimension,
                "blocking": check.blocking,
                "failed_rows": int(mask.sum()),
                "failed_pct": round(100 * mask.mean(), 3),
                "description": check.description,
            }
        )
        log.info(
            "  %-24s %-13s %9s rows (%5.2f%%) %s",
            check.name, check.dimension, f"{int(mask.sum()):,}", 100 * mask.mean(),
            "[blocking]" if check.blocking else "",
        )
    return pd.DataFrame(rows), flags


def split_quarantine(
    df: pd.DataFrame, flags: pd.DataFrame, cfg: Config
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split the frame into rows that may be loaded and rows that are quarantined."""
    blocking = [c.name for c in CHECKS if c.blocking]
    failed_any = flags[blocking].any(axis=1)

    quarantine = df[failed_any].copy()
    # Record *why* each quarantined row was held back - this is what makes the
    # quarantine table actionable rather than just a bucket of rejects.
    quarantine["quarantine_reasons"] = (
        flags.loc[failed_any, blocking]
        .apply(lambda r: ",".join(r.index[r.to_numpy()]), axis=1)
    )

    clean = df[~failed_any].copy()

    rate = failed_any.mean()
    log.info(
        "Quarantined %s of %s rows (%.2f%%); %s rows pass to the warehouse",
        f"{int(failed_any.sum()):,}", f"{len(df):,}", 100 * rate, f"{len(clean):,}",
    )
    if rate > cfg.quality["max_quarantine_rate"]:
        raise ValueError(
            f"Quarantine rate {rate:.1%} exceeds the configured ceiling "
            f"{cfg.quality['max_quarantine_rate']:.1%} - refusing to load. "
            "Investigate the source extract before re-running."
        )
    return clean, quarantine


def write_report(
    results: pd.DataFrame, cfg: Config, n_rows_in: int, n_rows_out: int, n_quarantined: int
) -> str:
    """Render a plain-Markdown data quality report next to the run outputs."""
    p999 = cfg.quality["price_outlier_quantile"]
    lines = [
        "# Data quality report",
        "",
        f"- Rows read from source: **{n_rows_in:,}**",
        f"- Rows quarantined (failed a blocking rule): **{n_quarantined:,}** "
        f"({100 * n_quarantined / n_rows_in:.2f}%)",
        f"- Rows loaded to the warehouse: **{n_rows_out:,}**",
        f"- Outlier band used for reference: p{p999 * 100:g} of unit price",
        "",
        "| Check | Dimension | Blocking | Failed rows | % of source | What it means |",
        "|---|---|---|---|---|---|",
    ]
    for _, r in results.iterrows():
        lines.append(
            f"| `{r['check']}` | {r['dimension']} | {'yes' if r['blocking'] else 'no'} "
            f"| {r['failed_rows']:,} | {r['failed_pct']:.2f}% | {r['description']} |"
        )
    lines += [
        "",
        "Rows may fail more than one check, so the column above does not sum to the",
        "quarantined total. Non-blocking failures are loaded and flagged for the",
        "downstream consumer to decide on.",
        "",
    ]
    text = "\n".join(lines)
    out = cfg.paths["reports"] / "data_quality_report.md"
    out.write_text(text, encoding="utf-8")
    log.info("Data quality report written to %s", out)
    return text
