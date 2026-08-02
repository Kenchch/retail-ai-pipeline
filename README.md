# Retail Data Pipeline & Product Recommendations

An end-to-end ETL pipeline over real retail transaction data: it ingests raw
invoice lines, profiles and enforces data quality, publishes a sales star
schema, and builds a "frequently bought together" recommendation table on top
of it.

The point of the project is the *data engineering*, not the model. Retail
transaction data arrives full of cancellations, returns, postage lines and
duplicate rows; the pipeline's job is to make those visible and handle them
explicitly before anything downstream consumes them.

---

## Results from a full run

Source: **UCI Online Retail** — 541,909 invoice lines from a UK online
giftware retailer, Dec 2010 – Dec 2011.

| | |
|---|---|
| Rows read from source | 541,909 line items across 25,900 invoices |
| Rows quarantined by data-quality rules | **19,343 (3.57%)** |
| Rows loaded to the warehouse | 522,566 line items / 19,773 invoices |
| Dimensions built | 3,803 products · 4,334 customers · 305 days |
| Revenue modelled | £10.25M |
| Recommendations produced | 17,300 rows covering 3,800 products (99.9% of catalogue) |
| End-to-end runtime | ~25 s on a laptop |

Sample output — the strongest associations the pipeline finds are exactly the
ones a merchandiser would expect, which is the cheapest sanity check there is:

```
LANDMARK FRAME COVENT GARDEN  ->  LANDMARK FRAME OXFORD STREET   lift 322.7   confidence 0.77
CHILDS GARDEN SPADE BLUE      ->  CHILDS GARDEN SPADE PINK       lift 234.1   confidence 0.66
KIDS RAIN MAC BLUE            ->  KIDS RAIN MAC PINK             lift 233.6   confidence 0.82
```

---

## Architecture

```
 data/raw/*.csv
      |
      v
 [1] extract .............. standardise names + types; nothing dropped yet
      |
      v
 [2] data quality ......... 9 rules across 4 dimensions
      |                     failing rows -> quarantine table (with reasons)
      |                     run refuses to load if the failure rate spikes
      v
 [3] transform ............ star schema: fact_sales + dim_product /
      |                     dim_customer / dim_date
      v
 [4] load ................. Parquet (analytics layer) + SQLite (serving layer)
      |
      v
 [5] recommend ............ co-purchase rules (support / confidence / lift)
                            + TF-IDF description fallback for cold-start products
      |
      v
 reports/data_quality_report.md   reports/run_metrics.json
```

Stages are separate importable modules, which is what lets the same code run
as a single script locally and as four independent Airflow tasks in
production (`dags/retail_pipeline_dag.py`).

---

## Data quality

Nine rules across four dimensions. Each is a small pure function returning a
mask of failing rows, declared in one table in `src/retail_pipeline/quality.py`
— adding a check is a one-line change and the report shape never drifts.

| Check | Dimension | Blocking | Failed | Why it matters |
|---|---|---|---|---|
| `duplicate_line_items` | uniqueness | yes | 0.97% | Same invoice/product/qty/price/timestamp twice — double-counts revenue |
| `missing_invoice_key` | completeness | yes | 0.00% | Row cannot be modelled at all |
| `cancelled_invoice` | validity | yes | 1.71% | `C`-prefixed invoices are cancellations, not sales |
| `non_positive_quantity` | validity | yes | 1.96% | Returns and stock adjustments |
| `non_positive_price` | validity | yes | 0.47% | Zero-price giveaways and manual corrections |
| `price_outlier` | validity | yes | 0.02% | Above the configured cap — almost always an adjustment line |
| `non_product_stock_code` | consistency | yes | 0.54% | `POST`, `BANK CHARGES`, `M` — real rows, but not products |
| `missing_description` | completeness | no | 0.27% | Degrades the recommender, not the sales facts |
| `missing_customer_id` | completeness | no | 24.93% | Guest checkout — fine for basket analysis, not for customer analytics |

Two design decisions worth calling out:

**Blocking vs non-blocking.** A quarter of all rows have no customer id. Dropping
them would throw away a quarter of the basket data to satisfy a rule that only
customer-level analytics cares about, so the rule reports and flags instead of
rejecting. Cancellations, on the other hand, are silently wrong if they reach a
sales fact table, so they are blocked.

**Quarantine, not delete.** Rejected rows are written to a `quarantine` table
with a `quarantine_reasons` column listing every rule they broke. That turns
rejects into something a data steward can actually work through, and makes the
pipeline reversible — a rule that turns out to be too aggressive can be relaxed
and the rows re-admitted.

The pipeline **fails the run** if the quarantine rate exceeds
`quality.max_quarantine_rate`, so a broken upstream extract stops before it
reaches the warehouse rather than quietly publishing thin data.

---

## Data model

```
             dim_product                dim_customer
          (stock_code PK)             (customer_id PK)
                 ^                           ^
                 |                           |
                 +--------- fact_sales ------+
                            (line items)
                                 |
                                 v
                            dim_date
                          (date_key PK)
```

`fact_sales` holds measures and foreign keys only — `quantity`, `unit_price`,
`revenue` — with every descriptive attribute in a dimension, so a product
renamed once is renamed everywhere. BI slices sales by product / customer /
time; the recommender reads only invoice + product. Both read the same
conformed dimensions.

Output is written twice: **Parquet** in `data/processed/` for the analytics
layer (columnar, compressed — what a Spark, Databricks or Synapse job would
read) and **SQLite** in `data/warehouse/` as a zero-infrastructure stand-in for
the serving database, indexed on the join keys. Swapping SQLite for Azure SQL
or Postgres is a change of connection string.

---

## Recommendations

**Co-purchase rules (structured data).** For every product pair appearing in the
same invoice:

```
support(A,B)     = baskets with both / all baskets
confidence(A→B)  = baskets with both / baskets with A
lift(A,B)        = confidence(A→B) / (baskets with B / all baskets)
```

Rules are ranked by **lift**, not raw co-occurrence, because raw counts just
re-rank the best sellers — a popular product co-occurs with everything, which
makes for useless recommendations. `lift > 1` means B is genuinely more likely
in a basket that already contains A than in a random basket.

From 16,782 usable baskets the pipeline counts 1.4M distinct pairs, of which
9,367 clear the minimum support and 9,573 directional rules clear the
confidence and lift thresholds.

**Description similarity (unstructured data).** 2,670 long-tail products never
appear in a qualifying pair and would get no recommendation at all. For those,
TF-IDF over the free-text product description plus cosine nearest neighbours
fills the slots with something defensible. Combining both signals takes
catalogue coverage from 30% to **99.9%**, and every row carries a `method`
column so a consumer can tell a behavioural rule from a content fallback.

All thresholds live in `config.yaml`.

---

## Running it

```bash
pip install -r requirements.txt
python scripts/download_data.py     # ~45 MB into data/raw/
python run_pipeline.py              # ~25 s end to end
pytest -q                           # 12 tests
```

Outputs:

```
data/processed/*.parquet         fact + dimensions + recommendations
data/warehouse/retail.db         queryable SQLite warehouse
reports/data_quality_report.md   per-rule failure counts for the run
reports/run_metrics.json         row counts, coverage, runtime
```

Query the warehouse directly:

```sql
SELECT p.description, r.recommended_description, r.lift, r.confidence
FROM   recommendations r
JOIN   dim_product p ON p.stock_code = r.stock_code
WHERE  r.method = 'co_purchase'
ORDER  BY r.lift DESC
LIMIT  10;
```

---

## Orchestration

`dags/retail_pipeline_dag.py` schedules the same modules as four Airflow tasks
(nightly, after end-of-day close) rather than one monolithic task — that is what
gives per-stage retries, per-stage runtime in the UI, and a failure that points
at the stage that broke. Dataframes move between tasks through the Parquet
layer on shared storage, not through XCom, which is a metadata channel and the
wrong place for half a million rows.

The Azure Data Factory equivalent is a pipeline with the same activities
chained on success and the quality activity's failure path wired to an alert;
the Python modules would be unchanged.

---

## Tests

```
tests/test_quality.py             every rule fires the expected number of times;
                                  the quarantine gate actually raises;
                                  non-blocking failures are not dropped
tests/test_transform_recommend.py star schema key uniqueness and referential
                                  integrity; revenue arithmetic; lift computed
                                  correctly on a hand-built example
```

The quality tests are the ones that matter most here: if a rule silently stops
firing, nothing crashes — bad rows just start flowing into the warehouse.

---

## Repository layout

```
config.yaml                       all paths, thresholds and rules
run_pipeline.py                   entry point
scripts/download_data.py          fetch the source dataset
src/retail_pipeline/
    config.py                     config loading + logging
    extract.py                    [1] read and standardise
    quality.py                    [2] rules, quarantine, report
    transform.py                  [3] star schema
    load.py                       [4] Parquet + SQLite
    recommend.py                  [5] co-purchase + TF-IDF fallback
    pipeline.py                   orchestration and run metrics
dags/retail_pipeline_dag.py       Airflow schedule
tests/                            pytest suite
```

## Data source

UCI Machine Learning Repository — *Online Retail* (Chen, D., 2015).
Transactions of a UK-based online retailer, 01/12/2010–09/12/2011.
