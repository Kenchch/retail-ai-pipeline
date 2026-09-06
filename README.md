# Retail Data Pipeline & Product Recommendations

[![CI](https://github.com/Kenchch/retail-ai-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Kenchch/retail-ai-pipeline/actions/workflows/ci.yml)

A retail analytics portfolio pipeline: invoice lines become a quality-checked
sales star schema, product recommendations and a Power BI semantic model.
It demonstrates engineering and adoption reporting; it has not been deployed,
and all usage telemetry is simulated.

## What I built

- Row-level quality rules and quarantine with source-row reconciliation.
- Per-run staging and atomic publication through `published/CURRENT.json`.
- Basket associations with description-based recommendation fallback.
- A contracted dbt/DuckDB daily sales mart and a Power BI model.
- An Airflow DAG, failure recovery tests and reproducible simulated adoption reports.

## Results and evidence

| Measure | Committed full-data result |
|---|---:|
| Input invoice lines | 541,909 |
| Loaded / quarantined | 522,566 / 19,343 (3.57%) |
| Gross accepted positive sales | £10,247,353.28 |
| Accepted invoices | 19,773 |
| Products / recommendation rows | 3,803 / 17,083 |

Evidence: [run metrics](reports/run_metrics.json), [quality report](reports/data_quality_report.md),
[Power BI model](bi/MODEL.md) and [report pages](bi/README.md).

The [R companion analysis](https://github.com/Kenchch/online-retail-analysis-r)
reports £9,883,659.86 after matching cancellations. It retains duplicates and
uses a different acceptance policy; its figure is not this pipeline's revenue.
The detailed revenue bridge is in [design notes](docs/DESIGN.md#reconciliation-with-online-retail-analysis-r).

## Temporal recommendation evaluation

Training uses accepted lines before 2011-10-01; evaluation uses later baskets.
Each product in a basket with at least two distinct products is a query, and a
hit retrieves another product in that basket. All three models use training data only.

| Model | Hit-rate@5 | Query coverage |
|---|---:|---:|
| Hybrid | 56.30% | 95.19% |
| Most popular | 52.84% | 100.00% |
| Content TF-IDF | 51.66% | 95.19% |

The 161,844 queries are basket-completion queries, not independent customers.
This offline comparison does not measure sales uplift. The
[evaluation artifact](reports/evaluation.json) records counts, cutoff, source hash
and library versions. Reproduce with `python -m retail_pipeline.evaluate` after
fetching the data. The [revenue reconciliation](reports/reconciliation.json) can
be regenerated with `python scripts/check_revenue_bridge.py`.

## Run it

```bash
python -m pip install -r requirements.txt
python scripts/get_data.py
python -m retail_pipeline.pipeline
pytest -q

# dbt consumer; install on every Airflow worker too
python -m pip install -r requirements-dbt.txt
python scripts/run_dbt.py
```

Airflow additionally requires `requirements-airflow.txt` and its versioned release
constraints. See [runtime and orchestration details](docs/DESIGN.md#how-it-works).

## Limits

- Publication uses a single machine and local storage.
- Warehouse and downstream dbt promotion are separate transaction boundaries.
- Revenue is gross accepted positive sales; cancellation netting is in the R project.
- Offline associations do not establish recommendation impact or causal uplift.
- Adoption metrics use generated telemetry, not real users.
- Power BI setup and the workshop describe a portfolio scenario.

## Data and licence

Code is MIT. UCI Online Retail (Chen, 2015) is CC BY 4.0.
The downloader uses Databricks' Spark: The Definitive Guide CSV mirror and verifies
a pinned SHA-256. Raw data is downloaded locally and excluded from Git.
See [NOTICE](NOTICE) for attribution and transformations.

[Design notes](docs/DESIGN.md) · [User guide](docs/02_user_guide.md) ·
[Adoption and communication](docs/03_adoption_and_comms.md)

Documentation examples are computed in [doc_figures.json](reports/doc_figures.json).
After downloading the source, run `python scripts/verify_doc_figures.py`, then
`python scripts/build_workshop.py` (requires python-pptx) to refresh the editable
workshop. Its PDF is a rendered preview; the PPTX retains editable text and charts.

## How this was built

I set the problem, the data contracts and the quality rules, ran the benchmarks
and reviewed every diff; Claude Code and OpenAI Codex drafted code, refactored
and scaffolded tests. The full note — including the `Co-Authored-By` trailers
removed from this repository's history on 6 September 2026 — is on my profile:
[How I use AI tools](https://github.com/Kenchch/Kenchch#how-i-use-ai-tools).
