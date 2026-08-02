# Data quality report

- Rows read from source: **541,909**
- Rows quarantined (failed a blocking rule): **19,343** (3.57%)
- Rows loaded to the warehouse: **522,566**
- Outlier band used for reference: p99.9 of unit price

| Check | Dimension | Blocking | Failed rows | % of source | What it means |
|---|---|---|---|---|---|
| `duplicate_line_items` | uniqueness | yes | 5,270 | 0.97% | Identical invoice / product / qty / price / timestamp row appearing more than once |
| `missing_invoice_key` | completeness | yes | 0 | 0.00% | Invoice number, stock code or timestamp is null - row cannot be modelled |
| `cancelled_invoice` | validity | yes | 9,288 | 1.71% | Invoice number prefixed with 'C' - a cancellation, not a sale |
| `non_positive_quantity` | validity | yes | 10,624 | 1.96% | Quantity is null, zero or negative (returns and stock adjustments) |
| `non_positive_price` | validity | yes | 2,521 | 0.47% | Unit price is null or below the minimum sellable price |
| `price_outlier` | validity | yes | 120 | 0.02% | Unit price above the configured cap - almost always an adjustment line |
| `non_product_stock_code` | consistency | yes | 2,916 | 0.54% | Stock code is postage / bank charge / manual adjustment, not a sellable product |
| `missing_description` | completeness | no | 1,454 | 0.27% | Product description is blank - degrades the recommender but not the sales facts |
| `missing_customer_id` | completeness | no | 135,080 | 24.93% | No customer id - guest checkout; usable for basket analysis, not for customer analytics |

Rows may fail more than one check, so the column above does not sum to the
quarantined total. Non-blocking failures are loaded and flagged for the
downstream consumer to decide on.
