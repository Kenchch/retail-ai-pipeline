# Data quality report

`run_id: local_20260819T094207619994`

- Rows read: **541,909**
- Quarantined (failed a blocking rule): **19,343** (3.57%)
- Loaded: **522,566**

| Check | Dimension | Blocking | Failed | % | What it means |
|---|---|---|---|---|---|
| `duplicate_line_items` | uniqueness | yes | 5,270 | 0.97% | Same invoice/product/qty/price/timestamp twice - double-counts revenue |
| `missing_invoice_key` | completeness | yes | 0 | 0.00% | Invoice, product or timestamp is null - the row cannot be modelled |
| `cancelled_invoice` | validity | yes | 9,288 | 1.71% | 'C'-prefixed invoices are cancellations, not sales |
| `non_positive_quantity` | validity | yes | 10,624 | 1.96% | Returns and stock adjustments |
| `non_positive_price` | validity | yes | 2,521 | 0.47% | Zero-price giveaways and manual corrections |
| `price_outlier` | validity | yes | 120 | 0.02% | Above the configured cap - almost always an adjustment line |
| `non_product_stock_code` | consistency | yes | 2,916 | 0.54% | POST, BANK CHARGES, M - real rows, but not sellable products |
| `missing_description` | completeness | no | 1,454 | 0.27% | Degrades the recommender, not the sales facts |
| `missing_customer_id` | completeness | no | 135,080 | 24.93% | Guest checkout - fine for basket analysis, not for customer analytics |

Rows can fail more than one check, so the column does not sum to the total.
