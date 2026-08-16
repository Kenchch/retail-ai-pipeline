# Screenshots

| File | What it shows |
|---|---|
| `model-view.png` | Model view — nine tables, nine relationships, cardinality and filter direction on each |
| `page1-sales.png` | Sales overview — revenue, orders, AOV, customers; revenue by month vs last year, by country, by product |
| `page2-customers.png` | Customers — value segments including guest checkout, top customers, concentration |
| `page3-quality.png` | Data quality — extracted vs loaded vs quarantined, rejection rate vs abort threshold, rejections by rule |

Captured from `RetailSales.pbix`, which is not committed: a .pbix is a binary
zip that changes wholesale on every save, so the repo carries the model
definition ([`../measures.dax`](../measures.dax),
[`../BUILD_POWERBI.md`](../BUILD_POWERBI.md)) and the pictures instead.
