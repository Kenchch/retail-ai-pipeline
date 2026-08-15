# Power BI semantic layer

The pipeline in this repo publishes a warehouse star schema. This folder turns
it into a **Power BI semantic model**: surrogate keys, unknown members, a
calendar that supports time intelligence, a second fact table for data quality,
a many-to-many bridge, a DAX measure library and dynamic row-level security.

**To build the .pbix you need Power BI Desktop and nothing else** — the nine
CSVs in [`model/`](model/) are already generated, and they still reconcile to the
numbers in the repo's own `reports/run_metrics.json`. Follow
[`BUILD_POWERBI.md`](BUILD_POWERBI.md), about 45 minutes.

To regenerate the CSVs after a data or pipeline change:

```bash
python -m retail_pipeline.pipeline   # produces data/processed/*.parquet
python -m bi.build_star_schema       # produces bi/model/*.csv
```

The build script validates before it writes: referential integrity on every
foreign key, uniqueness on every dimension key, and contiguity on the calendar.
It exits non-zero rather than publishing a model with orphans in it.

| | |
|---|---|
| [`MODEL.md`](MODEL.md) | The design and the ten decisions behind it — read this one |
| [`BUILD_POWERBI.md`](BUILD_POWERBI.md) | Click-by-click build, ~45 min, with the numbers to validate against |
| [`measures.dax`](measures.dax) | 30 measures and the three RLS role expressions |
| [`build_star_schema.py`](build_star_schema.py) | Warehouse star → semantic model |
| [`model/`](model/) | The output: 9 CSVs, ~23 MB |

## The model

```
                dim_date ────┬──── fact_sales ────┬──── dim_product
                             │     522,566 lines  │
             dim_customer ───┤                    ├──── dim_country
                             │                    │
                             └── fact_quarantine ─┘
                                  19,343 rows
                                       │
                        bridge_quarantine_rule (30,739)
                                       │
                              dim_quality_rule (9)
```

Two fact tables on conformed dimensions, so one set of slicers answers both
"what sold" and "what we rejected and why".

Reconciles to **£10,247,353.28** over **19,773 orders**.
522,566 loaded + 19,343 rejected = **541,909**, the row count of the source
extract — the one check that proves nothing was dropped silently.

## The four decisions worth arguing about

**Guest checkout gets a dimension member, not a null.** 25.1% of sales lines
have no customer id. A null foreign key becomes a blank dimension row, and a
blank row does not appear in a slicer — so every customer-sliced visual quietly
loses £1.51M and nothing says so. `CustomerKey = -1` makes the gap filterable
and measurable instead of invisible.

**The calendar covers whole years, not the observed range.** The data starts
2010-12-01. `SAMEPERIODLASTYEAR` over a calendar that starts there returns
blank for eleven months of 2011, which reads as "no data yet" rather than "your
date table is too short". The calendar runs 2010-01-01 to 2011-12-31 and flags
the 305 days actually traded.

**Aggregates come out of the dimensions.** `dim_product.revenue` and
`dim_customer.total_revenue` are facts in dimension tables: filter the page to
March, drag one onto a card, get the all-time number. Measures replace them.

**Rule is many-to-many, so it gets a bridge.** A rejected row breaks 1.59 rules
on average. Stored as a comma-separated string it cannot be sliced. The bridge
makes the grain explicit — and makes it explicit that rejected rows sliced by
rule correctly do *not* sum to the total.

Full reasoning, including what the model deliberately does **not** do
(no SCD Type 2, no aggregations, no incremental refresh) in
[`MODEL.md`](MODEL.md).

## Row-level security

One dynamic role driven by a mapping table, not a static role per market.
Three tables carry a filter, not one: filtering `dim_country` alone secures the
numbers and leaks the names, because dimensions are not filtered by the fact
and a UK analyst would still see all 4,334 customers in a slicer.

An unmapped account sees nothing and is told so. The `*` wildcard puts head
office through the same mechanism rather than a second unfiltered role, so
there is one place to look when someone asks who can see what.

> `model/security_user_country.csv` is **sample data with placeholder
> addresses**. In production it is a view over the entitlements system,
> refreshed with the model.
