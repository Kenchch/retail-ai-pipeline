# Building the .pbix

About 45 minutes. **Power BI Desktop is the only requirement** — no Python, no
repo, no raw data. The nine CSVs in [`model/`](model/) are the build output and
they are already here.

Work through it in order; relationships before measures, measures before
visuals. Every step ends with something you can check, and the numbers to check
against are in [§11](#11-validation).

---

## 0. Put the folder where it belongs

**`bi/` is not in the GitHub repo.** It never was — `.gitignore` keeps the 22 MB
of model CSVs out, and the folder itself was never committed. Cloning
`Kenchch/retail-ai-pipeline` on this machine gives you the pipeline and nothing
of the Power BI work. This zip is the only carrier.

1. Make sure the repo on this machine is current:

```powershell
cd D:\projects\retail-ai-pipeline
git pull
```

2. Unzip this package so that `bi\` lands inside the repo:

```
D:\projects\retail-ai-pipeline\bi\model\fact_sales.csv      22 MB, the big one
D:\projects\retail-ai-pipeline\bi\measures.dax               30 measures + 3 RLS expressions
D:\projects\retail-ai-pipeline\bi\BUILD_POWERBI.md           this file
D:\projects\retail-ai-pipeline\bi\MODEL.md                   design notes - read before an interview
D:\projects\retail-ai-pipeline\bi\screenshots\              empty, for step 13
```

Anywhere else works for building — §1 makes the path a parameter, so nothing in
the model is tied to a location. Inside the repo is where it needs to be for
§13's commit, and for §12 if you ever regenerate.

> **Do not run the Python build.** The CSVs in `model/` are already generated and
> still current: the repo's own `reports/run_metrics.json` reports 541,909 rows
> read · 19,343 quarantined (3.57%) · 522,566 loaded · 3,803 products · 17,083
> recommendations, and every one of those matches the CSVs shipped here. The
> pipeline has had commits since these were built — the most recent hardens the
> SQLite load path — but none of them change the published numbers or the shape
> of `data/processed/*.parquet` that the model is derived from. See
> [§12](#12-if-you-ever-do-need-to-regenerate).

## 1. Create the folder parameter first

Do this before loading anything. It is what makes the file open on another
machine — the reason you are reading this version of the document.

**Home → Transform data → Manage parameters → New:**

| Field | Value |
|---|---|
| Name | `ModelFolder` |
| Type | Text |
| Suggested values | Any value |
| Current value | `D:\projects\retail-ai-pipeline\bi\model` |

Leave Power Query open; §2 uses it.

Every query built in §2 refers to `ModelFolder` instead of a hard-coded path.
Moving the model to a new machine then means editing one parameter rather than
nine Source steps.

## 2. Load the tables

Still in the Power Query window: **New Source → Blank Query**, then
**Home → Advanced Editor**, and paste:

```m
let
    Source = Csv.Document(
        File.Contents(ModelFolder & "\dim_date.csv"),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
    Promoted
```

Rename the query `dim_date`. Then repeat for the other eight — right-click the
query → **Duplicate**, open Advanced Editor, change the filename and the query
name:

```
dim_date.csv                  dim_product.csv           dim_customer.csv
dim_country.csv               dim_quality_rule.csv      fact_sales.csv
fact_quarantine.csv           bridge_quarantine_rule.csv
security_user_country.csv
```

`fact_sales.csv` is 22 MB and imports in a few seconds; it compresses to well
under 10 MB in the model.

> Loading through **Get data → Text/CSV** also works and is fewer clicks, but it
> writes the absolute path into the Source step. If you go that route, edit each
> Source afterwards to use `ModelFolder & "\<file>.csv"` — same nine edits, done
> later instead of now.

Do **not** Close & Apply yet — set the types first.

## 3. Set data types

Power Query's guesses are right for most columns and wrong for the ones that
matter. Fix these explicitly, in Power Query, before applying:

| Table | Column | Type |
|---|---|---|
| `dim_date` | `Date` | **Date** (not Date/Time) |
| `dim_date` | `DateKey`, `Year`, `QuarterNo`, `MonthNo`, `MonthNameSort`, `YearMonthSort`, `DayOfMonth`, `DayOfWeekNo`, `DayNameSort`, `ISOWeek` | Whole number |
| `dim_date` | `IsWeekend`, `IsTradingDay` | True/False |
| `dim_product` | `ProductKey` | Whole number |
| `dim_product` | `StockCode` | **Text** — codes like `85123A` are not numbers |
| `dim_product` | `AvgUnitPrice` | Decimal number |
| `dim_product` | `FirstSoldDate`, `LastSoldDate` | Date |
| `dim_product` | `IsSellable` | True/False |
| `dim_customer` | `CustomerKey`, `CountryKey` | Whole number |
| `dim_customer` | `CustomerID` | **Text** — it is an identifier, and summing it is never the answer |
| `dim_customer` | `FirstOrderDate`, `LastOrderDate` | Date |
| `dim_customer` | `IsGuest` | True/False |
| `dim_country` | `CountryKey` | Whole number |
| `dim_country` | `IsDomestic` | True/False |
| `dim_quality_rule` | `RuleKey` | Whole number |
| `fact_sales` | `InvoiceNo` | **Text** — cancellations are `C536379` |
| `fact_sales` | `Date` | Date |
| `fact_sales` | `ProductKey`, `CustomerKey`, `CountryKey`, `Quantity` | Whole number |
| `fact_sales` | `UnitPrice`, `Revenue` | Decimal number |
| `fact_quarantine` | `QuarantineKey`, `ProductKey`, `CountryKey`, `Quantity` | Whole number |
| `fact_quarantine` | `Date` | Date |
| `fact_quarantine` | `UnitPrice`, `RejectedValue` | Decimal number |
| `bridge_quarantine_rule` | both columns | Whole number |
| `security_user_country` | `CountryKey` | Whole number |

Then **Close & Apply**.

Back in the report, **Column tools → Summarization → Don't summarize** on every
key column (`ProductKey`, `CustomerKey`, `CountryKey`, `RuleKey`,
`QuarantineKey`, `DateKey`). Left as-is, a key dragged into a visual sums, and
someone will eventually put "sum of CustomerKey" on a slide.

## 4. Hide what nobody should drag

**Model view**, right-click → Hide in report view:

- every `*Key` column on both fact tables
- `bridge_quarantine_rule` — the whole table
- `security_user_country` — the whole table
- `dim_date[DateKey]`, `dim_date[MonthNameSort]`, `dim_date[YearMonthSort]`, `dim_date[DayNameSort]`
- `dim_customer[CountryKey]`

Surrogate keys are plumbing. A field list that shows them invites exactly the
visuals you will later have to explain.

## 5. Relationships

**Model view.** Delete anything Power BI auto-detected, then create these by
dragging. All are one-to-many, single direction, from the dimension to the fact:

| From (one) | To (many) | Active |
|---|---|---|
| `dim_date[Date]` | `fact_sales[Date]` | yes |
| `dim_product[ProductKey]` | `fact_sales[ProductKey]` | yes |
| `dim_customer[CustomerKey]` | `fact_sales[CustomerKey]` | yes |
| `dim_country[CountryKey]` | `fact_sales[CountryKey]` | yes |
| `dim_date[Date]` | `fact_quarantine[Date]` | yes |
| `dim_product[ProductKey]` | `fact_quarantine[ProductKey]` | yes |
| `dim_country[CountryKey]` | `fact_quarantine[CountryKey]` | yes |
| `fact_quarantine[QuarantineKey]` | `bridge_quarantine_rule[QuarantineKey]` | yes |
| `dim_quality_rule[RuleKey]` | `bridge_quarantine_rule[RuleKey]` | yes |

**Do not create:**

- `dim_country` → `dim_customer`. Both it and `fact_sales` carry a country;
  relating both creates two paths from country to the fact, Power BI
  deactivates one without telling you, and country filters stop going where the
  diagram says. `dim_customer[Country]` stays a plain attribute.
- anything touching `security_user_country`. It is read as a table by the RLS
  expressions, not through a filter path.

**Leave every relationship single-direction.** If a measure seems to need
bidirectional, it needs `TREATAS` instead — see `Rejected Value` in
[`measures.dax`](measures.dax).

## 6. Mark the date table

Select `dim_date` → **Table tools → Mark as date table** → date column `Date`.

Skip this and `TOTALYTD` and `SAMEPERIODLASTYEAR` return plausible-looking
wrong numbers at year boundaries rather than erroring. It is the single
cheapest correctness step in the build.

## 7. Sort-by columns

Without these, a month slicer reads Apr, Aug, Dec, Feb — alphabetical, which
no one has ever wanted.

| Column | Sort by |
|---|---|
| `dim_date[MonthName]` | `MonthNameSort` |
| `dim_date[YearMonth]` | `YearMonthSort` |
| `dim_date[DayName]` | `DayNameSort` |

Select the column → **Column tools → Sort by column**.

The `PriceBand`, `CustomerSegment`, `Region` and `Action` values are prefixed
with a digit (`1. Under GBP 1`) so they sort correctly without a companion
column. Slightly ugly in a legend, and cheaper than four more sort columns.

## 8. Measures

**Home → Enter data**, name the table `_Measures`, leave the single column,
Load. Then in Model view delete `Column1` — the table stays, empty, and every
measure goes in it. Right-click it → **Move to top** so it sits above the data
tables in the field list.

Paste each measure from [`measures.dax`](measures.dax) via **Home → New
measure**, in file order — the later ones reference the earlier ones.

Set the format string on each as you go (**Measure tools → Format**); the
intended one is in the comment above every measure. A currency measure left on
"General" shows `10247353.28` on a card, and nobody reads that as ten million.

## 9. Row-level security

**Modelling → Manage roles → Create**, name it `Territory`. Add a filter to
three tables — `dim_country`, `dim_customer` and `security_user_country` —
pasting the expression for each from the bottom of
[`measures.dax`](measures.dax).

Then **Modelling → View as → Other user** and work through:

| Sign in as | Should see |
|---|---|
| `uk.analyst@example.com` | United Kingdom only |
| `dach.analyst@example.com` | Germany, Austria, Switzerland |
| `groupbi@example.com` | everything, via the `*` row |
| `nobody@example.com` | nothing, and a visual that says so |

Check the **customer slicer** in each case, not only the revenue card. A role
that filters the numbers but not the names is the failure that gets shipped.

## 10. Report pages

Three pages. The spec is deliberately short — the model is the piece of work,
and a report that fits on three pages is easier to talk through in an interview
than a twelve-page one you half remember.

**Page 1 — Sales overview.**
Cards: `Revenue`, `Orders`, `Average Order Value`, `Customers`.
Line chart: `Revenue` and `Revenue R3M` by `dim_date[YearMonth]`.
Bar: `Revenue` by `dim_product[Description]`, top 15 by `Product Revenue Rank`.
Map or bar: `Revenue` by `dim_country[Country]`.
Slicers: `dim_date[Year]`, `dim_country[Region]`, `dim_product[PriceBand]`.
Footer: a card with `Last Refresh`.

**Page 2 — Customers.**
Cards: `Customers`, `Guest Revenue %`, `Revenue Share of Top 20% Products`.
Bar: `Revenue` and `Revenue % of Total` by `dim_customer[CustomerSegment]` —
this is where the guest member earns its place, sitting visibly as its own bar
instead of vanishing from the chart.
Table: top customers by `Revenue` with `Orders` and `Average Order Value`.

**Page 3 — Data quality.**
Cards: `Rows Extracted`, `Rejected Rows`, `Quarantine Rate %`, `Rejected Value`.
Gauge: `Quarantine Rate %` against the 30% threshold that fails the load.
Bar: `Rule Violations` by `dim_quality_rule[RuleLabel]`, coloured by
`QualityDimension`.
Table: `dim_quality_rule` with `Action` and `Rationale`, plus `Rejected Rows`.
**Add a text box on this page** saying rejected rows sliced by rule sum to more
than the total because a row can break several rules — 1.59 on average. Nine
numbers that do not add to the total above them will be read as a bug
otherwise, and the person reading it will be right to ask.

## 11. Validation

Put these in a table visual with no filters and check them off.

| Measure | Expected |
|---|---:|
| `Revenue` | £10,247,353.28 |
| `Orders` | 19,773 |
| `Sales Lines` | 522,566 |
| `Customers` | 4,334 |
| `Products Sold` | 3,803 |
| `Average Order Value` | £518.25 |
| `Guest Revenue` | £1,510,677.49 |
| `Guest Revenue %` | 14.7% |
| `Rejected Rows` | 19,343 |
| `Rule Violations` | 30,739 |
| `Rules Broken per Rejected Row` | 1.59 |
| `Rows Extracted` | 541,909 |
| `Quarantine Rate %` | 3.57% |
| `Rejected Value` | −£499,605.35 |
| `Revenue Share of Top 20% Products` | 78.4% |

`Rows Extracted` matching 541,909 — the row count of the raw UCI file — is the
one that matters most. It is the only check that proves nothing was dropped
silently between the CSV and the model.

**A load check before the measures**, new in this version because the folder
travelled between machines: the row counts should read `fact_sales` 522,566 ·
`fact_quarantine` 19,343 · `bridge_quarantine_rule` 30,739 · `dim_customer`
4,335 · `dim_product` 3,804 · `dim_date` 730 · `dim_country` 39 ·
`dim_quality_rule` 9 · `security_user_country` 10. A short table means a CSV
was truncated in transit — re-copy it rather than debug the model.

Two more, which are not numbers:

- Slice any visual to **2011 Q1** and confirm `Revenue YoY %` is **blank**, not
  a percentage. There is no Q1 2010 in the source; a number here means the
  measure is comparing against zero and calling it growth.
- Slice to any **Saturday** and confirm the page shows the `No Data Message`
  rather than an empty canvas.

## 12. If you ever do need to regenerate

Only when the source data or the pipeline changes. Verified against the repo as
it stands on GitHub today:

```powershell
cd D:\projects\retail-ai-pipeline
git pull
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt      # pandas, numpy, pyarrow, scikit-learn, PyYAML, pytest
python scripts\get_data.py           # ~45 MB into data\raw - not in git
python -m retail_pipeline.pipeline   # ~12 s; writes data\processed\*.parquet
                                     # and data\warehouse\retail.db
pytest -q                            # 13 tests
python -m bi.build_star_schema       # rewrites bi\model\*.csv
```

Airflow is **not** needed — `dags/` is only read by a scheduler, and
`requirements.txt` says so explicitly.

`build_star_schema.py` imports `CHECKS` from `retail_pipeline.pipeline` and reads
four parquet files — `fact_sales`, `dim_product`, `dim_customer`, `quarantine`.
All four are still produced by the current pipeline under the same names, which
is why the script still runs against the updated repo. This is also why `bi/`
has to sit **inside** the repo: `python -m bi.build_star_schema` resolves
`retail_pipeline` from the repo root.

The script validates before it writes — referential integrity on every foreign
key, uniqueness on every dimension key, contiguity on the calendar — and exits
non-zero rather than publishing a model with orphans in it. It prints the same
numbers listed in §11.

Then in Power BI Desktop: **Home → Refresh**. Because the queries read through
`ModelFolder`, nothing else changes.

## 13. Before you show it to anyone

- Save as `RetailSales.pbix` in `D:\projects\retail-ai-pipeline\bi\`.
- Screenshot all three pages plus the Model view diagram, into `bi\screenshots\`.
  **The model diagram is the screenshot that gets you the interview**; the
  dashboard is the one that gets skimmed.
- Add the screenshots to the repo README so the work is visible without anyone
  downloading a .pbix.
- Commit and push:

```powershell
cd D:\projects\retail-ai-pipeline
git add bi/
git add -f bi/screenshots/*.png
git status                # confirm model/ and *.pbix are NOT staged
git commit -m "Add Power BI semantic layer: model, measures, RLS, build guide"
git push
```

This is the **first time `bi/` appears on GitHub** — the folder has only ever
existed locally. Check `git status` before committing: `bi/.gitignore` should be
keeping `model/` (22 MB) and `RetailSales.pbix` out, and the screenshots need
`-f` because that same file ignores the folder's build output.

`model/` and `*.pbix` stay out of git on purpose (see `bi/.gitignore`) — 22 MB
of build output that changes wholesale on every run is the worst possible shape
for a git object, and §12 reproduces it in one command.

- If you publish to the Power BI service, publish to a workspace you can share
  a link to, and re-test the RLS roles there — Desktop's "View as" and the
  service do not always agree, and the service is the one that counts.
