select
    invoice_no, stock_code, customer_id, cast(date_key as date) as date_key,
    invoice_ts, quantity, cast(unit_price as decimal(18, 4)) as unit_price,
    cast(revenue as decimal(18, 4)) as revenue, country,
    cast(reversed_by_credit as boolean) as reversed_by_credit,
    matched_credit_invoice
from {{ source('warehouse', 'fact_sales') }}
