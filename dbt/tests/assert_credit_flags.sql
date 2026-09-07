select invoice_no, stock_code
from {{ source('warehouse', 'fact_sales') }}
where reversed_by_credit <> (matched_credit_invoice is not null)
   or (reversed_by_credit and customer_id is null)
