select cast('{{ env_var("RETAIL_PUBLISHED_RUN_ID_SQL") }}' as varchar) as run_id,
    cast(stock_code as varchar) as stock_code,
    cast(count(distinct invoice_no) as bigint) as orders,
    cast(sum(quantity) as bigint) as units,
    cast(sum(revenue) as decimal(18, 4)) as revenue,
    cast(sum(matched_credit_revenue) as decimal(18, 4)) as matched_credit_revenue,
    cast(sum(net_revenue) as decimal(18, 4)) as net_revenue
from {{ ref('int_sales_measures') }}
group by stock_code
