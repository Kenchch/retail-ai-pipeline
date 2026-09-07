select cast('{{ env_var("RETAIL_PUBLISHED_RUN_ID_SQL") }}' as varchar) as run_id,
    cast(customer_id as bigint) as customer_id,
    cast(count(distinct invoice_no) as bigint) as orders,
    cast(sum(revenue) as decimal(18, 4)) as revenue,
    cast(sum(matched_credit_revenue) as decimal(18, 4)) as matched_credit_revenue,
    cast(sum(net_revenue) as decimal(18, 4)) as net_revenue,
    min(date_key) as first_sale_date, max(date_key) as last_sale_date
from {{ ref('int_sales_measures') }}
where customer_id is not null
 group by customer_id
