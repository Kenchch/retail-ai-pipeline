with expected as (
    select stock_code, sum(cast(revenue as decimal(18,4))) as revenue,
           sum(case when reversed_by_credit then 0 else cast(revenue as decimal(18,4)) end) as net_revenue
    from {{ source('warehouse', 'fact_sales') }} group by stock_code
)
select m.stock_code from {{ ref('mart_product_sales') }} m
full outer join expected e using (stock_code)
where m.stock_code is null or e.stock_code is null
   or m.revenue is distinct from e.revenue or m.net_revenue is distinct from e.net_revenue
