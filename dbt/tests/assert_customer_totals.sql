with expected as (
    select cast(customer_id as bigint) as customer_id,
           sum(cast(revenue as decimal(18,4))) as revenue,
           sum(case when reversed_by_credit then 0 else cast(revenue as decimal(18,4)) end) as net_revenue
    from {{ source('warehouse', 'fact_sales') }} where customer_id is not null
    group by customer_id
)
select m.customer_id from {{ ref('mart_customer_sales') }} m
full outer join expected e using (customer_id)
where m.customer_id is null or e.customer_id is null
   or m.revenue is distinct from e.revenue or m.net_revenue is distinct from e.net_revenue
