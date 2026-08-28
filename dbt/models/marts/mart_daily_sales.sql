with daily as (
    select
        date_key,
        sum(revenue) as revenue,
        count(distinct invoice_no) as orders,
        sum(case when customer_id is null then revenue else 0 end)
            / nullif(sum(revenue), 0) as guest_revenue_share
    from {{ source('warehouse', 'fact_sales') }}
    group by date_key
)

select
    cast('{{ env_var("RETAIL_PUBLISHED_RUN_ID_SQL") }}' as varchar) as run_id,
    cast(d.date_key as date) as date_key,
    cast(d.year as integer) as year,
    cast(d.month as integer) as month,
    cast(d.day_of_week as varchar) as day_of_week,
    cast(d.is_weekend as boolean) as is_weekend,
    cast(d.has_sales as boolean) as has_sales,
    cast(coalesce(s.revenue, 0) as decimal(18, 4)) as revenue,
    cast(coalesce(s.orders, 0) as bigint) as orders,
    cast(coalesce(s.guest_revenue_share, 0) as double) as guest_revenue_share
from {{ source('warehouse', 'dim_date') }} as d
left join daily as s using (date_key)
order by d.date_key
