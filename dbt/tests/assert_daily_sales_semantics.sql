with expected as (
    select
        d.date_key,
        d.year,
        d.month,
        d.day_of_week,
        d.is_weekend,
        count(f.invoice_no) > 0 as has_sales,
        coalesce(sum(f.revenue), 0) as revenue,
        count(distinct f.invoice_no) as orders,
        coalesce(
            sum(case when f.customer_id is null then f.revenue else 0 end)
                / nullif(sum(f.revenue), 0),
            0
        ) as guest_revenue_share
    from {{ source('warehouse', 'dim_date') }} as d
    left join {{ source('warehouse', 'fact_sales') }} as f using (date_key)
    group by d.date_key, d.year, d.month, d.day_of_week, d.is_weekend
)

select m.*, e.date_key as expected_date_key
from {{ ref('mart_daily_sales') }} as m
full outer join expected as e on m.date_key = cast(e.date_key as date)
where m.date_key is null
   or e.date_key is null
   or m.run_id <> '{{ env_var("RETAIL_PUBLISHED_RUN_ID_SQL") }}'
   or m.year <> e.year
   or m.month <> e.month
   or m.day_of_week <> e.day_of_week
   or m.is_weekend <> e.is_weekend
   or m.has_sales <> e.has_sales
   or m.revenue <> cast(e.revenue as decimal(18, 4))
   or m.orders <> e.orders
   or abs(m.guest_revenue_share - e.guest_revenue_share) > 0.000000001
   or m.guest_revenue_share not between 0 and 1
   or (
       not m.has_sales
       and (m.revenue <> 0 or m.orders <> 0 or m.guest_revenue_share <> 0)
   )
