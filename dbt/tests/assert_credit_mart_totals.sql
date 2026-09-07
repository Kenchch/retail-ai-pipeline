with expected as (
    select sum(cast(revenue as decimal(18,4))) as gross,
           sum(case when reversed_by_credit then cast(revenue as decimal(18,4)) else 0 end) as credits
    from {{ source('warehouse', 'fact_sales') }}
), actual as (
    select 'daily' as mart, sum(revenue) as gross, sum(matched_credit_revenue) as credits,
           sum(net_revenue) as net from {{ ref('mart_daily_sales') }}
    union all
    select 'product', sum(revenue), sum(matched_credit_revenue), sum(net_revenue)
    from {{ ref('mart_product_sales') }}
)
select actual.* from actual cross join expected
where actual.gross is distinct from expected.gross
   or actual.credits is distinct from expected.credits
   or actual.net is distinct from expected.gross - expected.credits
