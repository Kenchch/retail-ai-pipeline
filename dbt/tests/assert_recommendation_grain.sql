select stock_code, rank from {{ ref('mart_recommendations') }}
group by stock_code, rank having count(*) <> 1
union all
select stock_code, rank from {{ ref('mart_recommendations') }}
where stock_code = recommended_stock_code or rank < 1
