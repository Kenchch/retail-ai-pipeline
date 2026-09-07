select cast('{{ env_var("RETAIL_PUBLISHED_RUN_ID_SQL") }}' as varchar) as run_id,
    cast(stock_code as varchar) as stock_code,
    cast(recommended_stock_code as varchar) as recommended_stock_code,
    cast(rank as integer) as rank, cast(method as varchar) as method,
    cast(support as double) as support,
    cast(confidence as double) as confidence, cast(lift as double) as lift
from {{ ref('stg_recommendations') }}
