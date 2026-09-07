select *,
    case when reversed_by_credit then revenue else 0 end as matched_credit_revenue,
    case when reversed_by_credit then 0 else revenue end as net_revenue
from {{ ref('stg_sales') }}
