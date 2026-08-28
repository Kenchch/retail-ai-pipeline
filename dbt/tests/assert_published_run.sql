with identity as (
    select
        count(*) as metric_rows,
        min(run_id) as reported_run_id
    from {{ source('reports', 'run_metrics') }}
)

select *
from identity
where metric_rows <> 1
   or reported_run_id <> '{{ env_var("RETAIL_PUBLISHED_RUN_ID_SQL") }}'
