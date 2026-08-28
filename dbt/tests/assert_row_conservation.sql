with actual as (
    select
        (select count(*) from {{ source('warehouse', 'fact_sales') }}) as loaded,
        (select count(*) from {{ source('warehouse', 'quarantine') }}) as rejected
),

expected as (
    select
        rows_loaded::bigint as reported_loaded,
        rows_quarantined::bigint as reported_rejected,
        rows_source::bigint as reported_source,
        inputs['{{ env_var("RETAIL_RAW_INPUT_NAME_SQL") }}'].rows::bigint as input_rows
    from {{ source('reports', 'run_metrics') }}
)

select actual.*, expected.*
from actual
cross join expected
where actual.loaded <> expected.reported_loaded
   or actual.rejected <> expected.reported_rejected
   or actual.loaded + actual.rejected <> expected.reported_source
   or expected.reported_source <> expected.input_rows
