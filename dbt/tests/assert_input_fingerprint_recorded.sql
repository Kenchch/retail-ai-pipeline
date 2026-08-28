select run_id
from {{ source('reports', 'run_metrics') }}
where inputs['{{ env_var("RETAIL_RAW_INPUT_NAME_SQL") }}'].sha256 is null
   or not regexp_full_match(
       inputs['{{ env_var("RETAIL_RAW_INPUT_NAME_SQL") }}'].sha256,
       '^[0-9a-f]{16}$'
   )
   or inputs['{{ env_var("RETAIL_RAW_INPUT_NAME_SQL") }}'].rows is null
