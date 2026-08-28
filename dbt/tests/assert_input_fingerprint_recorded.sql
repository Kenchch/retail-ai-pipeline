select run_id
from {{ source('reports', 'run_metrics') }}
where inputs['online_retail.csv'].sha256 is null
   or not regexp_full_match(
       inputs['online_retail.csv'].sha256,
       '^[0-9a-f]{16}$'
   )
   or inputs['online_retail.csv'].rows is null
