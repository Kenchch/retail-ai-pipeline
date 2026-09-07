select stock_code, recommended_stock_code, rank, method,
       support, confidence, lift
from {{ source('warehouse', 'recommendations') }}
