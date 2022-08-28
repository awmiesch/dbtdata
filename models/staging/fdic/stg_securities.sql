select *
from {{ source('fdic', 'securities') }}