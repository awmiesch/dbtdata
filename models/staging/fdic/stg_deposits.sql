select *
from {{ source('fdic', 'deposits') }}