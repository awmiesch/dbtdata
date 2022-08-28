select *
from {{ source('fdic', 'debt_securities') }}