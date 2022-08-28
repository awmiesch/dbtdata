select *
from {{ source('fdic', 'derivatives') }}