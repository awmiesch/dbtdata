select *
from {{ source('fhlbof', 'ndx') }}