select *
from {{ source('fhlbof', 'cpn') }}