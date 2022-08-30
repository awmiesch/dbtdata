select *
from {{ source('fhlbof', 'allcpn') }}