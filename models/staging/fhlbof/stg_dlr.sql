select 
    effective_date,
    cusip,
    dealer
from {{ source('fhlbof', 'dlr') }}