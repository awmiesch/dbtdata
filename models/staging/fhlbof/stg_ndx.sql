select 
    effective_date,
    cusip,
    seq as sequence,
    descrip as description,
    f as reset_freq,
    usage
from {{ source('fhlbof', 'ndx') }}