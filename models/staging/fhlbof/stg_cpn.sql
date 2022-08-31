select 
    effective_date,
    cusip,
    seq as sequence,
    start as start_date,
    coupon,
    margin,
    simpndx as simp_index,
    firstrst as first_reset_date,
    cpnfrq as coupon_freq,
    formula
from {{ source('fhlbof', 'cpn') }}