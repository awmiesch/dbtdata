select 
    effective_date,
    cusip,
    seq as sequence,
    start as start_date,
    coupon,
    margin,
    simpndx as simp_index,
    case 
      when firstrst != 'N/A' 
      then date(
        cast(substr(firstrst, 1, 4) as int), 
        cast(substr(firstrst, 5, 2) as int), 
        cast(substr(firstrst, 7, 2) as int)
      ) 
      else null 
    end as first_reset_date,
    cpnfrq as coupon_freq,
    formula
from {{ source('fhlbof', 'cpn') }}