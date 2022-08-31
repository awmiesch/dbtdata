select 
    effective_date,
    cusip,
    calltype as call_type,
    startdt as start_date,
    enddt as end_date,
    nextcall as next_call_date,
    date,
    f as call_freq,
    simpndx as simp_index,
    formula
from {{ source('fhlbof', 'call') }}