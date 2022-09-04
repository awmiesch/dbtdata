{{
    config(
        materialized = 'table',
        unique_key = ['cusip'],
    )
}}

with

stg_call as (

    select 
        load_timestamp,
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

)

select * from stg_call

{% if is_incremental() %}

    where load_timestamp > (select max(load_timestamp) from {{ this }})

{% endif %}

