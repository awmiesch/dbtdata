with

stg_call as (

    select * from {{ ref('stg_call') }}

),

ref_call_type as (

    select * from {{ ref('ref_call_type') }}

),

ref_call_freq as (

    select * from {{ ref('ref_call_freq') }}

),

ext_call as (

    select
        stg_call.cusip,
        stg_call.next_call_date,
        ref_call_type.call_type_desc,
        ref_call_freq.call_freq_desc
    from stg_call

    left join ref_call_type on ref_call_type.call_type_code = stg_call.call_type

    left join ref_call_freq on ref_call_freq.call_freq_code = stg_call.call_freq

)

select * from ext_call

