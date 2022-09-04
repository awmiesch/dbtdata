{{
    config(
        materialized = 'table',
        unique_key = ['cusip'],
    )
}}

with

stg_ndx as (

    select 
        load_timestamp,
        cusip,
        seq as sequence,
        descrip as description,
        f as reset_freq,
        usage
    from {{ source('fhlbof', 'ndx') }}

)

select * from stg_ndx

{% if is_incremental() %}

    where load_timestamp > (select max(load_timestamp) from {{ this }})

{% endif %}

