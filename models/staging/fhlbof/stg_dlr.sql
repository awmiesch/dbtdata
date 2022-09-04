{{
    config(
        materialized = 'table',
        unique_key = ['cusip'],
    )
}}

with

stg_dlr as (

    select 
        load_timestamp,
        cusip,
        dealer
    from {{ source('fhlbof', 'dlr') }}

)

select * from stg_dlr

{% if is_incremental() %}

    where load_timestamp > (select max(load_timestamp) from {{ this }})

{% endif %}

