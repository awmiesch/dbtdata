{{
    config(
        materialized = 'table',
        unique_key = ['cusip'],
    )
}}

with

stg_cpn as (

  select 
      load_timestamp,
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

)

select * from stg_cpn

{% if is_incremental() %}

    where load_timestamp > (select max(load_timestamp) from {{ this }})

{% endif %}

