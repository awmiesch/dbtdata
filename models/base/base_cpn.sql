with

stg_cpn as (

    select * from {{ ref('stg_cpn') }}

),

ref_coupon_freq as (

    select * from {{ ref('ref_coupon_freq') }}

),

base_cpn as (

    select
        stg_cpn.cusip,
        ref_coupon_freq.coupon_freq_desc,
        stg_cpn.margin
    from stg_cpn

    left join ref_coupon_freq on ref_coupon_freq.coupon_freq_code = stg_cpn.coupon_freq

)

select * from base_cpn

