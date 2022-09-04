with

stg_bond as (

    select * from {{ ref('stg_bond') }}

),

stg_ndx as (

    select * from {{ ref('stg_ndx') }}

),

stg_dlr as (

    select * from {{ ref('stg_dlr') }}

),

ref_rate_type as (

    select * from {{ ref('ref_rate_type') }}

),

ref_rate_sub_type as (

    select * from {{ ref('ref_rate_sub_type') }}

),

ext_bonds as (

    select
        stg_bond.cusip,
        stg_bond.series,
        stg_bond.type,
        stg_bond.pay_agent,
        stg_dlr.dealer,
        stg_bond.trade_date,
        stg_bond.issue_date,
        stg_bond.maturity_date,
        stg_bond.first_coupon_date,
        ref_rate_type.rate_type_desc,
        ref_rate_sub_type.rate_sub_type_desc,
        stg_bond.issue_price,
        stg_bond.coupon,
        stg_bond.day_count,
        stg_bond.original_par_value,
        stg_bond.reopened_par_value,
        stg_bond.outstanding_par_value
    from stg_bond

    left join stg_dlr on stg_dlr.cusip = stg_bond.cusip

    left join ref_rate_type on ref_rate_type.rate_type_code = stg_bond.rate_type

    left join ref_rate_sub_type on ref_rate_sub_type.rate_sub_type_code = stg_bond.rate_sub_type

)

select * from ext_bonds

