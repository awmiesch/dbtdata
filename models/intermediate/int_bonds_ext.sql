with

stg_bond as (

    select * from {{ ref('stg_bond') }}

),

stg_cpn as (

    select * from {{ ref('stg_cpn') }}

),

stg_call as (

    select * from {{ ref('stg_call') }}

),

stg_ndx as (

    select * from {{ ref('stg_ndx') }}

),

stg_dlr as (

    select * from {{ ref('stg_dlr') }}

),

ref_call_type as (

    select * from {{ ref('ref_call_type') }}

),

ref_call_freq as (

    select * from {{ ref('ref_call_freq') }}

),

ref_rate_type as (

    select * from {{ ref('ref_rate_type') }}

),

ref_rate_sub_type as (

    select * from {{ ref('ref_rate_sub_type') }}

),

ref_coupon_freq as (

    select * from {{ ref('ref_coupon_freq') }}

),

int_bonds_ext as (

    select
        stg_bond.effective_date,
        stg_bond.cusip,
        stg_bond.series,
        stg_bond.type,
        stg_bond.pay_agent,
        stg_dlr.dealer,
        stg_bond.trade_date,
        stg_bond.issue_date,
        stg_bond.maturity_date,
        stg_call.next_call_date,
        stg_bond.first_coupon_date,
        ref_coupon_freq.coupon_freq_desc,
        ref_rate_type.rate_type_desc,
        ref_rate_sub_type.rate_sub_type_desc,
        ref_call_type.call_type_desc,
        ref_call_freq.call_freq_desc,
        stg_bond.issue_price,
        stg_bond.coupon,
        stg_bond.day_count,
        stg_cpn.margin,
        stg_bond.original_par_value,
        stg_bond.reopened_par_value,
        stg_bond.outstanding_par_value
    from stg_bond

    left join stg_cpn on stg_cpn.cusip = stg_bond.cusip and stg_cpn.effective_date = stg_bond.effective_date

    left join stg_call on stg_call.cusip = stg_bond.cusip and stg_call.effective_date = stg_bond.effective_date

    left join stg_ndx on stg_ndx.cusip = stg_bond.cusip and stg_ndx.effective_date = stg_bond.effective_date

    left join stg_dlr on stg_dlr.cusip = stg_bond.cusip and stg_dlr.effective_date = stg_bond.effective_date

    left join ref_call_type on ref_call_type.call_type_code = stg_bond.call_type

    left join ref_call_freq on ref_call_freq.call_freq_code = stg_call.call_freq

    left join ref_coupon_freq on ref_coupon_freq.coupon_freq_code = stg_cpn.coupon_freq

    left join ref_rate_type on ref_rate_type.rate_type_code = stg_bond.rate_type

    left join ref_rate_sub_type on ref_rate_sub_type.rate_sub_type_code = stg_bond.rate_sub_type

)

select * from int_bonds_ext

