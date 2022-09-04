with

ext_bonds as (

    select * from {{ ref('ext_bonds') }}

),

ext_call as (

    select * from {{ ref('ext_call') }}

),

ext_cpn as (

    select * from {{ ref('ext_cpn') }}

),

int_bonds as (

    select
        ext_bonds.cusip,
        ext_bonds.series,
        ext_bonds.type,
        ext_bonds.pay_agent,
        ext_bonds.dealer,
        ext_bonds.trade_date,
        ext_bonds.issue_date,
        ext_bonds.maturity_date,
        ext_call.next_call_date,
        ext_call.call_type_desc,
        ext_call.call_freq_desc,
        ext_bonds.first_coupon_date,
        ext_cpn.coupon_freq_desc,
        ext_bonds.rate_type_desc,
        ext_bonds.rate_sub_type_desc,
        ext_bonds.day_count,
        ext_bonds.issue_price,
        ext_bonds.coupon,
        ext_cpn.margin,
        ext_bonds.original_par_value,
        ext_bonds.reopened_par_value,
        ext_bonds.outstanding_par_value
    from ext_bonds

    left join ext_call on ext_call.cusip = ext_bonds.cusip

    left join ext_cpn on ext_cpn.cusip = ext_bonds.cusip

)

select * from int_bonds

