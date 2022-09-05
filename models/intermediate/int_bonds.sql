with

base_bonds as (

    select * from {{ ref('base_bonds') }}

),

base_call as (

    select * from {{ ref('base_call') }}

),

base_cpn as (

    select * from {{ ref('base_cpn') }}

),

merged_bonds as (

    select
        base_bonds.cusip,
        base_bonds.series,
        base_bonds.type,
        base_bonds.pay_agent,
        base_bonds.dealer,
        base_bonds.trade_date,
        base_bonds.issue_date,
        base_bonds.maturity_date,
        base_call.next_call_date,
        base_call.call_type_desc,
        base_call.call_freq_desc,
        base_bonds.first_coupon_date,
        base_cpn.coupon_freq_desc,
        base_bonds.rate_type_desc,
        base_bonds.rate_sub_type_desc,
        base_bonds.day_count,
        base_bonds.issue_price,
        base_bonds.coupon,
        base_cpn.margin,
        base_bonds.original_par_value,
        base_bonds.reopened_par_value,
        base_bonds.outstanding_par_value
    from base_bonds

    left join base_call on base_call.cusip = base_bonds.cusip

    left join base_cpn on base_cpn.cusip = base_bonds.cusip

),

int_bonds as (

    select
        cusip,
        series,
        type,
        pay_agent,
        dealer,
        trade_date,
        issue_date,
        maturity_date,
        date_diff(maturity_date, issue_date, year) as years_to_maturity,
        next_call_date,
        date_diff(coalesce(next_call_date, maturity_date), issue_date, year) as years_to_maturity_or_next_call,
        call_type_desc,
        call_freq_desc,
        first_coupon_date,
        coupon_freq_desc,
        rate_type_desc,
        rate_sub_type_desc,
        day_count,
        issue_price,
        coupon,
        margin,
        original_par_value,
        reopened_par_value,
        outstanding_par_value
    from merged_bonds

)

select * from int_bonds

