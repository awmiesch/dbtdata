{{
    config(
        materialized='incremental',
        unique_key=['cusip', 'effective_date'],
    )
}}

with 

bonds as (

    select
        effective_date,
        cusip,
        series,
        type,
        pay_agent,
        dealer,
        trade_date,
        issue_date,
        maturity_date,
        next_call_date,
        first_coupon_date,
        coupon_freq_desc,
        rate_type_desc,
        rate_sub_type_desc,
        call_type_desc,
        call_freq_desc,
        issue_price,
        coupon,
        day_count,
        margin,
        original_par_value,
        reopened_par_value,
        outstanding_par_value
    from {{ ref('int_bonds_ext') }}

)