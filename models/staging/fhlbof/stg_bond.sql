{{
    config(
        materialized = 'table',
        unique_key = ['cusip'],
    )
}}

with

stg_bond as (

    select 
        load_timestamp,
        cusip,
        series,
        traded as trade_date,
        issued as issue_date,
        maturity as maturity_date,
        type,
        firstcpn as first_coupon_date,
        f as coupon_freq,
        pcr as principal_currency,
        ccr as coupon_currency,
        r as rate_type,
        s as rate_sub_type,
        call as call_type,
        original as original_par_value,
        reopened as reopened_par_value,
        outstanding as outstanding_par_value,
        price as issue_price,
        coupon,
        mindenom as min_denomination,
        multiples,
        daycnt as day_count,
        pagent as pay_agent,
        v as variable_principal_ind,
        formula
    from {{ source('fhlbof', 'bond') }}

)

select * from stg_bond

{% if is_incremental() %}

    where load_timestamp > (select max(load_timestamp) from {{ this }})

{% endif %}



