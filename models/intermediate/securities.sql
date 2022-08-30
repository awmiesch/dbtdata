with 

base_table as (
    select 
        cert as cert,
        fed_rssd as fed_rssd,
        name as name,
        report_date as report_date,
        bkclass_code as bkclass_code,
        us_government_securities as us_government_securities,
        us_treasury_securities as us_treasury_securities,
        us_government_agency_obligations as us_government_agency_obligations,
        securities_issued_by_states_political_subdivisions as securities_issued_by_states_political_subdivisions,
        other_domestic_debt_securities as other_domestic_debt_securities
    from {{ ref('stg_fdic_securities') }}
), 

unpivoted_table as (
    select *
    from base_table
    unpivot(
        amount for category in (
            us_government_securities, 
            us_treasury_securities, 
            us_government_agency_obligations, 
            securities_issued_by_states_political_subdivisions,
            other_domestic_debt_securities
        )
    )
), 

result_table as (
    select 
        report_date,
        fed_rssd,
        name,
        bkclass_code,
        category,
        amount
    from unpivoted_table
)

select * from result_table

