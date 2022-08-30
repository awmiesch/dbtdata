select 
    cert as cert,
    docket as docket,
    fed_rssd as fed_rssd,
    rssdhcr as holding_company_rssd,
    name as name,
    city as city,
    stalp as state,
    zip as zip,
    repdte as report_date,
    rundate as run_date,
    bkclass as bkclass_code,
    address as address,
    namehcr as holding_company,
    offdom as offdom,
    offfor as offfor,
    stmult as stmult,
    specgrp as specgrp,
    subchaps as subchaps,
    county as county,
    cbsa_metro as cbsa_metro,
    cbsa_metro_name as cbsa_metro_name,
    estymd as estymd,
    insdate as insdate,
    effdate as effdate,
    mutual as mutual,
    parcert as parcert,
    trust as trust,
    regagnt as regagnt,
    insagnt1 as insagnt1,
    fdicdbs as fdicdbs,
    fdicsupv as fdicsupv,
    fldoff as fldoff,
    fed as fed,
    occdist as occdist,
    otsregnm as otsregnm,
    offoa as offoa,
    cb as cb,
    inst_webaddr as webaddr,
    scrdebt as total_debt_securities,
    scpt3les as res_1_4_fam_mbss_expected_average_life_three_months_or_less,
    scpt3t12 as res_1_4_fam_mbss_expected_average_life_over_three_months_through_twelve_months,
    scpt1t3 as res_1_4_fam_mbss_expected_average_life_over_three_yearsover_one_year_through_thr,
    scpt3t5 as res_1_4_fam_mbss_expected_average_life_over_three_years_through_five_years,
    scpt5t15 as res_1_4_fam_mbss_expected_average_life_over_five_years_through_fifteen_years,
    scptov15 as res_1_4_fam_mbss_expected_average_life_over_fifteen_years,
    sco3yles as other_mortgage_backed_securities_expected_average_life_three_years_or_less,
    scoov3y as other_mortgage_backed_securities_expected_average_life_over_three_years,
    scnm3les as debt_securities_maturity_or_repricing_three_months_or_less,
    scnm3t12 as debt_securities_maturity_or_repricing_over_three_months_through_twelve_months,
    scnm1t3 as debt_securities_maturity_or_repricing_over_one_year_through_three_years,
    scnm3t5 as debt_securities_maturity_or_repricing_over_three_years_through_five_years,
    scnm5t15 as debt_securities_maturity_or_repricing_over_five_years_through_fifteen_years,
    scnmov15 as debt_securities_maturity_or_repricing_over_fifteen_years,
    sc1les as with_remaining_maturity_of_one_year_or_less
from {{ source('fdic', 'debt_securities') }}