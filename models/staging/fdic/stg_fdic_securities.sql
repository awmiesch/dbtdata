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
    sc as total_securities,
    scus as us_government_securities,
    scust as us_treasury_securities,
    scage as us_government_agency_obligations,
    scmuni as securities_issued_by_states_political_subdivisions,
    SCDOMO as other_domestic_debt_securities,
    idscod as privately_issued_residential_mortgage_backed_securities,
    idsccmmb as idsccmmb,
    scabs as asset_backed_securities,
    scsfp as structured_financial_products_total,
    scodot as other_domestic_debt_securities_all_other,
    scford as foreign_debt_securities,
    IDSCEQ as equity_securities_not_held_for_trading,
    sceq as equity_securities_available_for_sale,
    SCEQFV as SCEQFV,
    schtmres as credit_loss_allowance_for_held_to_maturity_debt_securities,
    idahta as assets_held_in_trading_accounts_for_tfr_reporters,
    scres as general_valuation_allowances_for_securities_for_tfr_reporters,
    scpledge as pledged_securities,
    scmtgbk as mortgage_backed_securities,
    idscgtpc as certificates_of_participation_in_pools_of_residential_mortgages,
    scgty as mortgage_backed_pass_through_securities_issued_or_guaranteed_by_us,
    scodpc as mortgage_backed_pass_through_securities_privately_issued,
    idsccmo as collaterized_mortgage_obligations,
    sccol as cmos_issued_by_government_agencies_or_sponsored_agencies,
    scodpi as mortgage_backed_pass_through_securities_privately_issued_collateralized_by_mbss,
    idsccmt as commercial_mortgage_backed_securities_us_government_sponsored,
    sccmpt as commercial_mortgage_pass_through_securities,
    sccmot as other_commercial_mortgage_backed_securities,
    scha as held_to_maturity_securities_book_value,
    scaf as available_for_sale_securities_fair_market_value,
    scrdebt as total_debt_securities,
    scsnhaa as amortized_cost_of_structured_notes,
    scsnhaf as fair_value_of_structured_notes,
    trade as trading_account_assets,
    idtrrval as revaluation_gains_on_off_balance_sheet_contracts,
    trlreval as revaluation_losses_on_off_balance_sheet_contracts
from {{ source('fdic', 'securities') }}