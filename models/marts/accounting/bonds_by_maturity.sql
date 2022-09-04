with 

int_bonds as (

    select * from {{ ref('int_bonds') }}

),

ref_maturity_categories as (

    select * from {{ ref('ref_maturity_categories') }}

),

bonds_by_maturity as (

    select  
        int_bonds.years_to_maturity,
        ref_maturity_categories.maturity_category_desc as maturity_category,
        sum(int_bonds.outstanding_par_value) as outstanding_par_value
    from int_bonds
    left join ref_maturity_categories on ref_maturity_categories.maturity_category_code = int_bonds.years_to_maturity
    group by int_bonds.years_to_maturity, ref_maturity_categories.maturity_category_desc
    order by 1

)

select * from bonds_by_maturity
