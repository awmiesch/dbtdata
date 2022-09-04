with 

int_bonds as (

    select * from {{ ref('int_bonds') }}

),

bonds_by_rate_type as (

    select
        rate_type_desc,
        rate_sub_type_desc,
        sum(outstanding_par_value) as outstanding_par_value
    from int_bonds
    group by rate_type_desc, rate_sub_type_desc

)

select * from bonds_by_rate_type
