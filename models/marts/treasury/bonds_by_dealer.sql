with 

int_bonds as (

    select * from {{ ref('int_bonds') }}

),

bonds_by_dealer as (

    select
        dealer,
        sum(outstanding_par_value) as outstanding_par_value
    from int_bonds
    group by dealer

)

select * from bonds_by_dealer
