with

source as (

    select * from {{ source('jaffle_shop',  'customers') }}

),

staged as (

    select
        id as customer_id,
        first_name,
        last_name,

        upper(left(first_name, 1)) 
            || lower(right(first_name, len(first_name) -1)) 
            || ' '
            || upper(left(last_name, 1)) 
            || lower(right(last_name, len(last_name) -1)) 
        as full_name

    from source

)

select * from staged