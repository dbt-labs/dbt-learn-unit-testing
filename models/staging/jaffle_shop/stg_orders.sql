with

source as (

    select * from {{ source('jaffle_shop',  'orders') }}

),

staged as (

    select 
        id as order_id,
        user_id as customer_id,
        order_date,
        status like '%pending%' as is_status_pending,
        case 
            when status like '%return%' then 'returned'
            when status like '%pending%' then 'placed'
            else status
        end as status
    from source

)

select * from staged