with jan_clients as (
    select
        *, --client_id,
        row_number() over (partition by client_id order by value desc) as rn
    from {{ source('de_test_sources', 'payments') }}
    where payment_date >= '2023-01-01'
    and payment_date < '2023-02-01'
)

select
    client_name,
    payment_date,
    value 
from jan_clients
where rn = 1