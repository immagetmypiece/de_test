with payments_rn as (
    select
        *,
        row_number() over (partition by client_id order by payment_date) as rn
    from {{ source('de_test_sources', 'payments') }}
)
select
    id,
    value,
    client_id,
    client_name,
    payment_date,
    manager_name,
    manager_email,
    case when rn = 1 then 'Новый' else 'Действующий' end as client_state
from payments_rn