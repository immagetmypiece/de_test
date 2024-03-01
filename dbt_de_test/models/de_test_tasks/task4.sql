with month_total as (
   select
      toStartOfMonth(payment_date) as year_month,
       sum(value) as revenue_by_month
   from {{ source('de_test_sources', 'payments') }}
   group by 1
)
select
   concat(
       datename('month', year_month), ' ',
       datename('year', year_month)
    ) as period,
   revenue_by_month,
   sum(revenue_by_month) over (order by year_month) as revenue_cumulative
from month_total