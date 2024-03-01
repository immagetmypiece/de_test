with incomes as (
   select
      value,
      replaceRegexpOne(manager_email, '(.*)\.(.*)@(.*)', '\\1\\2@\\3') as manager_email
   from {{ source('de_test_sources', 'payments') }}
),

departments as (
   select
      replaceRegexpAll(department, '  ', ' ') as department,
      replaceRegexpOne(email, '(.*)\.(.*)@(.*)', '\\1\\2@\\3') as manager_email
   from {{ source('de_test_sources', 'manager_departments') }}
)

select
   coalesce(department, 'Отдел не определен'),
   sum(value)
from departments
full join incomes on incomes.manager_email = departments.manager_email
group by department