{{ config(
materialized='table',
schema='dw_oliver'
) }}

with s as (
select
employee_id
,certification_name
,certification_cost
,certification_awarded_date
from {{ ref('stg_employee_certifications') }}
)

select
d.date_key
,e.employee_key
,s.certification_name
,s.certification_cost
from s
join {{ ref('oliver_dim_employee') }} e on e.employee_id=s.employee_id
join {{ ref('oliver_dim_date') }} d on d.date_id=s.certification_awarded_date
