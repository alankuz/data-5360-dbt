{{ config(
materialized = 'table',
schema = 'dw_oliver'
) }}

with employee_certifications as (
select
employee_id
,first_name
,last_name
,email
,certification_completion_id
,parse_json(certification_json):certification_name::varchar as certification_name
,parse_json(certification_json):certification_cost::number(12,2) as certification_cost
,to_date(parse_json(certification_json):certification_awarded_date::varchar) as certification_awarded_date
,to_number(to_char(to_date(parse_json(certification_json):certification_awarded_date::varchar),'YYYYMMDD')) as date_key
from {{ source('oliver_landing','employee_certifications') }}
)

select
employee_id
,first_name
,last_name
,email
,certification_completion_id
,certification_name
,certification_cost
,certification_awarded_date
,date_key
from employee_certifications
qualify row_number() over (partition by employee_id,certification_name,certification_awarded_date order by certification_awarded_date desc)=1
