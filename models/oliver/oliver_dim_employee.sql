{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}


select
EMPLOYEE_ID as employee_key,
EMPLOYEE_ID,
first_name as firstname,
last_name as lastname,
email,
phone_number as phonenumber,
hire_date,
position

FROM {{ source('oliver_landing', 'employee') }}