{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}


select
customer_id as cust_key,
customer_id,
first_name as firstname,
last_name as lastname,
email,
phone_number as phonenumber,
state

FROM {{ source('oliver_landing', 'customer') }}