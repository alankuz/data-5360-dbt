{{ config(
    materialized = 'table',
    database = 'GROUP10PROJECT',
    schema = 'dw_ecoessentials'
    )
}}

with cust as (
  select
    to_varchar(customer_id) as customerid,
    customer_first_name as firstname,
    customer_last_name as lastname,
    customer_phone as phone,
    customer_address as street,
    customer_city as city,
    customer_state as state,
    customer_zip as zip,
    customer_country as country
  from {{ source('g10p_landing','customer') }}
),

ee as (
  select
    customerid,
    lower(trim(max(subscriberemail))) as email_norm
  from {{ source('g10p_landing','email_events') }}
  group by customerid
)

select
  c.customerid as customer_key,
  c.customerid as CustomerID,
  e.email_norm as email,
  c.firstname as FirstName,
  c.lastname as LastName,
  c.phone as phone,
  c.street as Street,
  c.city as City,
  c.state as State,
  c.zip as ZIP,
  c.country as Country
from cust c
left join ee e
on c.customerid=e.customerid
where e.email_norm is not null