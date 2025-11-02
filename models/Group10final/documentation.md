Group 10 Final Project Part 2
<img width="951" height="713" alt="image" src="https://github.com/user-attachments/assets/451467d1-8404-434b-839b-d84f9464847b" />
Initial Setup instructions:

Go here to get the fivetran connection credentials. As they include keys and passwords that are secret, they will not be included here:

https://usu.instructure.com/courses/791624/pages/final-project-eco-essentials-marketing-data-mart

1.Create New Folder Called Group10final inside of your models folder. 

2.Create two following YML files. These will ensure that you are able to interact with the existing files within the snowflake database. 

_schema_group10project.yml

~~~
version: 2

models:
  - name: ee_dim_customer
    description: "customer Dimension"
  - name: ee_dim_campaign
    description: "Campaign Dimension"
  - name: ee_dim_date
    description: "Date Dimension"
  - name: ee_dim_email
    description: " sent email Dimension"
  - name: ee_dim_product
    description: "product dimension"
  - name: ee_fact_emailevent
    description: "email event fact table. Grain = a single email sent to a single person"
  - name: ee_fact_sales
    description: "sales fact table. grain = a single sale on a single date for a single customer"
~~~

_src_group10project.yml
~~~
version: 2

sources:
  - name: g10p_landing
    database: GROUP10PROJECT
    schema: DW_ECOESSENTIALS_SOURCE_TRANSACTIONAL_DB
    tables:
      - name: customer
      - name: email_events
      - name: orders
      - name: order_line
      - name: product
      - name: promotional_campaign
~~~

3. Create the following new .sql files in the Group10final folder. NOTE: click "run build" after saving each of the .sql files
additional note: YOU NEED TO MAKE SURE TO FOLLOW THIS ORDER TO AVOID CONFLICTS LATER DOWN THE LINE.

ee_dim_date.sql
~~~
{{ config(
    materialized = 'table',
    database = 'GROUP10PROJECT',
    schema = 'dw_ecoessentials'
    )
}}

with cte_date as (
{{ dbt_date.get_date_dimension("1990-01-01", "2050-12-31") }}
)

SELECT
date_day as date_key
,date_day As date
,Date_part('day', date_day) As day
,month_of_year As month
,quarter_of_year As quarter
,year_number As year
from cte_date
~~~
ee_dim_email.sql
~~~
{{ config(
    materialized = 'table',
    database = 'GROUP10PROJECT',
    schema = 'dw_ecoessentials'
    )
}}

select
emailid as email_key,
emailid as email_id,
emailname



FROM {{ source('g10p_landing', 'email_events') }}
~~~
ee_dim_product.sql
~~~
{{ config(
    materialized = 'table',
    database = 'GROUP10PROJECT',
    schema = 'dw_ecoessentials'
    )
}}

select
product_id as product_key,
product_id,
product_name,
product_type,
price


FROM {{ source('g10p_landing', 'product') }}
~~~
ee_dim_campaign.sql
~~~
{{ config(
    materialized = 'table',
    database = 'GROUP10PROJECT',
    schema = 'dw_ecoessentials'
    )
}}

select
campaign_id as campaign_key,
campaign_id,
campaign_name,
campaign_discount,



FROM {{ source('g10p_landing', 'promotional_campaign') }}
~~~
ee_dim_customer.sql
~~~
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
~~~
After creating your dimensions, you may then create your fact tables using the following code. Make sure to run each of these after saving the files one at a time to avoid errors

ee_fact_emailevent.sql
~~~

{{ config(
    materialized = 'table',
    database = 'GROUP10PROJECT',
    schema = 'dw_ecoessentials'
    )
}}
select
    cu.customer_key as customer_key,
    ca.campaign_key as campaign_key,
    em.email_key as email_key,
    cast(e.sendtimestamp as date) as send_date_key,
    cast(e.eventtimestamp as date) as event_date_key,
    e.eventtype as event_type
from {{ source('g10p_landing','email_events') }} e
left join {{ ref('ee_dim_customer') }} cu
  on e.subscriberemail = cu.email
left join {{ ref('ee_dim_campaign') }} ca
  on ca.Campaign_ID = e.campaignid
left join {{ ref('ee_dim_email') }} em
  on em.email_ID = e.emailid
left join {{ ref('ee_dim_date') }} d_send
  on d_send.date_key = cast(e.sendtimestamp as date)
left join {{ ref('ee_dim_date') }} d_event
  on d_event.date_key = cast(e.eventtimestamp as date)
~~~
ee_fact_sales.sql
~~~
{{ config(
    materialized = 'table',
    database = 'GROUP10PROJECT',
    schema = 'dw_ecoessentials'
    )
}}

select
    cu.customer_key as customer_key,
    pr.product_key as product_key,
    ca.campaign_key as campaign_key,
    cast(o.order_timestamp as date) as Order_date_key,
    ol.quantity as quantity,
    pr.price as unit_price,
    ol.discount as discount,
    cast(round(ol.quantity * (pr.price - (pr.price * ol.discount)), 2) as number(18,2)) as dollars_sold
from {{ source('g10p_landing','orders') }} o
inner join {{ source('g10p_landing','order_line') }} ol
on ol.order_id=o.order_id
inner join {{ ref('ee_dim_customer') }} cu
on cu.CustomerID=o.customer_id
inner join {{ ref('ee_dim_product') }} pr
on pr.Product_ID=ol.product_id
left join {{ ref('ee_dim_campaign') }} ca
on ca.Campaign_ID=ol.campaign_id
inner join {{ ref('ee_dim_date') }} d
on d.date_key=cast(o.order_timestamp as date)
~~~


