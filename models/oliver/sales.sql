{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
) }}

Select
d.date_key As order_date_key
,c.firstname As customer_first_name
,c.lastname As customer_last_name
,em.firstname As employee_first_name
,em.lastname As employee_last_name
,pr.product_name
,pr.description
,st.store_name
,f.quantity
,f.unit_price
,f.dollars_sold
From {{ ref('fact_sales') }} f
Left Join {{ ref('oliver_dim_customer') }} c
  On f.cust_key = c.cust_key
Left Join {{ ref('oliver_dim_product') }} pr
  On f.product_key = pr.product_key
Left Join {{ ref('oliver_dim_employee') }} em
  On f.employee_key = em.employee_key
Left Join {{ ref('oliver_dim_store') }} st
  On f.store_key = st.store_key
Left Join {{ ref('oliver_dim_date') }} d
  On f.date_key = d.date_key