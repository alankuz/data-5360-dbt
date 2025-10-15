{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
) }}


Select
    cu.cust_key
  ,d.date_key
  ,st.store_key
  ,pr.product_key
  ,em.employee_key
  ,ol.quantity
  ,(ol.quantity * ol.unit_price) As dollars_sold
  , ol.unit_price
From {{ source('oliver_landing', 'orders') }} o
Inner Join {{ source('oliver_landing', 'orderline') }} ol
  On ol.order_id = o.order_id
Inner Join {{ ref('oliver_dim_customer') }} cu
  On cu.customer_id = o.customer_id
Inner Join {{ ref('oliver_dim_product') }} pr
  On pr.product_id = ol.product_id
Inner Join {{ ref('oliver_dim_employee') }} em
  On em.employee_id = o.employee_id
Inner Join {{ ref('oliver_dim_store') }} st
  On st.store_id = o.store_id
Inner Join {{ ref('oliver_dim_date') }} d
  On d.date_id = o.order_date
