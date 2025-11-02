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