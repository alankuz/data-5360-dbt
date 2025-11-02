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