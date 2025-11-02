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