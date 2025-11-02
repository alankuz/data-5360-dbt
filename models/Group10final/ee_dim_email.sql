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