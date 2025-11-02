
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