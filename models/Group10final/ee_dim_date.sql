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