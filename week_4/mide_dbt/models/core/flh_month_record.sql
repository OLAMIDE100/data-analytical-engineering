{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_fhl_trip') }}
)
    select 
    --date_trunc('month', pickup_datetime) as revenue_month, 
    --Note: For BQ use instead: 
    date_trunc(pickup_datetime, month) as revenue_month, 


    -- Additional calculations
    count(tripid) as total_monthly_trips,

    from trips_data
    group by 1