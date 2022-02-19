{{ config(materialized='table') }}

with fhl_data as (
    select *, 
    from {{ ref('stg_fhl_tripdata') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    fhl_data.tripid, 
    fhl_data.pickup_locationid, 
    fhl_data.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhl_data.pickup_datetime, 
    fhl_data.dropoff_datetime, 
    fhl_data.SR_Flag,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    pickup_zone.service_zone as pickup_service_zone,
    dropoff_zone.service_zone as dropoff_service_zone

from fhl_data
inner join dim_zones as pickup_zone
on fhl_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhl_data.dropoff_locationid = dropoff_zone.locationid