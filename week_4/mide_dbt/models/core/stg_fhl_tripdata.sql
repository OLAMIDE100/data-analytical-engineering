{{ config(materialized='table') }}

with tripdata as 
(
  select *
  from {{ source('core','fhv_tripdata_2019') }} 
)
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['Dispatching_base_num', 'pickup_datetime']) }} as tripid,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    SR_Flag,
   
from tripdata


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}