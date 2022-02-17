<-- QUESTION 1 -->
SELECT COUNT(*)
FROM trips_data_all.fhv_tripdata_2019


<-- QUESTION 2 -->
SELECT COUNT(DISTINCT(dispatching_base_num))
FROM trips_data_all.fhv_tripdata_2019

<-- QUESTION 3 -->
CREATE OR REPLACE TABLE trips_data_all.fhv_tripdata_2019_non_partitoned AS
SELECT * FROM trips_data_all.fhv_tripdata_2019;

CREATE OR REPLACE TABLE trips_data_all.fhv_tripdata_2019_partitoned_datetime
PARTITION BY
  DATE(dropoff_datetime) AS
SELECT * FROM trips_data_all.fhv_tripdata_2019;

CREATE OR REPLACE TABLE trips_data_all.fhv_tripdata_2019_partitoned_dispatching
PARTITION BY
  DATE(dispatching_base_num) AS
SELECT * FROM trips_data_all.fhv_tripdata_2019;

CREATE OR REPLACE TABLE trips_data_all.fhv_tripdata_2019_partitoned_datetime_cluster_dispatching
PARTITION BY
  DATE(dropoff_datetime) 
CLUSTER BY 
  dispatching_base_num AS
SELECT * FROM trips_data_all.fhv_tripdata_2019;

CREATE OR REPLACE TABLE trips_data_all.fhv_tripdata_2019_partitoned__cluster_datetime_dispatching
PARTITION BY
  DATE(dispatching_base_num) 
CLUSTER BY 
  dropoff_datetime AS
SELECT * FROM trips_data_all.fhv_tripdata_2019;


<-- QUESTION 4 -->
SELECT *
FROM trips_data_all.fhv_tripdata_2019
WHERE DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31' AND dispatching_base_num IN ('B00987','B02060','B02279') 