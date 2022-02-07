-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.external_yellow_tripdata_2019_2020`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mide_data_lake_endless-context-338620/raw/yellow_tripdata_2019-*.parquet', 'gs://mide_data_lake_endless-context-338620/raw/yellow_tripdata_2020-*.parquet']
);


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE trips_data_all.external_yellow_tripdata_2019_2020_non_partitoned AS
SELECT * FROM trips_data_all.external_yellow_tripdata_2019_2020;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE trips_data_all.external_yellow_tripdata_2019_2020_partitoned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM trips_data_all.external_yellow_tripdata_2019_2020;


-- Impact of partition
-- Scanning 14GB of data, 1.7 sec
SELECT * 
FROM `endless-context-338620.trips_data_all.external_yellow_tripdata_2019_2020_non_partitoned` 
WHERE tpep_pickup_datetime BETWEEN '2021-04-01' AND '2021-04-30'

-- Scanning  417B of data, 0.2 sec
SELECT * FROM `endless-context-338620.trips_data_all.external_yellow_tripdata_2019_2020_partitoned` 
WHERE tpep_pickup_datetime BETWEEN '2021-04-01' AND '2021-04-30'

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM endless-context-338620.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name = external_yellow_tripdata_2019_2020_partitoned
ORDER BY total_rows DESC;


-- Creating a partition and cluster table
CREATE OR REPLACE TABLE trips_data_all.external_yellow_tripdata_2019_2020_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM trips_data_all.external_yellow_tripdata_2019_2020;

-- Impact of clustering
-- Scanning 14GB of data, 0.5 sec
SELECT * 
FROM `endless-context-338620.trips_data_all.external_yellow_tripdata_2019_2020_non_partitoned` 
WHERE tpep_pickup_datetime BETWEEN '2021-04-01' AND '2021-04-30' AND  VendorID = 2;

-- Scanning  417B of data, 0.2 sec
SELECT * FROM `endless-context-338620.trips_data_all.external_yellow_tripdata_2019_2020_partitoned` 
WHERE tpep_pickup_datetime BETWEEN '2021-04-01' AND '2021-04-30' AND  VendorID = 2;

-- Scanning  417B of data, 0.2 sec
SELECT * FROM `endless-context-338620.trips_data_all.external_yellow_tripdata_2019_2020_partitoned_clustered` 
WHERE tpep_pickup_datetime BETWEEN '2021-04-01' AND '2021-04-30' AND  VendorID = 1;


CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.fhv_tripdata_2019`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mide_data_lake_endless-context-338620/for_hire_vehicle/fhv_tripdata_2019-*.parquet']
);
