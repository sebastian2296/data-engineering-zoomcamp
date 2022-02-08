-- 1. What is count for fhv vehicles data for year 2019

CREATE OR REPLACE EXTERNAL TABLE `norse-bond-337916.trips_data_all.fhv_table`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_norse-bond-337916/raw/fhv_tripdata_2019-*.parquet']
);

-- ans: 42084899

-- 2.How many distinct dispatching_base_num we have in fhv for 2019

SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `norse-bond-337916.trips_data_all.fhv_table` 

-- 3.Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num

CREATE OR REPLACE TABLE `norse-bond-337916.trips_data_all.fhv_tripdata_partitoned`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM `norse-bond-337916.trips_data_all.fhv_table`;

-- 4.What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279 

SELECT COUNT(*) FROM `norse-bond-337916.trips_data_all.fhv_tripdata_partitoned` 
WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
AND dispatching_base_num IN ('B00987', 'B02060', 'B02279');