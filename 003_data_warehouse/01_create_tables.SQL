/*
=====================================================================================================
=====================================================================================================
DIFFERENT TYPES OF TABLES WERE CREATED ON BIGQUERY TO CHECK AND UNDERSTAND HOW THEY FUNCTION AND HOW TO BETTER OPTIMIZE THEM.
=====================================================================================================
=====================================================================================================
*/

-- Create an external table as an experiment to see how it compares to an internal table that is partitioned and then clusttered
CREATE OR REPLACE EXTERNAL TABLE data-engineering-gcp-514411.zoomcamp.green_tripdata_external
OPTIONS (
  format = 'csv',
  uris = [
        'gs://zoomcamp-zackmendel/green_tripdata_2019-*.csv',
        'gs://zoomcamp-zackmendel/green_tripdata_2020-*.csv',
        'gs://zoomcamp-zackmendel/green_tripdata_2021-*.csv'
    ]
);

-- Create an internal table from the external table that is NEITHER partitioned nor clusterred.
CREATE OR REPLACE TABLE `data-engineering-gcp-514411.zoomcamp.green_tripdata_un_partitioned` AS
SELECT *
FROM `data-engineering-gcp-514411.zoomcamp.green_tripdata_external`
;

-- Create an internal table that is PARTITIONED but NOT clusterred.
CREATE OR REPLACE TABLE `data-engineering-gcp-514411.zoomcamp.green_tripdata_partitioned_non_clusterred`
PARTITION BY DATE(lpep_pickup_datetime) AS
SELECT *
FROM `data-engineering-gcp-514411.zoomcamp.green_tripdata_external`
;

-- Create an internal table that is BOTH partitioned and clusterred
CREATE OR REPLACE TABLE `data-engineering-gcp-514411.zoomcamp.green_tripdata_partioned_and_clusterred`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY VendorID AS 
SELECT *
FROM `data-engineering-gcp-514411.zoomcamp.green_tripdata_external`
;





