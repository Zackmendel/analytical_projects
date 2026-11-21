/*
=====================================================================================================
CHECKS THE RESOURCE USED WHEN THE DATE COLUMN (lpep_pickup_datetime) IS USED AS A FILTER
=====================================================================================================
*/


-- EXTERNAL TABLE
-- Uses 728MB resources when run

SELECT 
  DISTINCT(VendorID)
FROM data-engineering-gcp-514411.zoomcamp.green_tripdata_external
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30'
;


-- UN-PARTITIONED TABLE
-- Uses 118.3MB resources when run

SELECT 
  DISTINCT(VendorID)
FROM data-engineering-gcp-514411.zoomcamp.green_tripdata_un_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30'
;


-- PARTITIONED TABLE
-- Uses 7.19MB resources when run

SELECT 
  DISTINCT(VendorID)
FROM data-engineering-gcp-514411.zoomcamp.green_tripdata_partitioned_non_clusterred
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30'
;


-- PARTITIONED & CLUSTERRED TABLE
-- Uses 7.19MB resources when run

SELECT 
  DISTINCT(VendorID)
FROM data-engineering-gcp-514411.zoomcamp.green_tripdata_partioned_and_clusterred
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30'
;




