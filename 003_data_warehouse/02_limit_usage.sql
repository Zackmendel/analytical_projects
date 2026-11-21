/*
=====================================================================================================
CHECKS THE RESOURCE USED FOR A QUICK TABLE SCAN
=====================================================================================================
*/


-- EXTERNAL TABLE
-- Uses 1B resources when run

SELECT *
FROM data-engineering-gcp-514411.zoomcamp.green_tripdata_external
LIMIT 10
;


-- UN-PARTITIONED TABLE
-- Uses 1.07GB resources when run

SELECT *
FROM data-engineering-gcp-514411.zoomcamp.green_tripdata_un_partitioned
LIMIT 10
;


-- PARTITIONED TABLE
-- Uses 1.07GB resources when run

SELECT *
FROM data-engineering-gcp-514411.zoomcamp.green_tripdata_partitioned_non_clusterred
LIMIT 10
;


-- PARTITIONED & CLUSTERRED TABLE
-- Uses 1.07GB resources when run

SELECT *
FROM data-engineering-gcp-514411.zoomcamp.green_tripdata_partioned_and_clusterred
LIMIT 10
;




