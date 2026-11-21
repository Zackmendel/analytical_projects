-- Picking the requred features to predict the tip_amount of a ride
SELECT 
  passenger_count, 
  trip_distance, 
  PULocationID, 
  DOLocationID, 
  payment_type, 
  fare_amount, 
  tolls_amount, 
  tip_amount -- to be predicted (y)
FROM `data-engineering-gcp-514411.zoomcamp.green_tripdata_partitioned_non_clusterred` 
WHERE fare_amount != 0
  AND DATE(lpep_pickup_datetime) = '2019-01-01'
;

-- Our categories PULocationID, DOLocationID and payment_type are all of int type, bur because they are categorical and to help our model understand this, we will be casting them to string. We will create another table containing these changes
CREATE OR REPLACE TABLE `data-engineering-gcp-514411.zoomcamp.green_tripdata_ml` (
  `passenger_count` INTEGER,
  `trip_distance`   FLOAT64,
  `PULocationID`    STRING,
  `DOLocationID`    STRING,
  `payment_type`    STRING,
  `fare_amount`     FLOAT64,
  `tolls_amount`    FLOAT64,
  `tip_amount`      FLOAT64
) AS (
  SELECT 
    passenger_count, 
    trip_distance, 
    cast(PULocationID AS STRING), 
    CAST(DOLocationID AS STRING),
    CAST(payment_type AS STRING), 
    fare_amount, 
    tolls_amount, 
    tip_amount
  FROM `data-engineering-gcp-514411.zoomcamp.green_tripdata_partitioned_non_clusterred` 
  WHERE fare_amount != 0
);

-- Next, we create our model and fit our dataset into it
CREATE OR REPLACE MODEL `data-engineering-gcp-514411.zoomcamp.tip_model`
OPTIONS (
  model_type='linear_reg',
  input_label_cols=['tip_amount'],
  DATA_SPLIT_METHOD='AUTO_SPLIT'
) AS

SELECT
  *
FROM `data-engineering-gcp-514411.zoomcamp.green_tripdata_ml`
WHERE tip_amount IS NOT NULL
;


-- Checking the features
SELECT * 
FROM ML.FEATURE_INFO(MODEL `data-engineering-gcp-514411.zoomcamp.tip_model`)
;


-- EVALUATING THE MODEL: we use ML.EVALUATE to score the model against a dataset.
/*
RESULT
mean_absolute_error	mean_squared_error	mean_squared_log_error	median_absolute_error	r2_score	explained_variance
0.87568991743805746	2.9807284933932325	0.36740862266852903	0.50526544682070362	0.3075096438383379	0.30750964387314517
*/
SELECT 
  *
FROM ML.EVALUATE (
  MODEL `data-engineering-gcp-514411.zoomcamp.tip_model`,
          (
            SELECT *
            FROM `data-engineering-gcp-514411.zoomcamp.green_tripdata_ml`
            WHERE tip_amount IS NOT NULL
          )
      )
;


-- PREDICTING VALUES: we use ML.PREDICT to apply the model to the dataset which then add the prediction as an extra column
SELECT 
  *
FROM ML.PREDICT (
  MODEL `data-engineering-gcp-514411.zoomcamp.tip_model`,
          (
            SELECT *
            FROM `data-engineering-gcp-514411.zoomcamp.green_tripdata_ml`
            WHERE tip_amount IS NOT NULL
          )
      )
;


-- EXPLAINING PREDICTIONS: we can also tell which features drive a prediction by using ML.EXPLAIN_PREDICT
/*
RESULT
Row	predicted_tip_amount	top_feature_attributions. feature	top_feature_attributions. attribution	baseline_prediction_value	prediction_value	approximation_error	passenger_count	trip_distance	PULocationID	DOLocationID	payment_type	fare_amount	tolls_amount	tip_amount
1	2.7801496683478035	PULocationID	-9382.1037433278543	9197.7201731619261	2.7801496683478035	0.0	1	22.21	66	76	1	59.5	0.0	0.0
		DOLocationID	205.9829427120942											
		payment_type	-19.4041072296575											
2	2.0747187228098483	PULocationID	-9382.8372437996259	9197.7201731619261	2.0747187228098483	0.0	1	19.05	75	76	1	63.5	6.12	0.0
		DOLocationID	205.9829427120942											
		payment_type	-19.4041072296575		


NOTICE: PULocationID, DOLocationID & payment_type are the most repeated categories signifying that our model heavily relies on them							
*/
SELECT 
  *
FROM ML.EXPLAIN_PREDICT (
  MODEL `data-engineering-gcp-514411.zoomcamp.tip_model`,
          (
            SELECT *
            FROM `data-engineering-gcp-514411.zoomcamp.green_tripdata_ml`
            WHERE tip_amount IS NOT NULL
          ), 
          STRUCT(3 as top_k_features)
      )
;


-- HYPERPARAMETER TUNING: our model is not optimal, the best way to fine tune it is using hyperparameters
CREATE OR REPLACE MODEL `data-engineering-gcp-514411.zoomcamp.tip_hyperparam_model`
OPTIONS
  (
    model_type='linear_reg',
    input_label_cols=['tip_amount'],
    DATA_SPLIT_METHOD='AUTO_SPLIT',
    num_trials=5,
    max_parallel_trials=2,
    l1_reg=hparam_range(0, 20),
    l2_reg=hparam_candidates([0, 0.1, 1, 10])
  ) AS
  SELECT *
  FROM `data-engineering-gcp-514411.zoomcamp.green_tripdata_ml`
  WHERE tip_amount IS NOT NULL
;