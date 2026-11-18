/*
3.
For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), 
how many trips had a trip_distance of less than or equal to 1 mile?
*/

SELECT 
	COUNT(*)
FROM public.green_tripdata
WHERE lpep_pickup_datetime >= '2025-11-01' 
	AND lpep_pickup_datetime < '2025-12-01'
	AND trip_distance <= 1
;

/*
4.
Which was the pick up day with the longest trip distance? 
Only consider trips with trip_distance less than 100 miles (to exclude data errors).
Use the pick up time for your calculations.
*/
SELECT 
	DATE_TRUNC('DAY', lpep_pickup_datetime) AS dates,
	trip_distance
FROM public.green_tripdata
WHERE trip_distance < 100
ORDER BY trip_distance DESC
LIMIT 1
;

/*
5.
Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?
*/
SELECT 
	"Zone" AS zone,
	ROUND(SUM(trip_distance)::numeric, 2) AS total_distance
FROM public.green_tripdata g
JOIN taxi_zone_lookup z
	ON g."PULocationID" = z."LocationID"
WHERE DATE_TRUNC('DAY', lpep_pickup_datetime) = '2025-11-18'
GROUP BY zone
ORDER BY total_distance DESC
LIMIT 1
;

/*
6.
For the passengers picked up in the zone named "East Harlem North" in November 2025, 
which was the drop off zone that had the largest tip?

Note: it's tip , not trip. We need the name of the zone, not the ID.
*/
SELECT 
	dro."Zone" AS drop_zone,
	pu."Zone" AS pickup_zone,
	tip_amount
FROM public.green_tripdata g
JOIN taxi_zone_lookup pu
	ON g."PULocationID" = pu."LocationID"
JOIN taxi_zone_lookup dro
	ON g."DOLocationID" = dro."LocationID"
WHERE pu."Zone" = 'East Harlem North'
ORDER BY tip_amount DESC
LIMIT 1
;
