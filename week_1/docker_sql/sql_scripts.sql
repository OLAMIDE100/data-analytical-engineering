QUESTION 1
SELECT
    CAST(tpep_pickup_datetime AS DATE) AS day,
    COUNT(1)
FROM
    yellow_taxi_trips t
WHERE
     CAST(tpep_pickup_datetime AS DATE) = '2021-01-15'
GROUP BY
    CAST(tpep_pickup_datetime AS DATE)

QUESTION 2
SELECT
    CAST(tpep_pickup_datetime AS DATE) AS day,
    Tip_amount AS amount
FROM
    yellow_taxi_trips
WHERE
     CAST(tpep_pickup_datetime AS DATE) BETWEEN '2021-01-01' AND '2021-01-31'
ORDER BY
       amount DESC
LIMIT 1  
QUESTION 3
SELECT
  z."Zone",
  COUNT(1)
FROM
    yellow_taxi_trips as t
JOIN
    taxi_zone_lookup as z
ON 
  z."LocationID" = t."DOLocationID"
WHERE
     CAST(t.tpep_pickup_datetime AS DATE) = '2021-01-14'
GROUP BY
     z."Zone"
ORDER BY
     count DESC
QUESTION 4
SELECT 
	CONCAT(COALESCE(CAST("PUZONE" AS VARCHAR(50)),'unknown'), '/', COALESCE(CAST(zod."Zone" AS VARCHAR(50)),'unknown')) AS "pickup_dropoff",
	SUM(total_amount)/COUNT(*) AS average
FROM
	(SELECT 
		 t."PULocationID" AS "PU",
		 t."DOLocationID" AS "DO",
		 z."Zone" AS "PUZONE",
		 total_amount
	FROM 
		yellow_taxi_trips AS t
	JOIN
	 	taxi_zone_lookup AS z
	ON 
	 	t."PULocationID" = z."LocationID") AS t1
JOIN 
	taxi_zone_lookup AS zod
ON 
	t1."DO" = zod."LocationID" 
GROUP BY 
	"pickup_dropoff"
ORDER BY 
	"average" DESC
LIMIT 1 