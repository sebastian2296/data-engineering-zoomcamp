-- Joining tables

-- First way

SELECT tpep_pickup_datetime, tpep_dropoff_datetime, total_amount,
CONCAT(zpu."Borough" , '/ ', zpu."Zone") as "pickup_loc",
CONCAT(zdo."Borough", '/ ' , zdo."Zone") as "pickup_loc"
FROM yellow_cabs t,
zones zpu,
zones zdo
WHERE 
	t."PULocationID" = zpu."LocationID"
	AND t."DOLocationID" = zdo."LocationID"
LIMIT 100;

-- Second way

SELECT tpep_pickup_datetime, tpep_dropoff_datetime, total_amount,
CONCAT(zpu."Borough" , '/ ', zpu."Zone") as "pickup_loc",
CONCAT(zdo."Borough", '/ ' , zdo."Zone") as "dropoff_loc",
"PULocationID", "DOLocationID"
FROM yellow_cabs t JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
LIMIT 100;