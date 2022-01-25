-- How many taxi trips were there on January 15? 
select count(1) 
from yellow_cabs 
where extract(day from tpep_pickup_datetime) = 15 
and extract(year from tpep_pickup_datetime) = 2021;

-- "On which day it was the largest tip in January? (note: it's not a typo, it's "tip", not "trip")"

select tpep_pickup_datetime, tpep_dropoff_datetime, tip_amount 
from yellow_cabs order by 3 desc;

-- What was the most popular destination for passengers picked up in central park on January 14? Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown"


SELECT CONCAT(EXTRACT(YEAR FROM t."tpep_pickup_datetime"), '-0', EXTRACT(MONTH FROM t."tpep_pickup_datetime"), '-', EXTRACT(DAY FROM t."tpep_pickup_datetime")),
zpu."Zone",
zdo."Zone",
COUNT(zdo."Zone")
FROM yellow_cabs t JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE EXTRACT(DAY FROM t."tpep_pickup_datetime") = 14 AND EXTRACT(YEAR FROM t."tpep_pickup_datetime") = 2021 AND zpu."Zone" = 'Central Park'
GROUP BY 1, 2, 3
ORDER BY 4 DESC;

--What's the pickup-dropoff pair with the largest average price for a ride (calculated based on total_amount)? Enter two zone names separated by a slashFor example:"Jamaica Bay / Clinton East If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East

SELECT CONCAT(zpu."Zone",'/ ', zdo."Zone"),
AVG(t."total_amount")
FROM yellow_cabs t JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
GROUP BY 1
ORDER BY 2 DESC;