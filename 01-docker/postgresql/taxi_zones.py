import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()

query = """
SELECT * FROM green_taxi_trips LIMIT 10
"""
print(pd.read_sql(query, con=engine))

query_2 = """
WITH trips_by_distance AS (
    SELECT 
        CASE 
            WHEN trip_distance <= 1 THEN '0-1 miles'
            WHEN trip_distance > 1 AND trip_distance <= 3 THEN '1-3 miles'
            WHEN trip_distance > 3 AND trip_distance <= 7 THEN '3-7 miles'
            WHEN trip_distance > 7 AND trip_distance <= 10 THEN '7-10 miles'
            WHEN trip_distance > 10 THEN 'over 10 miles'
        END AS distance_range,
        COUNT(*) as number_trips
    FROM 
        green_taxi_trips
    WHERE 
        lpep_pickup_datetime >= '2019-10-01' 
        AND lpep_pickup_datetime < '2019-11-01'
    GROUP BY 1
)
SELECT *
FROM trips_by_distance
ORDER BY 
    CASE distance_range
        WHEN '0-1 miles' THEN 1
        WHEN '1-3 miles' THEN 2
        WHEN '3-7 miles' THEN 3
        WHEN '7-10 miles' THEN 4
        WHEN 'over 10 miles' THEN 5
    END;
"""
# print(pd.read_sql(query_2, con=engine))

query_3 = """
SELECT 
    DATE(lpep_pickup_datetime) as pickup_day,
    MAX(trip_distance) as longest_distance
FROM 
    green_taxi_trips
GROUP BY 
    DATE(lpep_pickup_datetime)
ORDER BY 
    longest_distance DESC
LIMIT 1;
"""
# print(pd.read_sql(query_3, con=engine))

query_4 = """
SELECT 
    zpu."Zone" as pickup_zone,
    SUM(total_amount) as total_amount
FROM 
    green_taxi_trips t
    JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
WHERE 
    DATE(lpep_pickup_datetime) = '2019-10-18'
GROUP BY 
    zpu."Zone"
HAVING 
    SUM(total_amount) > 13000
ORDER BY 
    total_amount DESC;
"""
# print(pd.read_sql(query_4, con=engine))

query_5 = """
SELECT 
    zdo."Zone" as dropoff_zone,
    MAX(tip_amount) as max_tip
FROM 
    green_taxi_trips t
    JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
    JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE 
    zpu."Zone" = 'East Harlem North'
    AND DATE(lpep_pickup_datetime) >= '2019-10-01'
    AND DATE(lpep_pickup_datetime) < '2019-11-01'
GROUP BY 
    zdo."Zone"
ORDER BY 
    max_tip DESC
LIMIT 1;
"""
print(pd.read_sql(query_5, con=engine))