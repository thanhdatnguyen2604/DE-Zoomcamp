
## Homework Solutions

### Question 1: Understanding dbt model resolution
**Answer**: `select * from myproject.raw_nyc_tripdata.ext_green_taxi`

### Question 2: dbt Variables & Dynamic Models
**Answer**: `Update the WHERE clause to pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY`

### Question 3: dbt Data Lineage and Execution
**Answer**: `dbt run --select models/staging/+`

### Question 4: dbt Macros and Jinja
**Correct statements**:
- Setting a value for `DBT_BIGQUERY_TARGET_DATASET` env var is mandatory, or it'll fail to compile
- When using `core`, it materializes in the dataset defined in `DBT_BIGQUERY_TARGET_DATASET`
- When using `stg`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET`
- When using `staging`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET`


### Question 5: Taxi Quarterly Revenue Growth
```sql
SELECT 
  service_type,
  quarter,
  year_quarter,
  yoy_growth_percentage
FROM `zoomcamp-450020.dbt_dkumar.fct_taxi_trips_quarterly_revenue`
WHERE year = 2020
ORDER BY service_type, yoy_growth_percentage ASC;
```

**Answer**: green: {best: 2020/Q1, worst: 2020/Q2} yellow: {best: 2020/Q1, worst: 2020/Q2}

### Question 6: P97/P95/P90 Taxi Monthly Fare
```sql
SELECT 
  service_type,
  p97, p95, p90
FROM `zoomcamp-450020.dbt_dkumar.fct_taxi_trips_monthly_fare_p95`
WHERE year = 2020 AND month = 4
ORDER BY service_type;
```

**Answer**: green: {p97: 55.0, p95: 45.0, p90: 26.5} yellow: {p97: 31.5, p95: 25.5, p90: 19.0}

### Question 7: Top #Nth longest P90 travel time Location for FHV
```sql
WITH ranked_destinations AS (
  SELECT
    pickup_zone,
    dropoff_zone,
    p90_duration,
    DENSE_RANK() OVER (PARTITION BY pickup_zone ORDER BY p90_duration DESC) AS duration_rank
  FROM `zoomcamp-450020.dbt_dkumar.fct_fhv_monthly_zone_traveltime_p90`
  WHERE 
    year = 2019 
    AND month = 11
    AND pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
)
SELECT
  pickup_zone,
  dropoff_zone,
  p90_duration
FROM ranked_destinations
WHERE duration_rank = 2
ORDER BY pickup_zone
```
**Answer** : LaGuardia Airport, Chinatown, Garment District