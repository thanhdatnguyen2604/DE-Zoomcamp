Q3:
SELECT COUNT(*) as total_rows
FROM public.yellow_tripdata 
WHERE filename LIKE 'yellow_tripdata_2020-__.csv';

Q4:
SELECT COUNT(*) as total_rows
FROM public.green_tripdata 
WHERE filename LIKE 'green_tripdata_2020-__.csv';

Q5:
SELECT COUNT(*) as total_rows
FROM public.yellow_tripdata 
WHERE filename = 'yellow_tripdata_2021-03.csv';