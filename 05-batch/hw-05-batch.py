import os
import pyspark
from pyspark.sql import SparkSession
import structlog
# Construct
LOG: structlog.stdlib.BoundLogger = structlog.get_logger()

#-------------------------------- Question 1
LOG.info("Pyspark version", version=pyspark.__version__)

#-------------------------------- Question 2
# Initialize Spark Session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("hw-05-batch") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()

# # Read the Parquet file
df_yellow_tripdata = spark.read.parquet("05-batch/data/yellow_tripdata_2024-10.parquet")

# # Write the DataFrame to Parquet file
# df.repartition(4).write.parquet("05-batch/data/yellow_tripdata_2024-10_repartitioned_parquet")

# LOG.info("Data has been written to Parquet file successfully")

folder_path = "05-batch/data/yellow_tripdata_2024-10_repartitioned_parquet"

def average_parquet_size(folder_path):
    parquet_files = [f for f in os.listdir(folder_path) if f.endswith(".parquet")]
    
    if not parquet_files:
        LOG.info("No Parquet files found.")
        return 0
    
    total_size = sum(os.path.getsize(os.path.join(folder_path, f)) for f in parquet_files)
    avg_size = total_size / len(parquet_files)

    LOG.info("Total Parquet Files", total_parquet_files=len(parquet_files))
    LOG.info("Average Size", average_size=f"{avg_size / (1024 * 1024):.2f} MB")

average_parquet_size(folder_path)

#-------------------------------- Question 3
from pyspark.sql.functions import col, to_date

df_filtered = df_yellow_tripdata.filter(to_date(col("tpep_pickup_datetime")) == "2024-10-15")

LOG.info("Number of rows in the filtered DataFrame", num_rows=df_filtered.count())

#-------------------------------- Question 4

from pyspark.sql.functions import unix_timestamp

df_with_duration = df_yellow_tripdata.withColumn(
    "duration_hours", 
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime")))/3600
    ).orderBy(col("duration_hours").desc())

LOG.info(df_with_duration.first())

#-------------------------------- Question 5
url = "https://spark.apache.org/docs/3.5.5/cluster-overview.html"

LOG.info("The url is", url=url)
LOG.info("The Answer to the Question in url is 4040" )

#-------------------------------- Question 6
df_zone_lookup = spark.read.csv("05-batch/data/taxi_zone_lookup.csv", header=True, inferSchema=True)

LOG.info(df_zone_lookup.printSchema())
LOG.info(df_yellow_tripdata.printSchema())

# Create zone_lookup view and join with yellow_tripdata view
df_zone_lookup.createOrReplaceTempView("zone_lookup")
df_yellow_tripdata.createOrReplaceTempView("yellow_tripdata")

least_frequent_zone = spark.sql("""
    SELECT z.Zone as pickup_zone, COUNT(*) as trip_count
    FROM yellow_tripdata y
    JOIN zone_lookup z 
    ON y.PULocationID = z.LocationID
    GROUP BY z.Zone
    ORDER BY trip_count ASC
    LIMIT 1
""")

LOG.info(least_frequent_zone.show())
