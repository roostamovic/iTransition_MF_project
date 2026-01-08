# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "78ab0d01-3eeb-4acd-8aaa-f80501acf15e",
# META       "default_lakehouse_name": "MedallionLH",
# META       "default_lakehouse_workspace_id": "c45e0693-30d0-4f7e-955e-3bcca7cd0beb",
# META       "known_lakehouses": [
# META         {
# META           "id": "78ab0d01-3eeb-4acd-8aaa-f80501acf15e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

!pip install openaq

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from openaq import OpenAQ
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, MapType

spark = SparkSession.builder.getOrCreate()

API_KEY = "1731846000fef59c8448375c2ec6b4a931f6ac643f0803a04d862e152a3401a5"
client = OpenAQ(api_key=API_KEY)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

LAT, LONG = 40.7128, -74.0060  # NYC coordinates
resp = client.locations.list(
    coordinates = (LAT, LONG), #(40.7068, -74.005),
    # coordinates = (  74.0060,40.7128), 
    radius=25 * 1000,
    page=1,
    limit=1000
)
data = resp.dict()['results']
locations_df = spark.createDataFrame(data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensor_ids_df = (
    locations_df
    .select(explode(col("sensors")).alias("sensor"))
    .select(col("sensor.id").alias("sensor_id"))
    .distinct()
)

sensor_ids = [row.sensor_id for row in sensor_ids_df.collect()]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time
from math import ceil

RATE_LIMIT = 60          # requests
WINDOW_SECONDS = 40      # per minute

sensor_lookup = {}

total_ids = len(sensor_ids)
num_batches = ceil(total_ids / RATE_LIMIT)

for batch_idx in range(num_batches):
    start = batch_idx * RATE_LIMIT
    end = min(start + RATE_LIMIT, total_ids)

    batch = sensor_ids[start:end]

    print(f"Fetching sensors {start + 1}–{end} / {total_ids}")

    for sensor_id in batch:
        try:
            response = client.sensors.get(sensor_id).dict()
            sensor_lookup[sensor_id] = response.get("results", [])
        except Exception as e:
            print(f"⚠️ Failed to fetch sensor {sensor_id}: {e}")
            sensor_lookup[sensor_id] = []

    # ⏱️ Wait only if there are more batches left
    if batch_idx < num_batches - 1:
        print(f"⏳ Rate limit reached — sleeping {WINDOW_SECONDS} seconds...")
        time.sleep(WINDOW_SECONDS)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

broadcast_sensors = spark.sparkContext.broadcast(sensor_lookup)

from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, MapType, StringType

def enrich_sensors(sensors):
    enriched = []
    lookup = broadcast_sensors.value
    for s in sensors:
        sid = s["id"]
        enriched.extend(lookup.get(sid, []))
    return enriched

enrich_udf = udf(enrich_sensors, ArrayType(MapType(StringType(), StringType())))

enriched_df = locations_df.withColumn(
    "sensors",
    enrich_udf(col("sensors"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# import json
# from datetime import datetime

# bronze_path = f"Files/Bronze/WeatherData"

enriched_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .format('json') \
    .save("Files/Bronze/WeatherData/NYC")

# pd_df = enriched_df.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
