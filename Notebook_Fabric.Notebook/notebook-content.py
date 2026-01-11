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
WINDOW_SECONDS = 35      # per minute

sensor_rows = []

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
            for r in response.get("results", []):
                #r["sensor_id"] = sensor_id
                sensor_rows.append(r)
        except Exception as e:
            print(f"⚠️ Failed to fetch sensor {sensor_id}: {e}")


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

from pyspark.sql.types import *

schema = StructType([

    StructField("id", LongType(), True),
    StructField("name", StringType(), True),

    StructField("parameter", StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("units", StringType(), True),
        StructField("display_name", StringType(), True)
    ]), True),

    StructField("datetime_first", StructType([
        StructField("utc", StringType(), True),
        StructField("local", StringType(), True)
    ]), True),

    StructField("datetime_last", StructType([
        StructField("utc", StringType(), True),
        StructField("local", StringType(), True)
    ]), True),

    StructField("coverage", StructType([
        StructField("expected_count", LongType(), True),
        StructField("expected_interval", StringType(), True),
        StructField("observed_count", LongType(), True),
        StructField("observed_interval", StringType(), True),
        StructField("percent_complete", DoubleType(), True),
        StructField("percent_coverage", DoubleType(), True),
        StructField("datetime_from", StructType([
            StructField("utc", StringType(), True),
            StructField("local", StringType(), True)
        ]), True),
        StructField("datetime_to", StructType([
            StructField("utc", StringType(), True),
            StructField("local", StringType(), True)
        ]), True)
    ]), True),

    StructField("latest", StructType([
        StructField("datetime", StructType([
            StructField("utc", StringType(), True),
            StructField("local", StringType(), True)
        ]), True),
        StructField("value", DoubleType(), True),
        StructField("coordinates", StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True)
        ]), True)
    ]), True),

    StructField("summary", StructType([
        StructField("min", DoubleType(), True),
        StructField("q02", DoubleType(), True),
        StructField("q25", DoubleType(), True),
        StructField("median", DoubleType(), True),
        StructField("q75", DoubleType(), True),
        StructField("q98", DoubleType(), True),
        StructField("max", DoubleType(), True),
        StructField("avg", DoubleType(), True),
        StructField("sd", DoubleType(), True)
    ]), True)

])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors_df = spark.createDataFrame(sensor_rows, schema=schema)
sensors_df = (
                sensors_df.withColumnRenamed("id", "sensor_id") \
                          .withColumnRenamed("name", "param_name")
            )
sensors_df = sensors_df.drop("datetime_first", "datetime_last")

silver = (
    locations_df
    .withColumn("sensor", explode("sensors"))
    .join(
        sensors_df,
        col("sensor.id") == col("sensor_id"),
        "left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# resp = client.sensors.get(7972702).dict()
# sample = spark.createDataFrame(list(resp.get('results', [])), schema=schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# broadcast_sensors = spark.sparkContext.broadcast(sensor_lookup)

# from pyspark.sql.functions import udf
# from pyspark.sql.types import ArrayType, MapType, StringType

# def enrich_sensors(sensors):
#     enriched = []
#     lookup = broadcast_sensors.value
#     for s in sensors:
#         sid = s["id"]
#         enriched.extend(lookup.get(sid, []))
#     return enriched

# enrich_udf = udf(enrich_sensors, ArrayType(MapType(StringType(), StringType())))

# enriched_df = locations_df.withColumn(
#     "sensors",
#     enrich_udf(col("sensors"))
# )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# import json
# from datetime import datetime

# bronze_path = f"Files/Bronze/WeatherData"

silver \
    .write \
    .mode("append") \
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
