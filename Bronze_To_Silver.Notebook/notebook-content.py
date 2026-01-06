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

df = spark.read.option("multiline", "true").json("Files/Bronze/WeatherData/155/openaq-155-2026-01-05.json")
# df now is a Spark DataFrame containing JSON data from "Files/Bronze/WeatherData/41/openaq-41-2025-12-29.json".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df = spark.read \
#     .option("multiline", "true").json("Files/Bronze/WeatherData/41/openaq-41-2025-12-29.json")

# df.printSchema()
# df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import explode, col

locations_df = df.select(
    explode("results").alias("r")
)

display(locations_df)

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

# CELL ********************

sensors_df = locations_df.select(
    col("r.id").alias("location_id"),
    col("r.name").alias("location_name"),
    col("r.locality"),
    col("r.timezone"),
    col("r.isMobile"),
    col("r.isMonitor"),

    # coordinates
    col("r.coordinates.latitude").alias("latitude"),
    col("r.coordinates.longitude").alias("longitude"),

    # country
    col("r.country.code").alias("country_code"),
    col("r.country.name").alias("country_name"),

    # provider
    col("r.provider.id").alias("provider_id"),
    col("r.provider.name").alias("provider_name"),

    # owner
    col("r.owner.id").alias("owner_id"),
    col("r.owner.name").alias("owner_name"),

    explode("r.sensors").alias("s")
)

display(sensors_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df = sensors_df.select(
    "location_id",
    "location_name",
    "locality",
    "timezone",
    "isMobile",
    "isMonitor",
    "latitude",
    "longitude",
    "country_code",
    "country_name",
    "provider_id",
    "provider_name",
    "owner_id",
    "owner_name",

    col("s.id").alias("sensor_id"),
    col("s.name").alias("sensor_name"),

    col("s.parameter.id").alias("parameter_id"),
    col("s.parameter.name").alias("parameter_name"),
    col("s.parameter.displayName").alias("parameter_display_name"),
    col("s.parameter.units").alias("unit")
)

display(silver_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(silver_df.tail(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

!pip install openaq

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

# CELL ********************

from openaq import OpenAQ
import requests

API_KEY = "1731846000fef59c8448375c2ec6b4a931f6ac643f0803a04d862e152a3401a5"
client = OpenAQ(api_key=API_KEY)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

resp = client.locations.list(
    coordinates = (40.7128, -74.0060), #(40.7068, -74.005),
    # coordinates = (  74.0060,40.7128), 
    radius=25 * 1000,
    page=1,
    limit=1000
)
nyc_sensors = set()
for station in resp.dict()['results']:
    for sensor in station['sensors']:
        nyc_sensors.add(sensor['id'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

resp.dict()['results']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

client.sensors.get(12212730).dict()['results']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

client.measurements.list(671).dict()['results']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

pd.read_csv('Files/Bronze/NYCTaxiData/nyc_green.csv')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

nyc_green = spark.read.csv('Files/Bronze/NYCTaxiData/nyc_green.csv')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
# Load data into pandas DataFrame from "/lakehouse/default/Files/Bronze/NYCTaxiData/nyc_green.csv"
df = pd.read_csv("/lakehouse/default/Files/Bronze/NYCTaxiData/nyc_yellow.csv")
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

client.locations.get(155).dict()['results']

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
