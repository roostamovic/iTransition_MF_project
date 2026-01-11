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

# MARKDOWN ********************

# ## NYC Taxi Data

# CELL ********************

import pandas as pd
# Load data into pandas DataFrame from "/lakehouse/default/Files/Bronze/NYCTaxiData/nyc_green.csv"
# yellow = pd.read_csv("/lakehouse/default/Files/Bronze/NYCTaxiData/nyc_yellow.csv")
# green = pd.read_csv("/lakehouse/default/Files/Bronze/NYCTaxiData/nyc_green.csv")
yellow_df = spark.read.format("csv").option("header","true").load("Files/Bronze/NYCTaxiData/nyc_yellow.csv")
green_df = spark.read.format("csv").option("header","true").load("Files/Bronze/NYCTaxiData/nyc_green.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# yellow_df = spark.createDataFrame(yellow)
# green_df = spark.createDataFrame(green)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lit, to_timestamp

timestamp_format = "M/d/yyyy h:mm:ss a"

yellow_silver = (
    yellow_df
    .select(
        col("tpep_pickup_datetime").alias("pickup_datetime"),
        col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        "pulocationid",
        "dolocationid",
        "passenger_count",
        "trip_distance",
        "ratecodeid",
        "fare_amount",
        "tip_amount",
        "total_amount"
    )
    .withColumn("pickup_datetime", 
                to_timestamp("pickup_datetime", timestamp_format)
    )
    .withColumn("dropoff_datetime", 
                to_timestamp("dropoff_datetime", timestamp_format)
    )
    .withColumn("taxi_type", lit('yellow'))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

green_silver = (
    green_df
    .select(
        col("lpep_pickup_datetime").alias("pickup_datetime"),
        col("lpep_dropoff_datetime").alias("dropoff_datetime"),
        "pulocationid",
        "dolocationid",
        "passenger_count",
        "trip_distance",
        "ratecodeid",
        "fare_amount",
        "tip_amount",
        "total_amount"
    )
    .withColumn(
        "pickup_datetime",
        to_timestamp("pickup_datetime", timestamp_format)
    )
    .withColumn(
        "dropoff_datetime",
        to_timestamp("dropoff_datetime", timestamp_format)
    )
    .withColumn("taxi_type", lit("green"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

taxi_silver = yellow_silver.unionByName(green_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (
    hour, dayofweek, month, year,
    unix_timestamp, round
)
from pyspark.sql.types import DecimalType

taxi_silver = (
    taxi_silver
    .withColumn("pickup_hour", hour("pickup_datetime"))
    .withColumn("pickup_day", dayofweek("pickup_datetime"))
    .withColumn("pickup_month", month("pickup_datetime"))
    .withColumn("pickup_year", year("pickup_datetime"))
    .withColumn(
        "trip_duration_min",
        round((unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60, 2
        ).cast(DecimalType(10, 2))
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

taxi_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.nyc_taxi_trips")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import count

taxi_silver.groupBy("pickup_hour") \
    .agg(count("*").alias("trip_count")) \
    .orderBy("trip_count", ascending=False) \
    .show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

taxi_silver.groupBy("pulocationid") \
    .agg(count("*").alias("trip_count")) \
    .orderBy("trip_count", ascending=False) \
    .show(10)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

taxi_silver.filter(col("ratecodeid").isin(2, 3, 4)) \
    .groupBy("ratecodeid") \
    .count() \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import avg

taxi_silver.agg(
    avg("total_amount").alias("avg_revenue"),
    avg("trip_duration_min").alias("avg_duration")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

taxi_silver.groupBy("pickup_hour") \
    .agg(avg("total_amount").alias("avg_revenue")) \
    .orderBy("pickup_hour") \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

taxi_silver.groupBy("pickup_hour") \
    .agg(avg("trip_distance").alias("avg_distance")) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

taxi_silver.groupBy("pickup_year", "pickup_month") \
    .agg(
        count("*").alias("trip_count"),
        avg("total_amount").alias("avg_revenue")
    ) \
    .orderBy("pickup_year", "pickup_month") \
    .show()

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

# MARKDOWN ********************

# ## World Bank Data

# CELL ********************

fx_raw = spark.read.format("csv").option("header","true").load("Files/Bronze/WorldBankData/FX_EFB.csv")
# df now is a Spark DataFrame containing CSV data from "Files/Bronze/WorldBankData/FX_EFB.csv".
#display(fx_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, to_date

timestamp_format_fx = "M/d/yyyy"

fx_silver = (
    fx_raw
    .select(
        to_date(col("Date"), timestamp_format_fx).alias("fx_date"),
        col("Rate").cast("double").alias("usd_to_eur")
    )
    .filter(col("usd_to_eur").isNotNull())
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fx_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.fx_efb")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gdp_raw = spark.read.json(
    "Files/Bronze/WorldBankData/GDP.json"
)

gdp_raw = gdp_raw.filter(col("indicator").isNotNull())

gdp_silver = (
    gdp_raw
    .select(
        col("country.value").alias("country"),
        col("countryiso3code").alias("country_code"),
        col("date").cast("int").alias("year"),
        col("value").alias("gdp_usd")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gdp_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.gdp")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import sum as Fsum, avg as Favg, count as Fcount
taxi_silver = taxi_silver.withColumn("trip_date", to_date(col("pickup_datetime"), timestamp_format_fx))
taxi_daily = (
    taxi_silver
    .groupBy("trip_date")
    .agg(
        Fsum("total_amount").alias("revenue_usd"),
        Fcount("*").alias("trips_count"),
        Favg("trip_duration_min").alias("avg_trip_duration_min"),
        Favg("trip_distance").alias("avg_trip_distance")
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(taxi_daily)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

taxi_eur = (
    taxi_daily
    .join(
        fx_silver,
        taxi_daily.trip_date == fx_silver.fx_date,
        "left"
    )
    .withColumn(
        "revenue_eur",
        col("revenue_usd") * col("usd_to_eur")
    )
)
#display(taxi_eur)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import year

taxi_eur = taxi_eur.withColumn("year", year(col("trip_date")))

taxi_yearly = (
    taxi_eur
    .groupBy("year")
    .sum("revenue_eur")
)

macro_vs_taxi = (
    taxi_yearly
    .join(
        gdp_silver.filter(col("country_code") == "USA"),
        taxi_yearly.year == gdp_silver.year
    )
)

#display(macro_vs_taxi)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## OpenAQ API Data

# CELL ********************

import os

nyc_files = os.listdir("/lakehouse/default/Files/Bronze/WeatherData/NYC")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, year, month, dayofweek, hour, current_timestamp

# Path to the raw JSON file in the lakehouse
json_path = f"Files/Bronze/WeatherData/NYC/{nyc_files[-1]}"

# Read JSON
openaq_raw = spark.read.option("multiline", False).json(json_path)

openaq_raw.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Explode the 'sensors' array so each sensor is a separate row
from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver_df = (
    openaq_raw
    .select(
        F.col("id").alias("location_id"),
        F.col("name").alias("location_name"),
        F.col("locality"),
        F.col("coordinates.latitude").cast("double").alias("latitude"),
        F.col("coordinates.longitude").cast("double").alias("longitude"),
        F.lower(F.col("param_name")).alias("parameter_raw"),
        F.col("latest.value").cast("double").alias("value_raw"),
        F.col("summary.avg").cast("double").alias("avg"),
        F.col("summary.min").cast("double").alias("min"),
        F.col("summary.max").cast("double").alias("max"),
        F.to_timestamp("latest.datetime.utc").alias("datetime_utc"),
        F.to_timestamp("latest.datetime.local").alias("datetime_local"),
        F.col("provider.id").alias("provider_id"),
        F.col("is_mobile"),
        F.col("is_monitor")
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df = (
    silver_df
    .withColumn(
        "parameter",
        F.when(F.col("parameter_raw").contains("pm25"), "pm25")
         .when(F.col("parameter_raw").contains("pm10"), "pm10")
         .when(F.col("parameter_raw").contains("o3"), "o3")
         .otherwise("unknown")
    )
    .withColumn(
        "unit",
        F.when(F.col("parameter") == "o3", "ppm")
         .otherwise("µg/m³")
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df = (
    silver_df
    .withColumn(
        "value",
        F.when(F.col("value_raw") < 0, None)
         .when((F.col("parameter") == "pm25") & (F.col("value_raw") > 1000), None)
         .when((F.col("parameter") == "pm10") & (F.col("value_raw") > 1500), None)
         .otherwise(F.col("value_raw"))
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df = (
    silver_df
    .withColumn("date", F.to_date("datetime_utc"))
    .withColumn("hour", F.hour("datetime_utc"))
    .withColumn("year", F.year("datetime_utc"))
    .withColumn("month", F.month("datetime_utc"))
    .withColumn(
        "season",
        F.when(F.col("month").isin(12,1,2), "Winter")
         .when(F.col("month").isin(3,4,5), "Spring")
         .when(F.col("month").isin(6,7,8), "Summer")
         .otherwise("Fall")
    )
    .withColumn("ingestion_date", F.current_date())
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# window_spec = Window.partitionBy(
#     "location_id", "parameter", "datetime_utc"
# ).orderBy(F.col("ingestion_date").desc())

# dedup_df = (
#     silver_df
#     .withColumn("rn", F.row_number().over(window_spec))
#     .filter(F.col("rn") == 1)
#     .drop("rn")
# )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


(
    silver_df
    .write
    .format("delta")
    .mode("append")
    .partitionBy("date", "parameter")
    .saveAsTable("silver.openaq")
)

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
