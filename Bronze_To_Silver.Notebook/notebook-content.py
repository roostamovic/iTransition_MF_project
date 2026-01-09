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
yellow = pd.read_csv("/lakehouse/default/Files/Bronze/NYCTaxiData/nyc_yellow.csv")
green = pd.read_csv("/lakehouse/default/Files/Bronze/NYCTaxiData/nyc_green.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

yellow_df = spark.createDataFrame(yellow)
green_df = spark.createDataFrame(green)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

green_df.columns

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

taxi_silver.toPandas()['pickup_datetime'].min()

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
display(fx_raw)

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

display(taxi_daily)

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(taxi_eur)

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(macro_vs_taxi)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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
