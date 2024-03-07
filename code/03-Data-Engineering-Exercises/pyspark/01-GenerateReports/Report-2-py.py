# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC We will run various reports and visualize

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Examples

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show a few records

# COMMAND ----------

trips_df = spark.sql("select * from training.taxinyc_trips.yellow_taxi_trips_curated")
trips_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.  Trip count by pickup_borough

# COMMAND ----------

trips_df.groupBy("pickup_borough").count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2. Revenue sum by pickup_borough
# MAGIC
# MAGIC `total_amount` is the revenue for a trip.
# MAGIC
# MAGIC Sort by pickup_borough ascending.
# MAGIC
# MAGIC To sort / order by, use `.sort(F.col("col_name").asc())`.

# COMMAND ----------

trips_df.groupBy("pickup_borough").agg(
    F.sum("total_amount")
).sort(F.col("pickup_borough").asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Excercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Count trips by passenger_count
# MAGIC Order ascending by `passenger_count`

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.  Total revenue including tips
# MAGIC use `total_amount` to get revenue incl tips.
# MAGIC Round off with `F.round()`.
# MAGIC
# MAGIC Rename col to "revenue" using `.alias("revenue")` after round() function.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Total revenue by pickup_borough, payment_type
# MAGIC
# MAGIC `pickup_borough`, `payment_type`

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.  Revenue share by pickup_borough, exclude rows where pickup_borough is null
# MAGIC
# MAGIC Count pickup_borough, discard trips which pickup_borough is null.
# MAGIC
# MAGIC Use `.where(F.col("pickup_borough").isNotNull())` to filter out null values.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.  Trip count trend by month, pickup_borough, for 2016
# MAGIC
# MAGIC Use cols `trip_year`, `trip_month`.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.  Average trip distance by pickup_borough
# MAGIC
# MAGIC Use functions `avg()` for average, and `round()` to round off value.
# MAGIC
# MAGIC Use trip_distance.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.  Average trip amount by pickup_borough
# MAGIC
# MAGIC Use `total_amount`.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Bonus Exercises
# MAGIC
# MAGIC Do this AFTER you are done with the other tasks in 01-GenerateReports:
# MAGIC
# MAGIC * 02-FramData
# MAGIC * 03-DeployJob
# MAGIC * 04-DLTDataQuality

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.  Count trips with no tip, by pickup_borough
# MAGIC `tip_amount` is the tip column

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Count trips with no charge, by pickup_borough
# MAGIC
# MAGIC `Payment_type`
# MAGIC
# MAGIC A numeric code signifying how the passenger paid for the trip. Type values:
# MAGIC
# MAGIC 1. `Credit card`
# MAGIC 2. `Cash`
# MAGIC 3. `No charge`
# MAGIC 4. `Dispute`
# MAGIC 5. `Unknown`
# MAGIC 6. `Voided trip`

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Count trips by payment type

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Count trips by pickup hour in 2016
# MAGIC
# MAGIC Count by pickup_hour for year 2016

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. Visualize the pickup hour distribution per pickup_borough

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. Top 3 pickup-dropoff zones for 2016
# MAGIC
# MAGIC Find the top three combinations of pickup_zone and dropoff_zone.
# MAGIC
# MAGIC Filter out all pickup_zones and dropoff_zones with value NV.
# MAGIC Use & between conditions.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 13. Find the last trip of each month
# MAGIC
# MAGIC List the last trip started before midnight the last day of each month

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 14. Find the second last trip of each month
# MAGIC
# MAGIC List the last trip started before midnight the last day of each month.
# MAGIC
# MAGIC Tip: Use `lag()`-function to solve it.
# MAGIC
# MAGIC https://www.educba.com/pyspark-lag/

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 15. What percentage of trips for each month of 2016 crossed borough lines?
# MAGIC
# MAGIC E.g. had diffent pickup and dropoff borough.

# COMMAND ----------


