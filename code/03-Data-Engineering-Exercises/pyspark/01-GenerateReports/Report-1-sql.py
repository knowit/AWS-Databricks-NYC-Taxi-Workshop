# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC We will run various reports and visualize

# COMMAND ----------

# MAGIC %md
# MAGIC # Examples

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show a few records

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from
# MAGIC   training.taxinyc_trips.yellow_taxi_trips_curated;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.  Trip count by pickup_borough

# COMMAND ----------

# MAGIC %sql
# MAGIC -- use training;
# MAGIC select
# MAGIC   pickup_borough,
# MAGIC   count(*) as trip_count
# MAGIC from
# MAGIC   training.taxinyc_trips.yellow_taxi_trips_curated
# MAGIC   -- taxinyc_trips.taxi_trips_mat_view
# MAGIC group by pickup_borough

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Excercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Count trips by passenger_count
# MAGIC Order ascending by passenger_count

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.  Total revenue including tips
# MAGIC use `total_amount` to get revenue incl tips

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.  Revenue share by pickup_borough

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.  Revenue share by pickup_borough, exclude rows where pickup_borough is null
# MAGIC
# MAGIC Count pickup_borough, discard trips which pick_borough is null

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.  Trip count trend by month, by pickup_borough, for 2016

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.  Average trip distance by pickup_borough
# MAGIC
# MAGIC Use functions `avg()` for average, and `round()` to round off value.
# MAGIC
# MAGIC Use `trip_distance`.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.  Average trip amount by pickup_borough
# MAGIC
# MAGIC Use `total_amount`.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Bonus exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Count trips with no tip, by by pickup_borough
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



# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. Top 3 pickup-dropoff zones for 2016

# COMMAND ----------


