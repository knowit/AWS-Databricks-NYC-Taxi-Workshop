# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Table example
# MAGIC
# MAGIC Delta Live Tables is managed compute DBT-like ETL framework.
# MAGIC
# MAGIC What you will learn:
# MAGIC
# MAGIC 1. How to deploy a DLT job
# MAGIC 2. How to fix data to avoid expectations from failing
# MAGIC 3. Observe how DLT shows lineage
# MAGIC
# MAGIC **DO NOT RUN HERE!**
# MAGIC
# MAGIC **Very important!** This notebook cannot be run manually. It can only run from Workflows.
# MAGIC If you try to run here, you will get an error when trying to import dlt module.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy this notebook as a DLT pipeline
# MAGIC
# MAGIC How to deploy:
# MAGIC
# MAGIC 1. Go to Workflows Menu in the left bar, and the Delta Live Tables tab.
# MAGIC 2. Press `Create Pipeline`
# MAGIC
# MAGIC * Name: `donjohnson_trips_dq_dlt`, replace `donjohnson` with your name.
# MAGIC * Product edition: `advanced`
# MAGIC * Notebook source: Find this notebook
# MAGIC * Destination: Unity Catalog
# MAGIC * Catalog: `training`
# MAGIC * Policy: `dlt-training-policy`
# MAGIC * Cluster mode: `Enhanced autoscaling`
# MAGIC * Min workers: 1
# MAGIC * Max workers: 5
# MAGIC
# MAGIC Press `Create`
# MAGIC
# MAGIC 3. Run the pipeline by pressing `Start` button
# MAGIC
# MAGIC It can take a few minutes to start the pipeline, since we have not tuned the clusters.
# MAGIC
# MAGIC 4. Observe the output to see the lineage between tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Observe failed records
# MAGIC
# MAGIC In the DLT run view, press the Curated data set and observe the expectations fail rate, 
# MAGIC which should be about 36%.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise Convert pickup_borough Null values to `Unknown`
# MAGIC Make sure expectation is not failing.
# MAGIC Run job again to ensure there are no failing expectations.

# COMMAND ----------

@dlt.table(table_properties={"quality": "silver"})
@dlt.expect_or_fail("pickup_borough_not_null", "pickup_borough IS NOT NULL")
def curated():
  return (
    spark.table("training.taxinyc_trips.yellow_taxi_trips_curated")
    .where(
        # limit dataset to get faster processing
        "trip_year = 2016 and trip_month == '02'"
    ).fillna(value="Unknown",subset=["pickup_borough"])
  )

# COMMAND ----------

@dlt.table(
  comment="Trips by month and borough."
)
@dlt.expect_or_fail("pickup_borough_not_null", "pickup_borough IS NOT NULL")
def trips_by_month_and_borough():
  return (
    dlt.read("curated")
      .groupBy("pickup_borough", "trip_month")
      .count()
  )
