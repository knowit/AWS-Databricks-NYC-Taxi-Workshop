# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables example
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
# MAGIC * Schema: Don't select anything
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
# MAGIC In the DLT run view, press the Curated data set. Look on the Data Quality tab, and observe the expectations fail rate, which should be about 52%.
# MAGIC
# MAGIC This is becuase data from June 2016 did not have `pickup_borough` while July has it.
# MAGIC
# MAGIC The second step `trips_by_month_and_borough()` will fail hard because it has the expectation
# MAGIC `expect_or_fail()`, while `curated()` uses the weaker `expect()`, which allows failing records.
# MAGIC A third alternative is to use `expect_or_drop()`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise Convert pickup_borough Null values to `Unknown`
# MAGIC Make sure expectation is not failing.
# MAGIC Run job again to ensure there are no failing expectations.
# MAGIC
# MAGIC Tip: use `fillna()` function with pickup_borough as subset arg.
# MAGIC
# MAGIC ``````python
# MAGIC .fillna(value="Unknown",subset=["pickup_borough"])
# MAGIC ``````

# COMMAND ----------

@dlt.table(table_properties={"quality": "silver"})
@dlt.expect("pickup_borough_not_null", "pickup_borough IS NOT NULL")
def curated():
  return (
    spark.table("training.taxinyc_trips.yellow_taxi_trips_curated")
    .where(
        # limit dataset to get faster processing
        "trip_year = 2016 and trip_month in ('06', '07')"
    )
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

# COMMAND ----------


