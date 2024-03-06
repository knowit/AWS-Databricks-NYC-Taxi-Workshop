# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy a job
# MAGIC
# MAGIC You will learn
# MAGIC
# MAGIC 1. Transform a dataset
# MAGIC 2. Write to a new dataset
# MAGIC 3. Deploy the job as a git reference
# MAGIC 4. Run the job

# COMMAND ----------

# MAGIC %md
# MAGIC # Transform nyc trips dataset

# COMMAND ----------

# Import libs to define user and env specific table name
from libs.tblname import tblname
from libs.dbname import dbname

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read trips dataset

# COMMAND ----------

trips_df = spark.sql("select * from training.taxinyc_trips.yellow_taxi_trips_curated")
trips_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Group trips by month and borough

# COMMAND ----------

month_cnt_df = (trips_df
    .groupBy("pickup_borough", "trip_month")
    .count()
)
month_cnt_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write report to db

# COMMAND ----------

reports_db = dbname(db="nyctaxi_reports")
print(f"reports_db: {reports_db}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {reports_db}")

# COMMAND ----------

borough_month_tbl = tblname(db="nyctaxi_reports", tbl="trips_by_month_borough")
print(f"borough_month_tbl: {borough_month_tbl}")
month_cnt_df.write.mode("overwrite").format("delta").saveAsTable(borough_month_tbl)

# COMMAND ----------

# MAGIC %md
# MAGIC # Deploy the job under Workflows menu
# MAGIC
# MAGIC ## How to deploy this notebook a a job:
# MAGIC
# MAGIC 1. Go to Workflows menu
# MAGIC 2. Press create job
# MAGIC
# MAGIC * Task name: `donjohnson_report_trips_by_month_borough`, replace donjohnson with your name
# MAGIC * Type: `Notebook`
# MAGIC * Source: `Workspace`
# MAGIC
# MAGIC Source could also have been a git repo, but for simplicity we now refer this Notebook
# MAGIC
# MAGIC * Path: Find this notebook under Repo/username/....
# MAGIC * Cluster: `UC Training Cluster`
# MAGIC
# MAGIC Press Create
# MAGIC
# MAGIC 3. Run job
# MAGIC
# MAGIC Press `Run Now`
# MAGIC
# MAGIC 4. Inspect the run under `Runs` tab

# COMMAND ----------


