# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC
# MAGIC 1) Create a student-specific database<BR>

# COMMAND ----------

# MAGIC %md
# MAGIC ## DO NOW:
# MAGIC
# MAGIC 1. Select a cluster ("UC Shared Cluster N")
# MAGIC   2. Where N is the group number you have been asigned by the teacher
# MAGIC   3. If you are doing this course async, use `UC Shared Cluster 1`
# MAGIC 2. Press start on the cluster if it is not running
# MAGIC 3. Press the `Run all` button so the notebook starts processing while you study the code

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Create the taxi_db database in Databricks
# MAGIC
# MAGIC Specific for the student

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.1. Create database

# COMMAND ----------

from libs.dbname import dbname
taxi_db = dbname(db="taxi_db")
print("New db name: " + taxi_db)
spark.conf.set("nbvars.taxi_db", taxi_db)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ${nbvars.taxi_db};

# COMMAND ----------

from libs.dbname import dbname
crime_db = dbname(db="crime")
print("New db name: " + crime_db)
spark.conf.set("nbvars.crime_db", crime_db)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ${nbvars.crime_db};

# COMMAND ----------

from libs.dbname import dbname
books_db = dbname(db="books")
print("New db name: " + books_db)
spark.conf.set("nbvars.books_db", books_db)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ${nbvars.books_db};

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.2. Validate

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG training;
# MAGIC SHOW DATABASES;
