# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC We will do some exercises on the FRAM-dataset.
# MAGIC
# MAGIC Only used for the transport sector in Norway.
# MAGIC
# MAGIC Data can be found in `training.fram.trips`.
# MAGIC
# MAGIC Datasettet beskriver alle avganger, per stasjon fra 1.1.24 frem til i dag. Dette inkluderer hvor mange som går av og på hver stasjon, samt antall om bord mellom stasjonene. Uttrekket inneholder også kapasitet slik at man kan beregne hvor fullt toget er.
# MAGIC Og følgende kolonnebeskrivelser:
# MAGIC
# MAGIC * CapacityNormal - Normal kapasitet på toget 
# MAGIC * CapacityTheoretical - Teoretisk kapasitet på toget
# MAGIC * BoardingsOutputAdjusted - antall påstigende
# MAGIC * AlightingsOutputAdjusted - antall avstigende
# MAGIC * PaxOutputAdjusted – antall om bord på toget

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Examples

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show a few records

# COMMAND ----------

trips_df = spark.sql("select * from training.fram.trips")
trips_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.  Trip count by OriginJdirCode

# COMMAND ----------

trips_df.groupBy("OriginJdirCode").count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2. Combine with station names to get better report
# MAGIC
# MAGIC We will extract the station names from a remote zip file.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Stops data from Entur

# COMMAND ----------

stops_df = spark.sql("select * from training.fram.entur_gtfs_stops")
stops_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Excercises

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1. Create a stop code data set
# MAGIC
# MAGIC Get a distinct list of StopJdirCode and StopNSRID combinations.
# MAGIC
# MAGIC Remove duplicates

# COMMAND ----------

trips_df.select("StopJdirCode", "StopNSRID").distinct().display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Trip counts with station names
# MAGIC Show a table with trip counts per combination of start and stop destination.
# MAGIC Get station names from the stops dataset, combining them the stop code dataset.

# COMMAND ----------

trips_df.select("origin")

# COMMAND ----------


