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

trips_df.groupBy("OriginJdirCode").count().sort(F.col("count").desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2. Combine with station names to get better report
# MAGIC
# MAGIC We will extract the station names from Entur stop registry. This is public data from developer.entur.no. Loading of this data can be seen in 00-Admin-Prep

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



# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1.  Join with stop name

# COMMAND ----------

# Joining the `trips_df` with `stops_df` on `stop_id` column


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Boarding and alightings at Oslo S
# MAGIC Show a table with alighting and boarding sums pr day at Oslo S (NSR:StopPlace:337). 
# MAGIC
# MAGIC Test different visualizations to disply the results. 
# MAGIC
# MAGIC How is variation depending on workdays?

# COMMAND ----------




# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Exercises
# MAGIC
# MAGIC We recomment you do this AFTER you are done with the other tasks:
# MAGIC 03-DeployJob
# MAGIC 04-DLTDataQuality
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Between which station pairs are the capacity above > 90%?
# MAGIC
# MAGIC When considering "CapacityNormal"
# MAGIC
# MAGIC

# COMMAND ----------


