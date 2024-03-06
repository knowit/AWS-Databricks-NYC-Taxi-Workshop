# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # FRAM data
# MAGIC
# MAGIC Only used for the transport sector in Norway.
# MAGIC
# MAGIC Load data from csv and save in `training.fram.trips`.
# MAGIC
# MAGIC Beskrivelsen av datasettet fra Christine:
# MAGIC Datasettet beskriver alle avganger, per stasjon fra 1.1.24 frem til i dag. Dette inkluderer hvor mange som går av og på hver stasjon, samt antall om bord mellom stasjonene. Uttrekket inneholder også kapasitet slik at man kan beregne hvor fullt toget er.
# MAGIC Og følgende kolonnebeskrivelser:
# MAGIC
# MAGIC * CapacityNormal - Normal kapasitet på toget 
# MAGIC * CapacityTheoretical - Teoretisk kapasitet på toget
# MAGIC * BoardingsOutputAdjusted - antall påstigende
# MAGIC * AlightingsOutputAdjusted - antall avstigende
# MAGIC * PaxOutputAdjusted – antall om bord på toget

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG training;
# MAGIC USE fram;
# MAGIC DROP TABLE IF EXISTS trips;
# MAGIC CREATE TABLE IF NOT EXISTS trips;

# COMMAND ----------

# MAGIC %sh head -2 /Volumes/training/data/fram/Uttrekk\ TDS.csv

# COMMAND ----------

import os
from pyspark.sql.types import (
  StructType,
  StructField, 
  StringType, 
  IntegerType, 
  LongType, 
  FloatType, 
  DoubleType, 
  TimestampType, 
  DateType
)
from pyspark.sql import functions as F

# COMMAND ----------

FRAM_SCHEMA = StructType([
    StructField("Date", DateType(), True),
    StructField("DatedServiceJourneyId", StringType(), True),
    StructField("PlannedDepartureTime", TimestampType(), True),
    StructField("ActualDepartureTime", TimestampType(), True),
    StructField("LineName", StringType(), True),
    StructField("TrainNumber", StringType(), True),
    StructField("OriginJdirCode", StringType(), True),
    StructField("DestinationJdirCode", StringType(), True),
    StructField("StopJdirCode", StringType(), True),
    StructField("StopNSRID", StringType(), True),
    StructField("NextStopNSRID", StringType(), True),
    StructField("StopLocationLatitude", StringType(), True),
    StructField("StopLocationLongitude", StringType(), True),
    StructField("StopSequence", IntegerType(), True),
    StructField("WorkingDay", IntegerType(), True),
    StructField("CapacityNormal", IntegerType(), True),
    StructField("CapacityTheoretical", IntegerType(), True),
    StructField("BoardingsOutputAdjusted", StringType(), True),
    StructField("AlightingsOutputAdjusted", StringType(), True),
    StructField("PaxOutputAdjusted", StringType(), True),
])

# COMMAND ----------

src_file = "/Volumes/training/data/fram/Uttrekk TDS.csv"
fram_df = (sqlContext.read.format("csv")
                    .option("header", True)
                    .schema(FRAM_SCHEMA)
                    .option("delimiter",";")
                    .load(src_file).cache())


# COMMAND ----------

fram_df.display()

# COMMAND ----------

def to_float(name):
    return F.regexp_replace(name, ",", ".").cast("float").alias(name)


fram_df_w_floats = fram_df.select(
    "Date",
    "DatedServiceJourneyId",
    "PlannedDepartureTime",
    "ActualDepartureTime",
    "LineName",
    "TrainNumber",
    "OriginJdirCode",
    "DestinationJdirCode",
    "StopJdirCode",
    "StopNSRID",
    "NextStopNSRID",
    "StopLocationLatitude",
    to_float("StopLocationLongitude"),
    "StopSequence",
    "WorkingDay",
    "CapacityNormal",
    "CapacityTheoretical",
    to_float("BoardingsOutputAdjusted"),
    to_float("AlightingsOutputAdjusted"),
    to_float("PaxOutputAdjusted"),
)

fram_df_w_floats.display()

# COMMAND ----------

(fram_df_w_floats
.write
.mode("overwrite").format("delta")
.option("overwriteSchema", "true")
.saveAsTable("training.fram.trips")
)

# COMMAND ----------


