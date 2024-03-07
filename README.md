# AWS Databricks NYC Taxi Workshop

*Adapted from Azure Databricks NYC Taxi Workshop*

This is a workshop featuring Databricks. It covers basics of working with Azure Data Services from Spark on Databricks with Chicago crimes public dataset, followed by an end-to-end data engineering workshop with the NYC Taxi public dataset. This is a community contribution, so we appreciate feedback and contribution.

## Target Audience

- Architects
- Data Engineers
- Data Scientists
- Analysts

## Pre-Requisite Knowledge

- Some familiarity/experience with Python

## Teacher instructions

1. Run through the notebooks under `code/00-Workshop-Admin-Prep` once.
2. Setup UC Shared Cluster 1/2/3 etc, with the students' group having permission to restart them.

## Student instructions

DO NOT TOUCH anything under `code/00-Workshop-Admin-Prep`

Start here: `code/01-Student-Prep/pyspark/01-General/1-CreateDatabaseObjects`

1. Setup
    1. `code/01-Student-Prep/pyspark/01-General/1-CreateDatabaseObjects.py`
    2. Select a cluster (normally `UC Shared Cluster 1`, if many users use UC Shared Cluster [1/2/3/4/5/6]). You have been assigned a cluster number by the teacher.
2. Intro
    1. `code/02-Primer/pyspark/00-storage/4-read-write-primer.py`
    2. `code/02-Primer/pyspark/00-storage/5-delta-primer.py`
3. Exercises (solutions are under Solutions folders)
    1. `code/03-Data-Engineering-Exercises/pyspark/01-GenerateReports/Report-2-py.py` or
`code/03-Data-Engineering-Exercises/pyspark/01-GenerateReports/Report-1-sql.py`
        1. do python (Report-2) unless you really prefer sql
        2. you can skip bonus exercises until you are done with `02-FramData`, `03-DeployJob` and `04-DLTDataQuality`
    3. `code/03-Data-Engineering-Exercises/pyspark/02-FramData/framdata-analyse-2-py`
    3. `code/03-Data-Engineering-Exercises/pyspark/03-DeployJob/01-DeployJob.py`
    4. `code/03-Data-Engineering-Exercises/pyspark/04-DLTDataQuality/01-DataQualityCheckDLT.py`
