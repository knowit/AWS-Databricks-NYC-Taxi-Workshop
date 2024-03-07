# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Delta - primer
# MAGIC
# MAGIC ### What's in this exercise?
# MAGIC In this exercise, we will complete the following in **batch** (streaming covered in the event hub primer):<br>
# MAGIC
# MAGIC 1.  Create a dataset, persist in Delta format to Unity Catalog<br>
# MAGIC 2.  Update one or two random records<br>
# MAGIC 3.  Delete one or two records<br>
# MAGIC 4.  Add a couple new columns to the data and understand considerations for schema evolution<br>
# MAGIC
# MAGIC References:
# MAGIC https://docs.azuredatabricks.net/delta/index.html<br>

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
# MAGIC ##What is Delta Lake?
# MAGIC
# MAGIC
# MAGIC * **Delta lake is the default storage format for Databricks.**
# MAGIC * **If you use only parquet or json-files to store data in your s3 data lake, you must manually handle updates and deletes. Delta Lake solves it.**
# MAGIC
# MAGIC Delta Lake is an open-source storage layer designed to run on top of an existing data lake and improve its reliability, security, and performance. Delta Lakes support ACID transactions, scalable metadata, unified streaming, and batch data processing.

# COMMAND ----------

# Load libs for db and table naming
from libs.dbname import dbname
from libs.tblname import tblname

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.0. Basic create operation

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.0.1. Create some data

# COMMAND ----------

columns = ["book_id", "book_author", "book_name", "book_pub_year"]
vals = [
     ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887),
     ("b01001", "Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892),
     ("b00501", "Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893),
     ("b00300", "Arthur Conan Doyle", "The hounds of Baskerville", 1901)
]
books_df = spark.createDataFrame(vals, columns)
books_df.printSchema
display(books_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.0.2. Persist to Delta format

# COMMAND ----------

books_db = dbname(db="books")
spark.conf.set("nbvars.books_db", books_db)
books_tbl = tblname(db="books", tbl="books")
print("books_tbl:" + repr(books_tbl))
spark.conf.set("nbvars.books_tbl", books_tbl)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.0.3. Create Delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists ${nbvars.books_tbl};

# COMMAND ----------

# Persist dataframe to delta format without coalescing
books_df.write.mode("overwrite").format("delta").saveAsTable(books_tbl)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from ${nbvars.books_tbl};

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from ${nbvars.books_tbl};

# COMMAND ----------

# MAGIC  %md
# MAGIC #### 1.0.4. Performance optimization
# MAGIC  
# MAGIC We will run the "OPTIMIZE" command to compact small files into larger for performance.
# MAGIC Note: The performance improvements are evident at scale

# COMMAND ----------

# 2) Lets read the dataset and check the partition size, it should be the same as number of small files
preDeltaOptimizeDF = spark.sql(f"select * from {books_tbl}")
preDeltaOptimizeDF.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %sql DESCRIBE DETAIL ${nbvars.books_tbl};
# MAGIC --3) Lets run DESCRIBE DETAIL 
# MAGIC --Notice that numFiles = 5

# COMMAND ----------

# MAGIC %sql
# MAGIC --4) Now, lets run optimize
# MAGIC USE ${nbvars.books_db};
# MAGIC OPTIMIZE books;

# COMMAND ----------

# MAGIC %sql DESCRIBE DETAIL ${nbvars.books_tbl};
# MAGIC --5) Notice the number of files now - its 1 file

# COMMAND ----------

#7) Lets read the dataset and check the partition size, it should be the same as number of small files
postDeltaOptimizeDF = spark.sql(f"select * from {books_tbl}")
postDeltaOptimizeDF.rdd.getNumPartitions()
#Its 1, and not 6
#Guess why?

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2.0. Append operation

# COMMAND ----------

# MAGIC %md
# MAGIC 2.1. Create some data to add to the table & persist it

# COMMAND ----------

columns = ["book_id", "book_author", "book_name", "book_pub_year"]
vals = [
     ("b00909", "Arthur Conan Doyle", "A scandal in Bohemia", 1891),
     ("b00023", "Arthur Conan Doyle", "Playing with Fire", 1900)
]
books_df = spark.createDataFrame(vals, columns)
books_df.printSchema
display(books_df)

# COMMAND ----------

books_df.write.mode("append").format("delta").saveAsTable(books_tbl)

# COMMAND ----------

# MAGIC %md
# MAGIC 2.2. Lets query the table without making any changes or running table refresh<br>
# MAGIC We should see the new book entries.

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from ${nbvars.books_tbl};

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from ${nbvars.books_tbl};

# COMMAND ----------

# MAGIC %md
# MAGIC 2.3. Under the hood..

# COMMAND ----------

# MAGIC %sql DESCRIBE DETAIL ${nbvars.books_tbl};

# COMMAND ----------

# MAGIC %md
# MAGIC 2.4. Optimize and Vacuum
# MAGIC Vacuum will remove small left over files (garbage)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE ${nbvars.books_tbl}; -- compact into fewer files
# MAGIC VACUUM ${nbvars.books_tbl}; -- remove small left over garbage files

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 3.0. Update/upsert operation
# MAGIC
# MAGIC Update some rows

# COMMAND ----------

columns = ["book_id", "book_author", "book_name", "book_pub_year"]
vals = [
       ("b00001", "Sir Arthur Conan Doyle", "A study in scarlet", 1887),
       ("b00023", "Sir Arthur Conan Doyle", "A sign of four", 1890),
       ("b01001", "Sir Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892),
       ("b00501", "Sir Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893),
       ("b00300", "Sir Arthur Conan Doyle", "The hounds of Baskerville", 1901),
       ("b09999", "Sir Arthur Conan Doyle", "The return of Sherlock Holmes", 1905),
       ("b00909", "Sir Arthur Conan Doyle", "A scandal in Bohemia", 1891),
       ("b00223", "Sir Arthur Conan Doyle", "Playing with Fire", 1900)
]
booksUpsertDF = spark.createDataFrame(vals, columns)
booksUpsertDF.printSchema
display(booksUpsertDF)

# COMMAND ----------

# 2) Create a temporary view on the upserts
booksUpsertDF.createOrReplaceTempView("books_upserts")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3) Execute upsert
# MAGIC USE ${nbvars.books_db};
# MAGIC
# MAGIC MERGE INTO books
# MAGIC USING books_upserts
# MAGIC ON books.book_id = books_upserts.book_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     books.book_author = books_upserts.book_author,
# MAGIC     books.book_name = books_upserts.book_name,
# MAGIC     books.book_pub_year = books_upserts.book_pub_year
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (book_id, book_author, book_name, book_pub_year) VALUES (books_upserts.book_id, books_upserts.book_author, books_upserts.book_name, books_upserts.book_pub_year);

# COMMAND ----------

# MAGIC %sql
# MAGIC --4) Validate
# MAGIC select * from ${nbvars.books_tbl};

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- 6) What does describe detail say?
# MAGIC DESCRIBE DETAIL ${nbvars.books_tbl};

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 7) Lets optimize 
# MAGIC OPTIMIZE ${nbvars.books_tbl};
# MAGIC VACUUM ${nbvars.books_tbl};

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 4.0. Delete operation

# COMMAND ----------

# MAGIC %sql
# MAGIC --1) Lets isolate records to delete
# MAGIC select * from ${nbvars.books_tbl} where book_pub_year>=1900;

# COMMAND ----------

# MAGIC %sql
# MAGIC --2) Execute delete
# MAGIC USE ${nbvars.books_db};
# MAGIC
# MAGIC DELETE FROM books where book_pub_year >= 1900;

# COMMAND ----------

# MAGIC %sql
# MAGIC --3) Lets validate
# MAGIC select * from ${nbvars.books_tbl} where book_pub_year>=1900;

# COMMAND ----------

# MAGIC %sql
# MAGIC --4) Lets validate further
# MAGIC select * from ${nbvars.books_tbl};

# COMMAND ----------

# MAGIC %sql
# MAGIC --4) Lets validate further
# MAGIC select * from ${nbvars.books_tbl};

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.0. Overwrite
# MAGIC Works only when there are no schema changes

# COMMAND ----------

columns = ["book_id", "book_author", "book_name", "book_pub_year"]
vals = [
     ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887),
     ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887),
     ("b01001", "Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892),
     ("b00501", "Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893),
     ("b00300", "Arthur Conan Doyle", "The hounds of Baskerville", 1901),
     ("b00909", "Arthur Conan Doyle", "A scandal in Bohemia", 1891),
     ("b00023", "Arthur Conan Doyle", "Playing with Fire", 1900)
]
booksOverwriteDF = spark.createDataFrame(vals, columns)
booksOverwriteDF.printSchema
display(booksOverwriteDF)

# COMMAND ----------

# 2) Overwrite the table
booksOverwriteDF.write.mode("overwrite").format("delta").saveAsTable(books_tbl)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3) Query
# MAGIC select * from ${nbvars.books_tbl};

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.0. Automatic schema update

# COMMAND ----------

columns = ["book_id", "book_author", "book_name", "book_pub_year", "book_price"]
vals = [
     ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887, 5.12),
     ("b01001", "Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892, 12.00),
     ("b00501", "Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893, 13.39),
     ("b00300", "Arthur Conan Doyle", "The hounds of Baskerville", 1901, 22.00),
     ("b00909", "Arthur Conan Doyle", "A scandal in Bohemia", 1891, 18.00),
     ("b00023", "Arthur Conan Doyle", "Playing with Fire", 1900, 29.99)
]
booksNewColDF = spark.createDataFrame(vals, columns)
booksNewColDF.printSchema
display(booksNewColDF)

# COMMAND ----------

booksNewColDF.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(books_tbl)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3) Query
# MAGIC select * from ${nbvars.books_tbl};

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ${nbvars.books_db};
# MAGIC
# MAGIC -- Should give zero rows affected
# MAGIC DELETE FROM books where book_price is null;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.0. History

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY ${nbvars.books_tbl};

# COMMAND ----------


