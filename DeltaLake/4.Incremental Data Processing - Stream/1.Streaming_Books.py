# Databricks notebook source
# MAGIC %md
# MAGIC #### LOAD THE READY TO PROCESS DATA INTO STREAMING.BOOKS TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`/mnt/adobeadls/dwanalytics/books/processed/export_*.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW books_streaming_temp_processed_vw
# MAGIC (
# MAGIC   book_id STRING,
# MAGIC   title STRING,
# MAGIC   author STRING,
# MAGIC   category STRING,
# MAGIC   price DOUBLE
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS
# MAGIC (
# MAGIC   path = "/mnt/adobeadls/dwanalytics/books/processed/export_*.csv",
# MAGIC   header = "true",
# MAGIC   sep = ";"
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM books_streaming_temp_processed_vw ORDER BY 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS streaming.books;
# MAGIC CREATE TABLE IF NOT EXISTS streaming.books
# MAGIC AS
# MAGIC SELECT * FROM books_streaming_temp_processed_vw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM streaming.books;

# COMMAND ----------

# MAGIC %md
# MAGIC #### LOADING NEW ARRIVAL DATA

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS streaming.books_staging;
# MAGIC CREATE TABLE streaming.books_staging
# MAGIC (
# MAGIC     book_id STRING,
# MAGIC     title STRING,
# MAGIC     author STRING,
# MAGIC     category STRING,
# MAGIC     price DOUBLE
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS
# MAGIC (
# MAGIC   path = "/mnt/adobeadls/dwanalytics/books/books-csv-new/*.csv",
# MAGIC   header = "true",
# MAGIC   sep = ";"
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM streaming.books_staging ORDER BY 1;

# COMMAND ----------

spark.readStream \
    .table("streaming.books_staging") \
    .createOrReplaceTempView("books_streaming_temp_new_csv_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_streaming_temp_new_csv_vw;

# COMMAND ----------

spark.table("books_streaming_temp_new_csv_vw") \
    .writeStream \
    .trigger(processingTime = '4 seconds') \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/adobeadls/dwanalytics/books/checkpoint/books_aggregation") \
    .table("streaming.books")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM streaming.books ORDER BY 1 ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT
# MAGIC SUM(price) as total_cost,
# MAGIC COUNT(category) as cnt_genre,
# MAGIC category
# MAGIC FROM streaming.books
# MAGIC GROUP BY category;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED streaming.books

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dw_analytics.books_aggregation;
