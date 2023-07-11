# Databricks notebook source
# MAGIC %md
# MAGIC #### QUERY books table from Database

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dw_analytics.books;

# COMMAND ----------

spark.readStream \
    .table("dw_analytics.books") \
    .createOrReplaceTempView("books_streaming_temp_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_streaming_temp_vw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC sum(price) as total_cost,
# MAGIC count(category) as cnt_genre,
# MAGIC category
# MAGIC FROM books_streaming_temp_vw
# MAGIC GROUP BY category;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW books_aggregation_tmp_vw
# MAGIC AS
# MAGIC (
# MAGIC SELECT
# MAGIC sum(price) as total_cost,
# MAGIC count(category) as cnt_genre,
# MAGIC category
# MAGIC FROM books_streaming_temp_vw
# MAGIC GROUP BY category
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_aggregation_tmp_vw;

# COMMAND ----------

spark.table("books_aggregation_tmp_vw") \
    .writeStream \
    .trigger(processingTime = '1 seconds') \
    .outputMode('complete') \
    .option("checkpointLocation", "/mnt/adobeadls/dwanalytics/books/checkpoint/books_aggregation") \
    .table("dw_analytics.books_aggregation")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dw_analytics.books_aggregation;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dw_analytics.books_aggregation;
