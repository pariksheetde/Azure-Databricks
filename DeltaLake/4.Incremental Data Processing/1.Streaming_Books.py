# Databricks notebook source
# MAGIC %md
# MAGIC #### QUERY BOOKS Table From DW_ANALYTICS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dw_analytics.books;

# COMMAND ----------

spark.readStream \
    .table("dw_analytics.books") \
    .createOrReplaceTempView("books_streaming_temp_vw")

# COMMAND ----------

# DO NOT DELETE THIS CELL

# %sql
# SELECT * FROM books_streaming_temp_vw;

# COMMAND ----------

# DO NOT DELETE THIS CELL

# %sql
# SELECT
# sum(price) as total_cost,
# count(category) as cnt_genre,
# category
# FROM books_streaming_temp_vw
# GROUP BY category;

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

# DO NOT DELETE THIS CELL

# %sql
# SELECT * FROM books_aggregation_tmp_vw;

# COMMAND ----------

dbutils.fs.rm("/mnt/adobeadls/dwanalytics/books/checkpoint/books_aggregation", True);

spark.table("books_aggregation_tmp_vw") \
    .writeStream \
    .trigger(availableNow = True) \
    .outputMode('complete') \
    .option('skipChangeCommits', 'true') \
    .option("checkpointLocation", "/mnt/adobeadls/dwanalytics/books/checkpoint/books_aggregation") \
    .table("dw_analytics.books_aggregation") \
    .awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dw_analytics.books_aggregation;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dw_analytics.books_aggregation;
