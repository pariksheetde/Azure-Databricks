# Databricks notebook source
# MAGIC %md
# MAGIC #### LOAD READY TO PROCESS DATA INTO STREAMING.ORDERS TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS CNT FROM parquet.`/mnt/adobeadls/dwanalytics/orders/processed/*.parquet`;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS streaming.orders;
# MAGIC CREATE TABLE streaming.orders
# MAGIC AS
# MAGIC SELECT * FROM parquet.`/mnt/adobeadls/dwanalytics/orders/processed/*.parquet`;
# MAGIC
# MAGIC SELECT * FROM streaming.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS CNT FROM streaming.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC #### LOAD NEW ARRIVAL ORDERS DATA

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS streaming.orders_staging;
# MAGIC CREATE TABLE streaming.orders_staging
# MAGIC SELECT * FROM parquet.`/mnt/adobeadls/dwanalytics/orders/landing_zone/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS CNT FROM streaming.orders_staging;

# COMMAND ----------

spark.readStream \
    .table("streaming.orders_staging") \
    .createOrReplaceTempView("orders_streaming_temp_new_parquet_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_streaming_temp_new_parquet_vw;

# COMMAND ----------

spark.table("orders_streaming_temp_new_parquet_vw") \
    .writeStream \
    .trigger(processingTime = '5 seconds') \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/adobeadls/dwanalytics/orders/checkpoint/orders") \
    .table("streaming.orders")

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dw_analytics.directors_movie_aggregation;
