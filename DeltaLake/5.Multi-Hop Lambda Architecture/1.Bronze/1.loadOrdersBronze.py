# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM parquet.`/mnt/adobeadls/dwanalytics/orders/orders-raw/*.parquet`

# COMMAND ----------

files = dbutils.fs.ls("/mnt/adobeadls/dwanalytics/orders/orders-raw/")
display(files)

# COMMAND ----------

spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "/mnt/adobeadls/dwanalytics/orders/checkpoint/orders-raw") \
    .load("/mnt/adobeadls/dwanalytics/orders/orders-raw/") \
    .createOrReplaceTempView("orders_temp_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW orders_tmp
# MAGIC AS
# MAGIC   (
# MAGIC SELECT *, input_file_name() as file_name, current_timestamp() as load_ts FROM orders_temp_vw
# MAGIC   );

# COMMAND ----------

spark.table("orders_tmp") \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/adobeadls/dwanalytics/orders/checkpoint/orders_bronze") \
    .outputMode("append") \
    .table("bronze.orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS cnt FROM bronze.orders

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
