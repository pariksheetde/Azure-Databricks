# Databricks notebook source
# MAGIC %md
# MAGIC #### LOAD THE READY TO PROCESS DATA INTO STREAMING.CUSTOMERS TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS streaming.customers;
# MAGIC CREATE TABLE streaming.customers
# MAGIC AS
# MAGIC SELECT * FROM json.`/mnt/adobeadls/dwanalytics/customers/processed/*.json`;
# MAGIC
# MAGIC SELECT * FROM streaming.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS CNT FROM streaming.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM streaming.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC #### LOAD NEW ARRIVAL CUSTOMERS DATA

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`/mnt/adobeadls/dwanalytics/customers/landing_zone/*.json`;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customers_streaming_temp_vw;

# COMMAND ----------

spark.readStream \
    .format('cloudFiles') \
    .option('cloudFiles.format', 'json') \
    .option("mergeSchema", "true") \
    .option('cloudFiles.schemaLocation', '/mnt/adobeadls/dwanalytics/customers/checkpoint/customers_tmp') \
    .load('/mnt/adobeadls/dwanalytics/customers/landing_zone/*.json') \
    .writeStream \
    .option('checkpointLocation', '/mnt/adobeadls/dwanalytics/customers/checkpoint/customers_tmp') \
    .table('customers_streaming_temp_vw')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_streaming_temp_vw;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dw_analytics.customers_aggregation;
