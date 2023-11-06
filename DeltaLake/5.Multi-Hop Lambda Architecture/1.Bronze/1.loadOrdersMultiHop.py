# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM parquet.`/mnt/adobeadls/dwanalytics/orders/orders-raw/*.parquet`

# COMMAND ----------

# MAGIC %md
# MAGIC 1. READ THE ORDERS RAW DATA FROM BELOW ADLS LOCATION.
# MAGIC 2. FILES WILL BE PLACED AT THE BELOW ADLS AT REGULAR INTERVAL. 
# MAGIC 3. AUTOLOADER WILL PICK UP THOSE FILES WHENEVER NEW FILES ARRIVE

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

# MAGIC %sql
# MAGIC SELECT count(*) AS cnt FROM orders_tmp;

# COMMAND ----------

spark.table("orders_tmp") \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/adobeadls/dwanalytics/orders/checkpoint/orders_bronze") \
    .outputMode("append") \
    .table("bronze.orders")

# COMMAND ----------

# MAGIC %md
# MAGIC 1. READ THE CUSTOMERS LOOKUP FROM BELOW ADLS LOCATION.
# MAGIC 2. LOAD SILVER TABLE DATA INTO TEMP VIEW AS READSTREAM.

# COMMAND ----------

spark.read \
    .format("json") \
    .load("/mnt/adobeadls/dwanalytics/customers/customers-json/") \
    .createOrReplaceTempView("customers_lookup")

# COMMAND ----------

spark.readStream.table("bronze.orders") \
    .createOrReplaceTempView("orders_bronze_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp
# MAGIC AS
# MAGIC (
# MAGIC   SELECT
# MAGIC   order_id
# MAGIC   ,quantity
# MAGIC   ,o.customer_id
# MAGIC   ,c.profile:first_name
# MAGIC   ,c.profile:last_name
# MAGIC   ,cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) AS order_timestamp
# MAGIC   ,books
# MAGIC   FROM orders_bronze_temp o JOIN customers_lookup c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC   WHERE quantity > 0)

# COMMAND ----------

spark.table("orders_enriched_tmp") \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation","/mnt/adobeadls/dwanalytics/orders/checkpoint/orders_silver") \
    .outputMode("append") \
    .table("silver.orders")

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE bronze.orders;
