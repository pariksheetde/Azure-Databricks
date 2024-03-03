# Databricks notebook source
# MAGIC %md
# MAGIC #### LOAD ORDERS DATA FROM MULTIPLEX BRONZE TABLE INTO KAFKA.ORDERS_SILVER TABLE

# COMMAND ----------

from pyspark.sql import functions as F

ordersjson_schema = "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

qry = (spark.readStream.table("kafka.bronze")
       .filter("topic = 'orders'")
       .select(F.from_json(F.col("value").cast("string"), ordersjson_schema).alias("v"))
       .select("V.*")
       .writeStream
       .option("checkpointLocation", "/mnt/adobeadls/dwanalytics/orders/kafka/checkpoint/orders/silver")
       .trigger(processingTime='5 seconds')
       .table("kafka.orders_silver")
    )

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
