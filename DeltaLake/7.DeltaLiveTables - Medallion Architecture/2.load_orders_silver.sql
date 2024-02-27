-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### LOAD ORDERS DATA FROM MULTIPLEX BRONZE TABLE INTO KAFKA.ORDERS_SILVER TABLE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC ordersjson_schema = "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"
-- MAGIC
-- MAGIC qry = (spark.readStream.table("kafka.bronze")
-- MAGIC        .filter("topic = 'orders'")
-- MAGIC        .select(F.from_json(F.col("value").cast("string"), ordersjson_schema).alias("v"))
-- MAGIC        .select("V.*")
-- MAGIC        .writeStream
-- MAGIC        .option("checkpointLocation", "/mnt/adobeadls/dwanalytics/orders/kafka/orders/silver")
-- MAGIC        .trigger(processingTime='5 seconds')
-- MAGIC        .table("kafka.orders_silver")
-- MAGIC     )
