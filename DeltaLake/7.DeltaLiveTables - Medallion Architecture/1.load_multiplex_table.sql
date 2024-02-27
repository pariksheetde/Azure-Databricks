-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### QUERY JSON FILES FROM KAFKA SOURCE

-- COMMAND ----------

SELECT * FROM JSON.`/mnt/adobeadls/dwanalytics/orders/kafka/kafka-streaming`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### LOAD KAFKA DATA AS MULTIPLEX INTO SINGLE BRONZE TABLE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC schema = "key BINARY, offset BINARY, partition LONG, timestamp LONG, topic STRING, value BINARY"
-- MAGIC (spark.readStream
-- MAGIC            .format("cloudFiles")
-- MAGIC            .option("cloudFiles.format", "json")
-- MAGIC            .schema(schema)
-- MAGIC            .load("/mnt/adobeadls/dwanalytics/orders/kafka/kafka-streaming")
-- MAGIC            .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp"))
-- MAGIC            .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
-- MAGIC            .writeStream
-- MAGIC            .option("checkpointLocation", "/mnt/adobeadls/dwanalytics/orders/kafka/multiplex/bronze")
-- MAGIC            .option("mergeSchema", True)
-- MAGIC            .partitionBy("topic", "year_month")
-- MAGIC        #     .trigger(availableNow=True)
-- MAGIC            .trigger(processingTime='5 seconds')
-- MAGIC            .table("kafka.bronze")
-- MAGIC            )
