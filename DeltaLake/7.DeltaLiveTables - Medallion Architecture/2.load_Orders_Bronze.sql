-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### VALIDATE JSON FILES FROM KAFKA SOURCE

-- COMMAND ----------

SELECT * FROM JSON.`/mnt/adobeadls/dwanalytics/orders/kafka/kafka-streaming`;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC def process_bronze():
-- MAGIC     schema = "key BINARY, offset BINARY, partition LONG, timestamp LONG, topic STRING, value BINARY"
-- MAGIC
-- MAGIC     qry = (spark.readStream
-- MAGIC            .format("cloudFiles")
-- MAGIC            .option("cloudFiles.format", "json")
-- MAGIC            .schema(schema)
-- MAGIC            .load("/mnt/adobeadls/dwanalytics/orders/kafka/kafka-streaming")
-- MAGIC            .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp"))
-- MAGIC            .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
-- MAGIC            .writeStream
-- MAGIC            .option("checkpointLocation", "/mnt/adobeadls/dwanalytics/orders/kafka/bronze")
-- MAGIC            .option("mergeSchema", True)
-- MAGIC            .partitionBy("topic", "year_month")
-- MAGIC            .trigger(availableNow=True)
-- MAGIC            .table("kafka.bronze")
-- MAGIC            )
-- MAGIC     qry.awaitTermination()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC process_bronze()

-- COMMAND ----------

SELECT * FROM kafka.bronze;

-- COMMAND ----------


