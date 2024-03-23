# Databricks notebook source
# MAGIC %md
# MAGIC #### QUERY JSON FILES FROM KAFKA SOURCE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM JSON.`/mnt/adobeadls/dwanalytics/orders/kafka/kafka-streaming`;

# COMMAND ----------

# MAGIC %md
# MAGIC #### LOAD KAFKA DATA AS MULTIPLEX INTO SINGLE BRONZE TABLE

# COMMAND ----------

from pyspark.sql import functions as F
schema = "key BINARY, offset BINARY, partition LONG, timestamp LONG, topic STRING, value BINARY"
(spark.readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "json")
           .schema(schema)
           .load("/mnt/adobeadls/dwanalytics/orders/kafka/kafka-streaming")
           .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp"))
           .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
           .writeStream
           .option("checkpointLocation", "/mnt/adobeadls/dwanalytics/orders/kafka/checkpoint/multiplex/bronze")
           .option("mergeSchema", True)
           .partitionBy("topic", "year_month")
       #     .trigger(availableNow=True)
           .trigger(processingTime='5 seconds')
           .table("kafka.bronze")
           )

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED kafka.bronze
