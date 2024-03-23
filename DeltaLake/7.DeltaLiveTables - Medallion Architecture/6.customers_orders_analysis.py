# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS kafka.customers_orders
# MAGIC (
# MAGIC order_id INT,
# MAGIC order_timestamp TIMESTAMP,
# MAGIC customer_id STRING,
# MAGIC quantity BIGINT,
# MAGIC total BIGINT,
# MAGIC books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>,
# MAGIC email STRING,
# MAGIC first_name STRING,
# MAGIC last_name STRING,
# MAGIC gender STRING,
# MAGIC street STRING,
# MAGIC city STRING,
# MAGIC country STRING,
# MAGIC row_time TIMESTAMP,
# MAGIC processed_timestamp TIMESTAMP
# MAGIC )
# MAGIC TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, 
# MAGIC                delta.autoOptimize.autoCompact = true);

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def custumers_orders_upsert(microBatchDF, batchId):
    window = Window.partitionBy("order_id", "customer_id").orderBy(F.col("_commit_timestamp").desc())

    (microBatchDF.filter(F.col("_change_type").isin(["insert", "update_postimage"]))
                .withColumn("rank", F.rank().over(window))
                .filter("rank = 1")
                .drop("rank", "_change_type", "_commit_version")
                .withColumnRenamed("_commit_timestamp", "processed_timestamp")
                .createOrReplaceTempView("ranked_updates")
)
    query = """
            MERGE INTO kafka.customers_orders cust
            USING ranked_updates upd
            ON cust.order_id = upd.order_id AND cust.customer_id = upd.customer_id
            WHEN MATCHED AND cust.processed_timestamp < upd.processed_timestamp THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
    """
    microBatchDF.sparkSession.sql(query)

# COMMAND ----------

def process_customers_orders():
    orders_df = spark.readStream.table("kafka.orders_silver")

    cdf_customers_df = (spark.readStream
                             .option("readChangeData", True)
                             .option("startingVersion", 2)
                             .table("kafka.customers_silver")
                        )
    
    query = (orders_df.join(cdf_customers_df, ["customer_id"], "inner")
                     .writeStream
                     .foreachBatch(custumers_orders_upsert)
                     .option("checkpointLocation", "/mnt/adobeadls/dwanalytics/orders/kafka/checkpoint/customers_orders")
                     .trigger(availableNow = True)
                     .start())
    query.awaitTermination()

# COMMAND ----------

process_customers_orders()

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
