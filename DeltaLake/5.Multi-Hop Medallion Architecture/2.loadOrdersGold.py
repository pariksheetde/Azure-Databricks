# Databricks notebook source
# MAGIC %md
# MAGIC #### READ FROM SILVER.ORDERS TABLE TO PERFORM AGGREGATION

# COMMAND ----------

spark.readStream \
    .table("silver.orders") \
     .createOrReplaceTempView("orders_silver_tmp")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW daily_customers_books_tmp
# MAGIC AS
# MAGIC (
# MAGIC   SELECT customer_id, 
# MAGIC          first_name,
# MAGIC          last_name,
# MAGIC          date_trunc('DD', order_timestamp) as order_date,
# MAGIC          sum(quantity) as books_qty
# MAGIC          FROM orders_silver_tmp
# MAGIC          GROUP BY customer_id, 
# MAGIC          first_name,
# MAGIC          last_name,
# MAGIC          date_trunc('DD', order_timestamp)
# MAGIC )

# COMMAND ----------

spark.table("daily_customers_books_tmp") \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation","/mnt/adobeadls/dwanalytics/orders/checkpoint/orders_gold") \
    .outputMode("complete") \
    .trigger(availableNow=True) \
    .table("gold.daily_customers_books")

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
