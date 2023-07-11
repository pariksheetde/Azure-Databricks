# Databricks notebook source
# MAGIC %md
# MAGIC #### QUERY movies table from Database

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dw_analytics.customers;

# COMMAND ----------

spark.readStream \
    .table("dw_analytics.customers") \
    .createOrReplaceTempView("customers_streaming_temp_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_streaming_temp_vw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC count(*) as population
# MAGIC ,c.profile:gender as gender
# MAGIC ,c.profile:address:country as country
# MAGIC FROM customers_streaming_temp_vw c
# MAGIC WHERE customer_id IS NOT NULL
# MAGIC GROUP BY gender, country
# MAGIC HAVING population > 50
# MAGIC ORDER BY population DESC, country ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW customers_aggregation_tmp_vw
# MAGIC AS
# MAGIC (
# MAGIC SELECT 
# MAGIC count(*) as population
# MAGIC ,c.profile:gender as gender
# MAGIC ,c.profile:address:country as country
# MAGIC FROM customers_streaming_temp_vw c
# MAGIC WHERE customer_id IS NOT NULL
# MAGIC GROUP BY gender, country
# MAGIC HAVING population > 50
# MAGIC ORDER BY population DESC, country ASC
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_aggregation_tmp_vw;

# COMMAND ----------

spark.table("customers_aggregation_tmp_vw") \
    .writeStream \
    .trigger(processingTime = '1 seconds') \
    .outputMode('complete') \
    .option("checkpointLocation", "/mnt/adobeadls/dwanalytics/customers/checkpoint/customers_aggregation") \
    .table("dw_analytics.customers_aggregation")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dw_analytics.customers_aggregation ORDER BY population DESC;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dw_analytics.customers_aggregation;
