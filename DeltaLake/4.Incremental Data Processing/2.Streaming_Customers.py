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

# DO NOT DELETE THIS CELL
# %sql
# SELECT * FROM customers_streaming_temp_vw;

# COMMAND ----------

# DO NOT DELETE THIS CELL
# %sql
# SELECT 
# count(*) as population
# ,c.profile:gender as gender
# ,c.profile:address:country as country
# FROM customers_streaming_temp_vw c
# WHERE customer_id IS NOT NULL
# GROUP BY gender, country
# HAVING population > 50
# ORDER BY population DESC, country ASC;

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

# DO NOT DELETE THIS CELL
# %sql
# SELECT * FROM customers_aggregation_tmp_vw;

# COMMAND ----------

# dbutils.fs.rm("/mnt/adobeadls/dwanalytics/customers/checkpoint/customers_aggregation", True);

spark.table("customers_aggregation_tmp_vw") \
    .writeStream \
    .trigger(availableNow = True) \
    .outputMode('complete') \
    .option('skipChangeCommits', 'true') \
    .option("checkpointLocation", "/mnt/adobeadls/dwanalytics/customers/checkpoint/customers_aggregation") \
    .table("dw_analytics.customers_aggregation") \
    .awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dw_analytics.customers_aggregation ORDER BY population DESC;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dw_analytics.customers_aggregation;
