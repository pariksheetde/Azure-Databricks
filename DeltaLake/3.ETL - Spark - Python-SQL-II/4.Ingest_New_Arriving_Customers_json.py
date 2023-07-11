# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM json.`/mnt/adobeadls/dwanalytics/customers/customers-json-new/export_*.json`;

# COMMAND ----------

spark.readStream \
    .format('cloudFiles') \
    .option('cloudFiles.format', 'json') \
    .option('cloudFiles.schemaLocation', '/mnt/adobeadls/dwanalytics/customers/checkpoint/customers_tmp') \
    .load('/mnt/adobeadls/dwanalytics/customers/customers-json-new/*') \
    .writeStream \
    .option('checkpointLocation', '/mnt/adobeadls/dwanalytics/customers/checkpoint/customers_tmp') \
    .table('dw_analytics.customers_staging')


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW customers_tmp_vw
# MAGIC AS
# MAGIC SELECT * FROM dw_analytics.customers_staging;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dw_analytics.customers tgt
# MAGIC USING customers_tmp_vw src
# MAGIC ON tgt.customer_id = src.customer_id
# MAGIC WHEN MATCHED AND tgt.email IS NULL AND src.email IS NOT NULL THEN
# MAGIC   UPDATE SET tgt.email = src.email, tgt.updated = src.updated
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;

# COMMAND ----------

# %sql
# CREATE OR REPLACE TEMP VIEW customers_temp_vw
# AS SELECT * FROM json.`/mnt/adobeadls/dwanalytics/customers/customers-json-new/`;

# COMMAND ----------

# %sql
# MERGE INTO dw_analytics.customers tgt
# USING customers_temp_vw src
# ON tgt.customer_id = src.customer_id
# WHEN MATCHED AND tgt.email IS NULL AND src.email IS NOT NULL THEN
#   UPDATE SET tgt.email = src.email, tgt.updated = src.updated
# WHEN NOT MATCHED THEN
#   INSERT *;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT count(*) AS count FROM dw_analytics.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC customer_id as cust_id
# MAGIC ,email
# MAGIC ,profile:first_name as first_name
# MAGIC ,profile:last_name as last_name
# MAGIC ,profile:first_name||' '||profile:last_name as full_name
# MAGIC ,profile:gender
# MAGIC ,profile:address:street
# MAGIC ,profile:address:city
# MAGIC ,profile:address:country
# MAGIC  FROM dw_analytics.customers
# MAGIC  ORDER BY 1 ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC count(*) as population
# MAGIC ,c.profile:gender as gender
# MAGIC ,c.profile:address:country as country
# MAGIC FROM dw_analytics.customers c
# MAGIC WHERE customer_id IS NOT NULL
# MAGIC GROUP BY gender, country
# MAGIC HAVING population > 50
# MAGIC ORDER BY population DESC, country ASC;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE customers_temp_vw;