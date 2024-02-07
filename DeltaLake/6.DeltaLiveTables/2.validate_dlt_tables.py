# Databricks notebook source
files = dbutils.fs.ls("/mnt/adobeadls/dwanalytics/orders/metadata")
display(files)

# COMMAND ----------

files = dbutils.fs.ls("/mnt/adobeadls/dwanalytics/orders/metadata/tables")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dlt.orders_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dlt.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dlt.orders_cleaned;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dlt.daily_customer_books_count;
