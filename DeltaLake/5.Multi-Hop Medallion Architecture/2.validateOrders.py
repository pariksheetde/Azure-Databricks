# Databricks notebook source
files = dbutils.fs.ls("/mnt/adobeadls/dwanalytics/orders/orders-raw/")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS cnt FROM bronze.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS CNT FROM silver.orders;
