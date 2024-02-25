# Databricks notebook source
files = dbutils.fs.ls("/mnt/adobeadls/dwanalytics/orders/orders-raw/")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS cnt FROM bronze.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS CNT FROM silver.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.daily_customers_books ORDER BY books_qty DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS CNT FROM gold.daily_customers_books;

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
