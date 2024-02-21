# Databricks notebook source
files = dbutils.fs.ls("/mnt/adobeadls/dwanalytics/orders/dlt")
display(files)

# COMMAND ----------

files = dbutils.fs.ls("/mnt/adobeadls/dwanalytics/orders/dlt/tables")
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

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC  book_id,
# MAGIC  title,
# MAGIC  author,
# MAGIC  category,
# MAGIC  price,
# MAGIC  row_time,
# MAGIC  row_status,
# MAGIC  `_rescued_data`
# MAGIC  FROM dlt.books_raw
# MAGIC  ORDER BY book_id ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC  book_id,
# MAGIC  title,
# MAGIC  author,
# MAGIC  category,
# MAGIC  price
# MAGIC  FROM dlt.books_silver
# MAGIC  ORDER BY book_id ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC *
# MAGIC FROM dlt.author_counts_state;
