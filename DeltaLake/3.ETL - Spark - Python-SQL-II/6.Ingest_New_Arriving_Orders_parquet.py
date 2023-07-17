# Databricks notebook source
# MAGIC %sql
# MAGIC INSERT INTO dw_analytics.orders
# MAGIC SELECT * FROM parquet.`/mnt/adobeadls/dwanalytics/orders/orders-new/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS count FROM dw_analytics.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC order_id
# MAGIC ,order_timestamp
# MAGIC ,books
# MAGIC ,explode(books) as explode_books
# MAGIC FROM
# MAGIC dw_analytics.orders
# MAGIC ORDER BY 1 ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC customer_id
# MAGIC ,collect_set(order_id) as order_id
# MAGIC ,collect_set(books.book_id) as book_set
# MAGIC FROM
# MAGIC dw_analytics.orders
# MAGIC GROUP BY customer_id
# MAGIC ORDER BY 1 ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC customer_id
# MAGIC ,collect_set(order_id) as order_id
# MAGIC ,collect_set(books.book_id) as book_set_before_flatten
# MAGIC ,array_distinct(flatten(collect_set(books.book_id))) as book_set_after_flatten
# MAGIC FROM
# MAGIC dw_analytics.orders
# MAGIC GROUP BY customer_id
# MAGIC ORDER BY 1 ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC *
# MAGIC FROM (
# MAGIC   SELECT *, explode(books) as book
# MAGIC   FROM dw_analytics.orders) o JOIN dw_analytics.books b
# MAGIC   ON o.book.book_id = b.book_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, explode(books) as book
# MAGIC   FROM dw_analytics.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED dw_analytics.orders;
