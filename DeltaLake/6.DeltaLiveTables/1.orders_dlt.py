# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM PARQUET.`/mnt/adobeadls/dwanalytics/orders/orders_raw/*.parquet`;

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE BRONZE LAYER TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE orders_raw BRONZE STREAMING DLT TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE orders_raw
# MAGIC COMMENT "The raw orders books ingested from orders_raw"
# MAGIC AS
# MAGIC SELECT * FROM cloud_files("/mnt/adobeadls/dwanalytics/orders/orders_raw/", "parquet",
# MAGIC                          map("schema", "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity INT"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE customers BRONZE DLT TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE customers
# MAGIC AS
# MAGIC SELECT * FROM JSON.`/mnt/adobeadls/dwanalytics/customers/customers-json/`;

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE SILVER LAYER TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE orders_cleaned
# MAGIC (
# MAGIC   CONSTRAINT valid_customer_no EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT "Cleaned books ordered with valid order id"
# MAGIC AS
# MAGIC SELECT
# MAGIC o.order_id,
# MAGIC o.quantity,
# MAGIC o.customer_id,
# MAGIC c.profile:first_name as first_name,
# MAGIC c.profile:last_name as last_name,
# MAGIC c.profile:address:country as country
# MAGIC FROM
# MAGIC STREAM (LIVE.orders_raw) as o LEFT JOIN (LIVE.customers) as c
# MAGIC ON o.customer_id = c.customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE GOLD LAYER TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE daily_customer_books_count
# MAGIC AS
# MAGIC SELECT
# MAGIC customer_id,
# MAGIC first_name,
# MAGIC last_name,
# MAGIC sum(quantity) as books_quantity
# MAGIC FROM (LIVE.orders_cleaned)
# MAGIC WHERE country = "China"
# MAGIC GROUP BY customer_id, first_name,last_name;
