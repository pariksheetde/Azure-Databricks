-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### CREATE BRONZE LAYER TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE orders_raw BRONZE STREAMING DLT TABLES

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_raw
COMMENT "The raw orders books ingested from orders_raw"
AS
SELECT * FROM cloud_files("/mnt/adobeadls/dwanalytics/orders/orders-raw/", "parquet",
                         map("schema", "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity INT"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE customers BRONZE DLT TABLES

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE customers
AS
SELECT * FROM JSON.`/mnt/adobeadls/dwanalytics/customers/customers-json/`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE SILVER LAYER TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC | Column 1 | Column 2 |
-- MAGIC | ----------- | ----------- |
-- MAGIC | Row 1 | Value 1 |
-- MAGIC | Row 2 | Value 2 |

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_cleaned
(
  CONSTRAINT valid_customer_no EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Cleaned books ordered with valid order id"
AS
SELECT
o.order_id,
o.quantity,
o.customer_id,
c.profile:first_name as first_name,
c.profile:last_name as last_name,
c.profile:address:country as country
FROM
STREAM (LIVE.orders_raw) as o LEFT JOIN (LIVE.customers) as c
ON o.customer_id = c.customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE GOLD LAYER TABLES

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE daily_customer_books_count
AS
SELECT
customer_id,
first_name,
last_name,
sum(quantity) as books_quantity
FROM (LIVE.orders_cleaned)
WHERE country = "China"
GROUP BY customer_id, first_name,last_name;
