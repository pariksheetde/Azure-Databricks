-- Databricks notebook source
SELECT * FROM JSON.`/mnt/adobeadls/dwanalytics/orders/books-raw/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE books_raw BRONZE STREAMING DLT TABLES

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE books_raw
COMMENT "The raw books store ingested from CDC Feed"
AS SELECT * FROM cloud_files("/mnt/adobeadls/dwanalytics/orders/books-raw/", "json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE SILVER LAYER TABLES

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE books_silver;

APPLY CHANGES INTO LIVE.books_silver
FROM STREAM(LIVE.books_raw)
KEYS (book_id)
SEQUENCE BY row_time
COLUMNS * EXCEPT (row_status, row_time)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE GOLD LAYER TABLES

-- COMMAND ----------


