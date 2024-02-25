-- Databricks notebook source
-- SELECT * FROM JSON.`/mnt/adobeadls/dwanalytics/orders/books-raw/`

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
COLUMNS * EXCEPT (row_status, row_time, _rescued_data)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE GOLD LAYER TABLES

-- COMMAND ----------

CREATE LIVE TABLE author_counts_state
COMMENT "Number of books per author"
AS
SELECT
author,
count(*) AS books_count,
current_timestamp() AS updated_time
FROM LIVE.books_silver
GROUP BY author

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE GOLD LAYER VIEWS

-- COMMAND ----------

-- CREATE LIVE TABLE / VIEW books_sales
-- AS
-- SELECT
--   b.title,
--   o.quantity
--   FROM (
--     SELECT *, explode(books) as book
--     FROM LIVE.orders_cleaned
--   ) AS O INNER JOIN LIVE.books_silver AS B
--   ON o.book.book_id = b.book_id
