-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### READ THE NEW ARRIVING BOOKS DATA FROM ADLS (Azure Data Lake Storage)

-- COMMAND ----------

SELECT * FROM csv.`/mnt/adobeadls/dwanalytics/books/landing_zone/*.csv`

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_temp_vw
(
    book_id STRING,
    title STRING,
    author STRING,
    category STRING,
    price DOUBLE
)
USING CSV
OPTIONS
(
  path = "/mnt/adobeadls/dwanalytics/books/landing_zone/*.csv",
  header = "true",
  sep = ";"
);

SELECT * FROM books_temp_vw ORDER BY 1;

-- COMMAND ----------

MERGE INTO dw_analytics.books AS tgt
USING books_temp_vw src
ON tgt.book_id = src.book_id
  WHEN MATCHED THEN UPDATE SET tgt.title = src.title, tgt.author = src.author, tgt.category = src.category, tgt.price = src.price 
  WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

SELECT count(*) AS CNT FROM dw_analytics.books;

-- COMMAND ----------

SELECT * FROM dw_analytics.books ORDER BY 1;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
