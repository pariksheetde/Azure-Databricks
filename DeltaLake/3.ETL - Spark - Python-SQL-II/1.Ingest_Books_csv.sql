-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### READ THE BOOKS DATA FROM ADLS (Azure Data Lake Storage)

-- COMMAND ----------

SELECT * FROM csv.`/mnt/adobeadls/dwanalytics/books/processed/export_*.csv`

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_staging_temp_vw
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
  path = "/mnt/adobeadls/dwanalytics/books/processed/export_*.csv",
  header = "true",
  sep = ";"
);

SELECT * FROM books_staging_temp_vw ORDER BY 1;

-- COMMAND ----------

SELECT COUNT(*) AS count FROM books_staging_temp_vw;

-- COMMAND ----------

CREATE OR REPLACE TABLE dw_analytics.books
AS
SELECT * FROM books_staging_temp_vw;

-- COMMAND ----------

DESC EXTENDED dw_analytics.books;

-- COMMAND ----------

SELECT * FROM dw_analytics.books ORDER BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE DELTA TABLE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

-- COMMAND ----------

SELECT * FROM dw_analytics.books;

-- COMMAND ----------

DROP TABLE IF EXISTS dw_analytics.books;
