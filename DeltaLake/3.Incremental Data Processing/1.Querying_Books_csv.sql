-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### READ THE BOOKS DATA FROM ADLS (Azure Data Lake Storage)

-- COMMAND ----------

SELECT * FROM csv.`/mnt/adobeadls/dwanalytics/books/export_*.csv`

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
  path = "/mnt/adobeadls/dwanalytics/books/export_*.csv",
  header = "true",
  sep = ";"
);

SELECT * FROM books_staging_temp_vw;

-- COMMAND ----------

SELECT COUNT(*) AS count FROM books_staging_temp_vw;

-- COMMAND ----------

CREATE OR REPLACE TABLE dw_analytics.books
AS
SELECT * FROM books_staging_temp_vw;

-- COMMAND ----------

DESC EXTENDED dw_analytics.books;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### READ RAW BOOKS.CSV FROM ADLS

-- COMMAND ----------

SELECT * FROM csv.`/mnt/adobeadls/dwanalytics/books/books-csv-new/*.csv`

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
  path = "/mnt/adobeadls/dwanalytics/books/books-csv-new/*.csv",
  header = "true",
  sep = ";"
);

SELECT * FROM books_temp_vw;

-- COMMAND ----------

MERGE INTO dw_analytics.books AS tgt
USING books_temp_vw src
ON tgt.book_id = src.book_id
  WHEN MATCHED THEN UPDATE SET tgt.title = src.title, tgt.author = src.author, tgt.category = src.category, tgt.price = src.price 
  WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE DELTA TABLE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read \
-- MAGIC     .table("dw_analytics.books") \
-- MAGIC     .write \
-- MAGIC     .mode("overwrite") \
-- MAGIC     .format("delta") \
-- MAGIC     .option("header", "true") \
-- MAGIC     .option("delimiter", ";") \
-- MAGIC     .save("/mnt/adobeadls/processed/books/")

-- COMMAND ----------

SELECT * FROM delta.`/mnt/adobeadls/processed/books/`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
