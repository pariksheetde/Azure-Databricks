-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### CHECK THE FILES FOR BOOKS.csv (Azure Data Lake Storage)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"/mnt/adobeadls/dwanalytics/books/")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CHECK THE FILES IN RAW LAYERS FOR BOOKS.csv (Azure Data Lake Storage)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"/mnt/adobeadls/dwanalytics/books/books-csv-new")
-- MAGIC display(files)

-- COMMAND ----------

SELECT * FROM csv.`/mnt/adobeadls/dwanalytics/books/export_*.csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### READ THE BOOKS DATA FROM ADLS (Azure Data Lake Storage) and load in the books staging table

-- COMMAND ----------

DROP TABLE IF EXISTS dw_analytics.books;
CREATE TABLE dw_analytics.books
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
header = "true",
sep = ";"
)
LOCATION "/mnt/adobeadls/dwanalytics/books/export_*.csv";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### VALIDATE THE RECORDS

-- COMMAND ----------

SELECT * FROM dw_analytics.books;

-- COMMAND ----------

SELECT 
count(*) AS CNT
FROM dw_analytics.books;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### NEW RAW DATA FOR BOOKS

-- COMMAND ----------

SELECT * FROM csv.`/mnt/adobeadls/dwanalytics/books/books-csv-new/export_*.csv`

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_stg_vw
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
  path = "/mnt/adobeadls/dwanalytics/books/books-csv-new",
  header = "true",
  sep = ";"
);

CREATE OR REPLACE TABLE dw_analytics.books_staging
AS SELECT * FROM books_stg_vw;

SELECT * FROM dw_analytics.books_staging;

-- COMMAND ----------

SELECT * FROM dw_analytics.books_staging;

-- COMMAND ----------

DESC EXTENDED dw_analytics.books_staging;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### WRITE THE DATA TO STAGING LAYER

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read \
-- MAGIC     .table("dw_analytics.books_staging") \
-- MAGIC     .write \
-- MAGIC     .mode("append") \
-- MAGIC     .format("csv") \
-- MAGIC     .option("header", "true") \
-- MAGIC     .option("delimiter", ";") \
-- MAGIC     .save("/mnt/adobeadls/processed/books/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls("/mnt/adobeadls/processed/books/")
-- MAGIC display(files)

-- COMMAND ----------

DROP TABLE IF EXISTS dw_analytics.books_unparsed;
CREATE TABLE dw_analytics.books_unparsed
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
header = "true",
sep = ";"

)
LOCATION "/mnt/adobeadls/dwanalytics/books/books-csv-new/export_*.csv";

-- COMMAND ----------

SELECT * FROM dw_analytics.books_unparsed;

-- COMMAND ----------

SELECT * FROM csv.`/mnt/adobeadls/processed/books/`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE DELTA TABLE

-- COMMAND ----------

REFRESH TABLE dw_analytics.books;

-- COMMAND ----------

DROP TABLE dw_analytics.books;
CREATE TABLE dw_analytics.books
AS SELECT * FROM csv.`/mnt/adobeadls/processed/books/`;

SELECT * FROM dw_analytics.books;
SELECT COUNT(*) AS CNT FROM dw_analytics.books;

-- COMMAND ----------

DESC EXTENDED dw_analytics.books;

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
path = "/mnt/adobeadls/processed/books/",
header = "true",
delimiter = ";"
);

DROP TABLE dw_analytics.books_parsed;
CREATE TABLE dw_analytics.books_parsed
AS
SELECT
  book_id
  ,title
  ,author
  ,category
  ,price
  FROM
(
  SELECT
  book_id
  ,title
  ,author
  ,category
  ,price
  ,row_number() OVER (PARTITION BY book_id ORDER BY price ASC) AS row_number 
  FROM
  books_temp_vw
) Temp 
WHERE Temp.row_number = 1;

SELECT * FROM dw_analytics.books_parsed;

-- COMMAND ----------

SELECT count(*) AS CNT FROM dw_analytics.books_parsed;

-- COMMAND ----------

SELECT * FROM dw_analytics.books_parsed;

-- COMMAND ----------

SELECT 
book_id
,count(*) AS CNT
FROM books_temp_vw
GROUP BY book_id
HAVING CNT > 1;

-- COMMAND ----------

MERGE INTO dw_analytics.books AS tgt
USING dw_analytics.books_parsed src
ON tgt.book_id = src.book_id
  WHEN MATCHED THEN UPDATE *
  WHEN NOT MATCHED THEN 

-- COMMAND ----------

SELECT * FROM dw_analytics.books;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

-- COMMAND ----------

DROP TABLE dw_analytics.books;
