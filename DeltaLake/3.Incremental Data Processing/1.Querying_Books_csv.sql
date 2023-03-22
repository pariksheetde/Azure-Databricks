-- Databricks notebook source
-- MAGIC %run ../0.Includes/1.Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

SELECT * FROM csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

DROP TABLE IF EXISTS delta.books_staging;
CREATE TABLE delta.books_staging
(book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS
(
header = "true",
delimiter = ";"
)
LOCATION "${dataset.bookstore}/books-csv";

-- COMMAND ----------

SELECT * FROM delta.books_staging;

-- COMMAND ----------

DESC EXTENDED delta.books_staging;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read \
-- MAGIC .table("delta.books_staging") \
-- MAGIC .write \
-- MAGIC .mode("append") \
-- MAGIC .format("csv") \
-- MAGIC .option("header", "true") \
-- MAGIC .option("delimiter", ";") \
-- MAGIC .save(f"{dataset_bookstore}/books-csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %fs rm -r "dbfs:/mnt/demo-datasets/bookstore/books-csv/part-00001-tid-7883297870500917170-94a1daad-c235-41fd-a57e-9fcca069b9d0-30-1-c000.csv"

-- COMMAND ----------

SELECT COUNT(*) AS CNT FROM delta.books_staging;

-- COMMAND ----------

REFRESH TABLE delta.books_staging;

-- COMMAND ----------

SELECT COUNT(*) AS CNT FROM delta.books_staging;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_temp_vw
(book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS
(
path = "${dataset.bookstore}/books-csv",
header = "true",
delimiter = ";"
);

SELECT * FROM books_temp_vw;

-- COMMAND ----------

SELECT * FROM books_temp_vw;

-- COMMAND ----------

SELECT COUNT(*) FROM books_temp_vw;

-- COMMAND ----------

DROP TABLE IF EXISTS delta.books;
CREATE TABLE delta.books
AS
SELECT * FROM books_temp_vw;

-- COMMAND ----------

MERGE INTO delta.books b
USING books_temp_vw a
ON a.book_id = b.book_id AND b.title = a.title
WHEN NOT MATCHED AND a.category = "Computer Science" THEN
INSERT *

-- COMMAND ----------

SELECT * FROM delta.books;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP TABLE delta.top_director_csv
