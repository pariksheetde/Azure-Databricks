-- Databricks notebook source
-- MAGIC %run ../0.Includes/1.Copy-Datasets

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

DROP TABLE IF EXISTS delta.customers;
CREATE TABLE delta.customers
AS
SELECT * FROM json.`${dataset.bookstore}/customers-json`;

-- COMMAND ----------

SELECT * FROM delta.customers;

-- COMMAND ----------

DESC EXTENDED delta.customers;

-- COMMAND ----------



-- COMMAND ----------

SELECT COUNT(*) AS CNT FROM delta.books_staging;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP TABLE delta.top_director_csv
