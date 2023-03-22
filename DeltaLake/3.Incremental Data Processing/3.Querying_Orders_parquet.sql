-- Databricks notebook source
-- MAGIC %run ../0.Includes/1.Copy-Datasets

-- COMMAND ----------

SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

CREATE OR REPLACE TABLE delta.orders
AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`;

-- COMMAND ----------

SELECT * FROM delta.orders;

-- COMMAND ----------

DESC EXTENDED delta.orders;

-- COMMAND ----------

SELECT COUNT(*) AS CNT FROM delta.orders;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
