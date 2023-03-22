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



-- COMMAND ----------

SELECT COUNT(*) AS CNT FROM delta.orders;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP TABLE delta.top_director_csv
