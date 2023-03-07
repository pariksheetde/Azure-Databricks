-- Databricks notebook source
SELECT * FROM parquet.`/mnt/adobeadls/processed/circuits/*`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC processed_path = "/mnt/adobeadls/processed"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC processed_path

-- COMMAND ----------

SELECT * FROM parquet.`${processed_path}/circuits`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
