-- Databricks notebook source
SELECT 
*, 
input_file_name() as file_name 
FROM parquet.`/mnt/adobeadls/processed/races/race_year=2000`

-- COMMAND ----------

SELECT 
COUNT(*) AS CNT,
input_file_name() AS file_name
FROM parquet.`/mnt/adobeadls/processed/races/*`
GROUP BY input_file_name();

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
