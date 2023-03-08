-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### QUERY parquet data file from constructors

-- COMMAND ----------

SELECT 
*, 
input_file_name() as file_name 
FROM parquet.`/mnt/adobeadls/processed/constructors`

-- COMMAND ----------

SELECT 
COUNT(*) AS CNT,
input_file_name() AS file_name
FROM parquet.`/mnt/adobeadls/processed/races/*`
GROUP BY input_file_name();

-- COMMAND ----------

DROP TABLE IF EXISTS delta.constructors_parquet;
CREATE TABLE delta.constructors_parquet
AS
SELECT * FROM parquet.`/mnt/adobeadls/processed/constructors`;

-- COMMAND ----------

SELECT * FROM delta.constructors_parquet;

-- COMMAND ----------

DESC EXTENDED delta.constructors_parquet;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
