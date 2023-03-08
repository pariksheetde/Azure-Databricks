-- Databricks notebook source
SELECT 
*, 
input_file_name() as file_name 
FROM parquet.`/mnt/adobeadls/processed/qualifying`;

-- COMMAND ----------

SELECT 
COUNT(*) AS CNT,
input_file_name() AS file_name
FROM parquet.`/mnt/adobeadls/processed/qualifying`
GROUP BY input_file_name();

-- COMMAND ----------

DROP TABLE IF EXISTS delta.qualifying_parquet;
CREATE TABLE delta.qualifying_parquet
AS
SELECT * FROM parquet.`/mnt/adobeadls/processed/qualifying`;

-- COMMAND ----------

SELECT * FROM delta.qualifying_parquet;

-- COMMAND ----------

DESC EXTENDED delta.qualifying_parquet;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
