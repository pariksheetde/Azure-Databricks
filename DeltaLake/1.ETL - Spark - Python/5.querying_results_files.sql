-- Databricks notebook source
SELECT 
*, 
input_file_name() as file_name 
FROM parquet.`/mnt/adobeadls/processed/results`

-- COMMAND ----------

DROP TABLE IF EXISTS delta.results_parquet;
CREATE TABLE delta.results_parquet
AS
SELECT * FROM parquet.`/mnt/adobeadls/processed/results`;

-- COMMAND ----------

SELECT
*
FROM delta.results_parquet 
LIMIT 15;

-- COMMAND ----------

DESC EXTENDED delta.results_parquet;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
