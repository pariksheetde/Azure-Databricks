-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### QUERY PARQUET DATA FILE FROM PROCESSED/RESULTS (EXTERNAL DATA SOURCE)

-- COMMAND ----------

SELECT 
*, 
input_file_name() as file_name 
FROM parquet.`/mnt/adobeadls/processed/results`

-- COMMAND ----------

DROP TABLE IF EXISTS dw_analytics.results_parquet;
CREATE TABLE dw_analytics.results_parquet
AS
SELECT * FROM parquet.`/mnt/adobeadls/processed/results`;

-- COMMAND ----------

SELECT
*
FROM dw_analytics.results_parquet 
LIMIT 15;

-- COMMAND ----------

DESC EXTENDED dw_analytics.results_parquet;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP TABLE IF EXISTS delta.results_parquet;
