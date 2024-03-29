-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### QUERY PARQUET DATA FILE FROM PROCESSED/QUALIFYING(EXTERNAL DATA SOURCE)

-- COMMAND ----------

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

CREATE OR REPLACE TABLE dw_analytics.qualifying_parquet
AS
SELECT * FROM parquet.`/mnt/adobeadls/processed/qualifying`;

-- COMMAND ----------

SELECT * FROM dw_analytics.qualifying_parquet;

-- COMMAND ----------

DESC EXTENDED dw_analytics.qualifying_parquet;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
