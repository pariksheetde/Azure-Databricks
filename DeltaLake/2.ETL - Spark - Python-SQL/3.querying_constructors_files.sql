-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Query parquet data file from processed/constructors

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

DROP TABLE IF EXISTS dw_analytics.constructors_parquet;
CREATE TABLE dw_analytics.constructors_parquet
AS
SELECT * FROM parquet.`/mnt/adobeadls/processed/constructors`;

-- COMMAND ----------

SELECT * FROM dw_analytics.constructors_parquet;

-- COMMAND ----------

DESC EXTENDED dw_analytics.constructors_parquet;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP TABLE IF EXISTS delta.constructors_parquet;
