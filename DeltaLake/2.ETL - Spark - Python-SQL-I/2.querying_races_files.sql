-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### QUERY PARQUET DATA FILE FROM PROCESSED/RACES (EXTERNAL DATA SOURCES)

-- COMMAND ----------

SELECT 
*, 
input_file_name() as file_name_with_location 
FROM parquet.`/mnt/adobeadls/processed/races/race_year=2000`

-- COMMAND ----------

SELECT 
COUNT(*) AS CNT,
input_file_name() AS file_name
FROM parquet.`/mnt/adobeadls/processed/races/*`
GROUP BY input_file_name();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE EXTERNAL TABLE TO QUERY DATA FROM PROCESSED DIRECTORY LOADED FROM 
-- MAGIC #### https://adb-1320557121170389.9.azuredatabricks.net/?o=1320557121170389#notebook/2138863165543167/command/2138863165544265

-- COMMAND ----------

DROP TABLE IF EXISTS dw_analytics.races_parquet;
CREATE TABLE dw_analytics.races_parquet
(
  race_id INT, 
  round INT, 
  circuit_id INT,
  name STRING, 
  file_name STRING, 
  load_ts TIMESTAMP,
  race_ts TIMESTAMP
)
USING parquet
LOCATION "/mnt/adobeadls/processed/races";

-- COMMAND ----------

SELECT * FROM dw_analytics.races_parquet WHERE race_year = 2015;

-- COMMAND ----------

DESC EXTENDED dw_analytics.races_parquet;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
