-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### QUERY parquet data file from processed/races

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

DROP TABLE IF EXISTS delta.races_parquet;
CREATE TABLE delta.races_parquet
(
race_id INT, round INT, circuit_id INT,
name STRING, file_name STRING, load_ts TIMESTAMP,
race_ts TIMESTAMP
)
USING parquet
LOCATION "/mnt/adobeadls/processed/races";

-- COMMAND ----------

SELECT * FROM delta.races_parquet WHERE race_year = 2015;

-- COMMAND ----------

DESC EXTENDED delta.races_parquet;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
