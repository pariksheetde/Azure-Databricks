-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### QUERY PARQUET DATA FILE FROM PROCESSED/CIRCUITS (EXTERNAL DATA SOURCE)

-- COMMAND ----------

SELECT * FROM parquet.`/mnt/adobeadls/processed/circuits/*`

-- COMMAND ----------

SELECT 
* 
FROM parquet.`/mnt/adobeadls/processed/circuits/*`
WHERE country = 'Spain';

-- COMMAND ----------

SELECT 
COUNT(*) AS CNT,
country
FROM parquet.`/mnt/adobeadls/processed/circuits/*`
GROUP BY country
ORDER BY CNT DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE EXTERNAL TABLE TO QUERY DATA FROM PROCESSED DIRECTORY LOADED FROM 
-- MAGIC ##### https://adb-1320557121170389.9.azuredatabricks.net/?o=1320557121170389#notebook/2138863165542933/command/2138863165544089 

-- COMMAND ----------

DROP TABLE IF EXISTS dw_analytics.circuits_parquet;
CREATE TABLE dw_analytics.circuits_parquet
(
    circuit_id INT, 
    circuit_ref STRING, 
    name STRING, 
    location STRING, 
    country STRING, 
    latitude DOUBLE, 
    longitude DOUBLE, 
    altitude DOUBLE,
    file_name STRING, 
    load_ts TIMESTAMP
)
USING parquet
LOCATION "/mnt/adobeadls/processed/circuits/*";

-- COMMAND ----------

SELECT * FROM dw_analytics.circuits_parquet;

-- COMMAND ----------

DESC EXTENDED dw_analytics.circuits_parquet;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
