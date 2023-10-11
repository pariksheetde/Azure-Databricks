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

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP TABLE IF EXISTS delta.circuits_parquet;
