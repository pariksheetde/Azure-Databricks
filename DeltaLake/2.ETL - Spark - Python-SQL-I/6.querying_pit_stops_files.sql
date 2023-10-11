-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### QUERY PARQUET DATA FILE FROM PROCESSED/PIT_STOPS(EXTERNAL DATA SOURCE)

-- COMMAND ----------

SELECT 
*, 
input_file_name() as file_name 
FROM parquet.`/mnt/adobeadls/processed/pit_stops`

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW pit_stops_parquet_temp_vw
(
driver_id INT, 
duration STRING, 
lap INT,
milliseconds INT, 
race_id INT, 
stop STRING, 
time STRING, 
load_ts TIMESTAMP,
file_name STRING
)
USING parquet
OPTIONS 
(
path = "/mnt/adobeadls/processed/pit_stops"
);

-- COMMAND ----------

SELECT * FROM pit_stops_parquet_temp_vw;

-- COMMAND ----------

CREATE OR REPLACE TABLE dw_analytics.pit_stops_parquet
AS
SELECT * FROM pit_stops_parquet_temp_vw;

-- COMMAND ----------

SELECT
*
FROM dw_analytics.pit_stops_parquet
LIMIT 15;

-- COMMAND ----------

DESC EXTENDED dw_analytics.pit_stops_parquet;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP TABLE IF EXISTS delta.pit_stops_parquet;
