-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### QUERY csv data file from processed/lap_times

-- COMMAND ----------

SELECT 
*, 
input_file_name() as file_name 
FROM csv.`/mnt/adobeadls/raw/incremental/2021-03-21/lap_times`

-- COMMAND ----------

SELECT 
COUNT(*) AS CNT,
input_file_name() AS file_name
FROM csv.`/mnt/adobeadls/raw/incremental/2021-03-21/lap_times`
GROUP BY input_file_name();

-- COMMAND ----------

DROP TABLE IF EXISTS delta.lap_times_csv;
CREATE TABLE delta.lap_times_csv
(raceId INT, driverId INT, lap INT, position INT, time STRING, milliseconds INT)
USING CSV
OPTIONS
(
header = "true"
)
LOCATION "/mnt/adobeadls/raw/incremental/2021-03-21/lap_times"

-- COMMAND ----------

SELECT * FROM delta.lap_times_csv;

-- COMMAND ----------

DESC EXTENDED delta.lap_times_csv;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
