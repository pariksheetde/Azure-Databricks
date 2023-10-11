-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### QUERY CSV DATA FILE FROM PROCESSED/LAP_TIMES(EXTERNAL DATA SOURCE)

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

DROP TABLE IF EXISTS dw_analytics.lap_times_csv;
CREATE TABLE dw_analytics.lap_times_csv
(
  raceId INT, 
  driverId INT, 
  lap INT, 
  position INT, 
  time STRING, 
  milliseconds INT
)
USING CSV
OPTIONS
(
header = "true"
)
LOCATION "/mnt/adobeadls/raw/incremental/2021-03-21/lap_times"

-- COMMAND ----------

SELECT * FROM dw_analytics.lap_times_csv;

-- COMMAND ----------

DESC EXTENDED dw_analytics.lap_times_csv;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP TABLE IF EXISTS delta.lap_times_csv;
