-- Databricks notebook source
SELECT 
*, 
input_file_name() as file_name 
FROM csv.`/mnt/adobeadls/raw/incremental/2021-03-21/lap_times`

-- COMMAND ----------

DROP TABLE delta.lap_times_csv;

-- COMMAND ----------

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

SELECT 
COUNT(*) AS CNT,
input_file_name() AS file_name
FROM csv.`/mnt/adobeadls/raw/incremental/2021-03-21/lap_times`
GROUP BY input_file_name();

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

-- COMMAND ----------

DESC EXTENDED delta.lap_times_csv

-- COMMAND ----------

DROP TABLE delta.lap_times_csv;
