-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### QUERY JSON DATA FILE FROM PROCESSED/DRIVERS (EXTERNAL DATA SOURCE)

-- COMMAND ----------

SELECT 
*, 
input_file_name() as file_name 
FROM json.`/mnt/adobeadls/raw/incremental/2021-03-21/drivers.json`

-- COMMAND ----------

DROP TABLE IF EXISTS dw_analytics.drivers_json;
CREATE TABLE IF NOT EXISTS dw_analytics.drivers_json
AS
SELECT * FROM json.`/mnt/adobeadls/raw/incremental/2021-03-21/drivers.json`;

-- COMMAND ----------

SELECT
code, 
dob,
driverid,
driverref,
nationality,
name.forename AS f_name,
name.surname AS l_name
FROM dw_analytics.drivers_json;

-- COMMAND ----------

DESC EXTENDED dw_analytics.drivers_json;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP TABLE IF EXISTS delta.drivers_json;
