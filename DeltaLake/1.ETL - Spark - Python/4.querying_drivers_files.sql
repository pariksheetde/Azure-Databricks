-- Databricks notebook source
SELECT 
*, 
input_file_name() as file_name 
FROM json.`/mnt/adobeadls/raw/incremental/2021-03-21/drivers.json`

-- COMMAND ----------

DROP TABLE delta.drivers_json;
CREATE TABLE delta.drivers_json
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
FROM delta.drivers_json;

-- COMMAND ----------

DESC EXTENDED delta.drivers_json;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
