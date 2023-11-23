-- Databricks notebook source
DROP DATABASE IF EXISTS Delta;
CREATE DATABASE IF NOT EXISTS Delta;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### CREATE Delta Table, Locations

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Delta.Locations
(
loc_id int,
loc_name varchar(20)
)
USING DELTA;

-- COMMAND ----------

TRUNCATE TABLE delta.locations;

-- COMMAND ----------

INSERT INTO Delta.locations
VALUES 
(100, "London"),
(110, "Berlin"),
(120, "Moscow"),
(130, "Dubai"),
(140, "Paris"),
(150, "Paris"),
(160, "Toronto"),
(170, "Milan")

-- COMMAND ----------

SELECT * FROM delta.locations
ORDER BY 1 ASC;

-- COMMAND ----------

DESC DETAIL delta.locations;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/delta.db/locations'

-- COMMAND ----------

DESC HISTORY delta.locations;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
