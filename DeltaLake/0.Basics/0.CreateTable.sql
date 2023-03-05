-- Databricks notebook source
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

INSERT INTO Delta.locations
VALUES 
(100, "London"),
(110, "Berlin"),
(120, "Moscow"),
(130, "Dubai")

-- COMMAND ----------

INSERT INTO Delta.locations
VALUES 
(140, "Paris"),
(150, "Paris"),
(160, "Toronto"),
(170, "Milan")

-- COMMAND ----------

SELECT * FROM delta.locations
ORDER BY 1 ASC;
