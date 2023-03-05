-- Databricks notebook source
CREATE TABLE IF NOT EXISTS departments
(
dept_id int,
dept_name varchar(50),
loc_id int
)
LOCATION '/mnt/adobeadls/presentation/external/departments'

-- COMMAND ----------

TRUNCATE TABLE departments;

-- COMMAND ----------

INSERT INTO departments
VALUES 
(1000, "PySaprk Developer", 100),
(1010, "Python Developer", 100),
(1020, "AWS Solution Architect", 100),
(1030, "Azure Data Services", 100)

-- COMMAND ----------

SELECT * FROM departments;

-- COMMAND ----------

DESC EXTENDED departments;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

-- COMMAND ----------

DROP TABLE departments;
