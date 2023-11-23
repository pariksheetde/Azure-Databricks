-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### CREATE A PERSISTANT VIEW

-- COMMAND ----------

CREATE VIEW IF NOT EXISTS delta.emp_wise_loc_dept_vw
AS
SELECT 
l.loc_id,
l.loc_name,
d.dept_name
FROM delta.departments d JOIN delta.locations l
ON d.loc_id = l.loc_id;

-- COMMAND ----------

SELECT * FROM delta.emp_wise_loc_dept_vw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### CREATE A TEMPORARY VIEW

-- COMMAND ----------

CREATE TEMP VIEW emp_wise_loc_dept_temp_vw
AS
SELECT 
l.loc_id,
l.loc_name,
d.dept_name
FROM delta.departments d JOIN delta.locations l
ON d.loc_id = l.loc_id;

-- COMMAND ----------

SELECT * FROM emp_wise_loc_dept_temp_vw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### CREATE A GLOBAL TEMPORARY VIEW

-- COMMAND ----------

CREATE GLOBAL TEMP VIEW emp_wise_loc_dept_global_temp_vw
AS
SELECT 
l.loc_id,
l.loc_name,
d.dept_name
FROM delta.departments d JOIN delta.locations l
ON d.loc_id = l.loc_id;

-- COMMAND ----------

SELECT * FROM delta.emp_wise_loc_dept_global_temp_vw;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
